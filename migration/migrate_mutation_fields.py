"""
**File:** ``migrate_mutation_fields.py``
**Region:** ``migration``

Migrate Xledger GraphQL add/update input fields into packaged assets.

Introspects create/update mutation input types from the live Xledger schema and
syncs flattened fields into matching ``metadata.json`` files under
``src/ds_provider_xledger_py_lib/assets`` (add missing, remove stale).

Usage
-----
.. code-block:: bash

    # Dry-run (default): report adds/removes only
    uv run python migration/migrate_mutation_fields.py

    # Apply: add schema fields and remove fields not in schema
    uv run python migration/migrate_mutation_fields.py --apply

    # Limit to one entrypoint
    uv run python migration/migrate_mutation_fields.py --entrypoint salesOrderItems --apply

Environment
-----------
- ``XLEDGER_TOKEN`` (required): API token from ``.env``
- ``XLEDGER_HOST`` (optional): defaults to ``https://www.xledger.net/graphql``
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from ds_common_logger_py_lib import Logger

Logger.configure(level=logging.INFO)
logger = Logger.get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
ASSETS_ROOT = REPO_ROOT / "src" / "ds_provider_xledger_py_lib" / "assets"
DEFAULT_HOST = "https://www.xledger.net/graphql"

# Xledger burst limit is 20 requests / 5s; stay comfortably under that.
DEFAULT_REQUEST_INTERVAL_S = 0.35
DEFAULT_RATE_LIMIT_RETRIES = 6
DEFAULT_RATE_LIMIT_BACKOFF_S = 5.0

_WRITE_OPERATIONS = frozenset({"create", "update"})
_SKIP_EDGE_FIELDS = frozenset({"clientId", "clientMutationId"})
_SKIP_HELPER_FIELDS = frozenset({"showAlternatives", "specificity"})

_INPUT_TYPE_RE = re.compile(
    r"\$PlaceHolderInput:\s*\[(?P<input_type>[A-Za-z_][A-Za-z0-9_]*)!\]!",
)
_MUTATION_NAME_RE = re.compile(
    r"\b(?P<name>add[A-Za-z0-9_]+|update[A-Za-z0-9_]+)\s*\(",
)

_TYPE_MAP: dict[str, str] = {
    "String": "string",
    "ID": "string",
    "Int": "int64",
    "Long": "int64",
    "BigInt": "int64",
    "Float": "float64",
    "Decimal": "float64",
    "Boolean": "boolean",
    "Date": "datetime[ns]",
    "DateTime": "datetime[ns]",
    "Time": "datetime[ns]",
}

_INTROSPECTION_QUERY = """
query IntrospectInputType($name: String!) {
  __type(name: $name) {
    name
    kind
    inputFields {
      name
      description
      type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
    }
  }
}
"""


class MigrationError(RuntimeError):
    """Raised when mutation-field migration cannot continue for an operation."""


@dataclass(frozen=True, kw_only=True)
class FlatField:
    """Flattened GraphQL input leaf mapped to asset metadata shape."""

    name: str
    type: str
    description: str
    required: bool


@dataclass(frozen=True, kw_only=True)
class OperationTarget:
    """One create/update asset operation to migrate."""

    entrypoint: str
    operation: str
    metadata_path: Path
    query_path: Path
    input_type: str
    mutation_name: str

    @property
    def label(self) -> str:
        """Human-readable ``entrypoint/operation`` label."""
        return f"{self.entrypoint}/{self.operation}"


@dataclass(kw_only=True)
class MigrationResult:
    """Diff summary for one operation."""

    target: OperationTarget
    schema_fields: list[FlatField]
    added: list[str]
    covered: list[str]
    removed: list[str]
    skipped_lists: list[str]


@dataclass(kw_only=True)
class MigrationTotals:
    """Aggregate counters across all migrated operations."""

    added: int = 0
    covered: int = 0
    removed: int = 0
    failures: int = 0

    def record(self, result: MigrationResult) -> None:
        """Accumulate counters from one operation result."""
        self.added += len(result.added)
        self.covered += len(result.covered)
        self.removed += len(result.removed)


class XledgerIntrospector:
    """GraphQL introspection client with burst-rate throttling and retries."""

    def __init__(
        self,
        *,
        host: str,
        token: str,
        timeout: float = 60.0,
        request_interval_s: float = DEFAULT_REQUEST_INTERVAL_S,
        rate_limit_retries: int = DEFAULT_RATE_LIMIT_RETRIES,
        rate_limit_backoff_s: float = DEFAULT_RATE_LIMIT_BACKOFF_S,
    ) -> None:
        self.host = host.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.request_interval_s = max(0.0, request_interval_s)
        self.rate_limit_retries = max(0, rate_limit_retries)
        self.rate_limit_backoff_s = max(0.0, rate_limit_backoff_s)
        self._type_cache: dict[str, dict[str, Any] | None] = {}
        self._last_request_at = 0.0

    def get_input_type(self, name: str) -> dict[str, Any] | None:
        """Fetch and cache an INPUT_OBJECT type definition."""
        if name in self._type_cache:
            logger.debug("Using cached input type '%s'.", name)
            return self._type_cache[name]

        logger.debug("Introspecting input type '%s'.", name)
        payload = self._post(query=_INTROSPECTION_QUERY, variables={"name": name})
        type_data = (payload.get("data") or {}).get("__type")
        self._type_cache[name] = type_data
        return type_data

    def _wait_for_slot(self) -> None:
        """Sleep until the configured minimum interval since the last request."""
        if self.request_interval_s <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.request_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)

    @staticmethod
    def _is_burst_rate_limit(errors: Any) -> bool:
        """Return True when GraphQL errors indicate Xledger burst rate limiting."""
        if not isinstance(errors, list):
            return False
        for error in errors:
            if not isinstance(error, dict):
                continue
            code = str(error.get("code") or "")
            message = str(error.get("message") or "")
            if "BURST_RATE_LIMIT" in code or "Too many requests" in message:
                return True
        return False

    def _post(self, *, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """POST a GraphQL document with throttling and rate-limit retries."""
        body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        attempts = self.rate_limit_retries + 1

        for attempt in range(1, attempts + 1):
            self._wait_for_slot()
            request = urllib.request.Request(
                self.host,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"token {self.token}",
                    "Accept": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise MigrationError(f"HTTP {exc.code} from Xledger: {detail}") from exc
            except urllib.error.URLError as exc:
                raise MigrationError(f"Failed to reach Xledger host '{self.host}': {exc}") from exc
            finally:
                self._last_request_at = time.monotonic()

            errors = payload.get("errors")
            if not errors:
                return payload
            if self._is_burst_rate_limit(errors) and attempt < attempts:
                wait_s = self.rate_limit_backoff_s * attempt
                logger.warning(
                    "Burst rate limit reached; sleeping %.1fs (retry %s/%s).",
                    wait_s,
                    attempt,
                    self.rate_limit_retries,
                )
                time.sleep(wait_s)
                continue
            raise MigrationError(f"GraphQL introspection errors: {json.dumps(errors)}")

        raise MigrationError("GraphQL introspection failed after rate-limit retries.")


def discover_targets(*, entrypoint: str | None = None) -> list[OperationTarget]:
    """Discover create/update asset operations under ``assets/``."""
    targets: list[OperationTarget] = []
    for metadata_path in sorted(ASSETS_ROOT.glob("*/*/metadata.json")):
        operation = metadata_path.parent.name
        if operation not in _WRITE_OPERATIONS:
            continue

        entrypoint_name = metadata_path.parent.parent.name
        if entrypoint and entrypoint_name != entrypoint:
            continue

        query_path = metadata_path.parent / "query.graphql"
        if not query_path.exists():
            logger.warning("Skipping %s/%s: missing query.graphql.", entrypoint_name, operation)
            continue

        query_text = query_path.read_text(encoding="utf-8")
        input_match = _INPUT_TYPE_RE.search(query_text)
        if input_match is None:
            logger.warning(
                "Skipping %s/%s: could not parse PlaceHolderInput type.",
                entrypoint_name,
                operation,
            )
            continue

        mutation_match = _MUTATION_NAME_RE.search(query_text)
        mutation_name = mutation_match.group("name") if mutation_match else input_match.group("input_type")
        targets.append(
            OperationTarget(
                entrypoint=entrypoint_name,
                operation=operation,
                metadata_path=metadata_path,
                query_path=query_path,
                input_type=input_match.group("input_type"),
                mutation_name=mutation_name,
            )
        )
    return targets


def unwrap_named_type(
    type_node: dict[str, Any] | None,
) -> tuple[str | None, str | None, bool, bool]:
    """Unwrap NON_NULL/LIST wrappers.

    Returns:
        ``(name, kind, is_non_null, is_list)`` for the innermost named type.
    """
    if type_node is None:
        return None, None, False, False

    is_non_null = False
    is_list = False
    current: dict[str, Any] | None = type_node
    while current is not None:
        kind = current.get("kind")
        if kind == "NON_NULL":
            is_non_null = True
            current = current.get("ofType")
            continue
        if kind == "LIST":
            is_list = True
            current = current.get("ofType")
            continue
        return current.get("name"), kind, is_non_null, is_list
    return None, None, is_non_null, is_list


def map_graphql_type(*, name: str | None, kind: str | None) -> str:
    """Map a GraphQL scalar/enum type name to asset metadata type."""
    if kind == "ENUM" or name is None:
        return "string"
    return _TYPE_MAP.get(name, "string")


def resolve_node_root_type(introspector: XledgerIntrospector, input_type: str) -> str:
    """Prefer ``node`` input object when a mutation edge wrapper is present."""
    type_data = introspector.get_input_type(input_type)
    if type_data is None:
        raise MigrationError(f"Input type '{input_type}' not found in Xledger schema.")
    if type_data.get("kind") != "INPUT_OBJECT":
        raise MigrationError(f"Type '{input_type}' is {type_data.get('kind')}, expected INPUT_OBJECT.")

    for field_def in type_data.get("inputFields") or []:
        if field_def.get("name") != "node":
            continue
        name, kind, _, is_list = unwrap_named_type(field_def.get("type"))
        if not is_list and kind == "INPUT_OBJECT" and name:
            logger.debug("Resolved '%s' node root type to '%s'.", input_type, name)
            return name
    return input_type


def flatten_input_fields(
    *,
    introspector: XledgerIntrospector,
    type_name: str,
    prefix: str = "",
    parent_required: bool = True,
    path_stack: tuple[str, ...] = (),
    skipped_lists: list[str] | None = None,
    include_helpers: bool = False,
) -> list[FlatField]:
    """Recursively flatten INPUT_OBJECT fields using underscore nesting."""
    skipped_lists = skipped_lists if skipped_lists is not None else []
    if type_name in path_stack:
        return []

    type_data = introspector.get_input_type(type_name)
    if type_data is None or type_data.get("kind") != "INPUT_OBJECT":
        return []

    next_stack = (*path_stack, type_name)
    flattened: list[FlatField] = []
    for field_def in type_data.get("inputFields") or []:
        field_name = field_def.get("name")
        if not field_name:
            continue
        if prefix == "" and field_name in _SKIP_EDGE_FIELDS:
            continue
        if not include_helpers and field_name in _SKIP_HELPER_FIELDS:
            continue

        name, kind, is_non_null, is_list = unwrap_named_type(field_def.get("type"))
        flat_name = f"{prefix}{field_name}"
        required = parent_required and is_non_null
        description = (field_def.get("description") or f"GraphQL input field '{flat_name}'.").strip()

        if is_list:
            skipped_lists.append(flat_name)
            continue

        if kind == "INPUT_OBJECT" and name:
            flattened.extend(
                flatten_input_fields(
                    introspector=introspector,
                    type_name=name,
                    prefix=f"{flat_name}_",
                    parent_required=required,
                    path_stack=next_stack,
                    skipped_lists=skipped_lists,
                    include_helpers=include_helpers,
                )
            )
            continue

        flattened.append(
            FlatField(
                name=flat_name,
                type=map_graphql_type(name=name, kind=kind),
                description=description,
                required=required,
            )
        )
    return flattened


def _field_to_metadata(field_def: FlatField) -> dict[str, Any]:
    """Convert a flattened schema field into asset metadata JSON shape."""
    return {
        "name": field_def.name,
        "type": field_def.type,
        "description": field_def.description,
        "required": field_def.required,
    }


def migrate_operation(
    *,
    target: OperationTarget,
    introspector: XledgerIntrospector,
    apply: bool,
    include_helpers: bool = False,
) -> MigrationResult:
    """Compute and optionally apply field migration for one operation."""
    root_type = resolve_node_root_type(introspector, target.input_type)
    skipped_lists: list[str] = []
    schema_fields = flatten_input_fields(
        introspector=introspector,
        type_name=root_type,
        skipped_lists=skipped_lists,
        include_helpers=include_helpers,
    )

    metadata = json.loads(target.metadata_path.read_text(encoding="utf-8"))
    existing_fields = list(metadata.get("fields") or [])
    existing_by_name = {field_def["name"]: field_def for field_def in existing_fields if "name" in field_def}
    schema_by_name = {field_def.name: field_def for field_def in schema_fields}

    added_names = [name for name in schema_by_name if name not in existing_by_name]
    covered_names = [name for name in schema_by_name if name in existing_by_name]
    removed_names = [name for name in existing_by_name if name not in schema_by_name]

    if apply and (added_names or removed_names):
        removed_set = set(removed_names)
        merged_fields = [field_def for field_def in existing_fields if field_def.get("name") not in removed_set]
        merged_fields.extend(_field_to_metadata(schema_by_name[name]) for name in added_names)
        metadata["fields"] = merged_fields
        target.metadata_path.write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        logger.info(
            "Updated %s (%s added, %s removed).",
            target.metadata_path.relative_to(REPO_ROOT),
            len(added_names),
            len(removed_names),
        )

    return MigrationResult(
        target=target,
        schema_fields=schema_fields,
        added=added_names,
        covered=covered_names,
        removed=removed_names,
        skipped_lists=skipped_lists,
    )


def log_result(result: MigrationResult, *, apply: bool) -> None:
    """Log a compact per-operation migration report."""
    target = result.target
    removed_label = "removed" if apply else "stale"
    logger.info(
        "[%s] input=%s schema_fields=%s added=%s covered=%s %s=%s",
        target.label,
        target.input_type,
        len(result.schema_fields),
        len(result.added),
        len(result.covered),
        removed_label,
        len(result.removed),
    )
    if result.added:
        logger.info("  + %s", ", ".join(result.added))
    if result.removed:
        prefix = "-" if apply else "~ stale:"
        logger.info("  %s %s", prefix, ", ".join(result.removed))
    if result.skipped_lists:
        logger.warning("  skipped list fields: %s", ", ".join(result.skipped_lists))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Migrate Xledger add/update input fields into asset metadata.json files "
            "(add missing schema fields, remove stale asset fields)."
        ),
    )
    parser.add_argument(
        "--host",
        default=None,
        help=f"Xledger GraphQL host (default: $XLEDGER_HOST or {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--entrypoint",
        default=None,
        help="Limit migration to a single assets entrypoint (e.g. salesOrderItems)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes: add missing schema fields and remove stale asset fields",
    )
    parser.add_argument(
        "--include-helpers",
        action="store_true",
        help="Also migrate relation helper fields (showAlternatives, specificity)",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=DEFAULT_REQUEST_INTERVAL_S,
        help=(f"Minimum seconds between GraphQL requests (default: {DEFAULT_REQUEST_INTERVAL_S}; Xledger burst limit is 20/5s)"),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run mutation-field migration."""
    load_dotenv(REPO_ROOT / ".env")
    args = parse_args(argv)

    if args.verbose:
        Logger.configure(level=logging.DEBUG)

    token = os.getenv("XLEDGER_TOKEN", "").strip()
    if not token:
        logger.error("Missing XLEDGER_TOKEN. Add it to .env or export it.")
        return 1

    host = (args.host or os.getenv("XLEDGER_HOST") or DEFAULT_HOST).strip()
    targets = discover_targets(entrypoint=args.entrypoint)
    if not targets:
        logger.error("No create/update operations found to migrate.")
        return 1

    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(
        "%s: migrating %s operation(s) from %s (request interval=%.2fs).",
        mode,
        len(targets),
        host,
        args.request_interval,
    )

    introspector = XledgerIntrospector(
        host=host,
        token=token,
        request_interval_s=args.request_interval,
    )
    totals = MigrationTotals()

    for target in targets:
        try:
            result = migrate_operation(
                target=target,
                introspector=introspector,
                apply=args.apply,
                include_helpers=args.include_helpers,
            )
        except Exception as exc:
            totals.failures += 1
            logger.error("[%s] %s", target.label, exc)
            continue

        log_result(result, apply=args.apply)
        totals.record(result)

    removed_label = "removed" if args.apply else "stale"
    logger.info(
        "Done. added=%s covered=%s %s=%s failures=%s",
        totals.added,
        totals.covered,
        removed_label,
        totals.removed,
        totals.failures,
    )
    return 1 if totals.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
