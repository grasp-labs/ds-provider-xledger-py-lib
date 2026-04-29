"""
**File:** ``introspection.py``
**Region:** ``ds_provider_xledger_py_lib/utils``

Description
-----------
Metadata loading helpers for dataset operation contracts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from importlib.resources import files
from typing import Any

from ds_common_serde_py_lib.serializable import Serializable
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError, ValidationError

from ..enums import OperationType


@dataclass(frozen=True, kw_only=True)
class MetaField(Serializable):
    """Single field definition from an operation metadata asset."""

    name: str
    type: str
    description: str
    required: bool = False
    default: bool = False


@dataclass(kw_only=True)
class IncrementalMetaData(Serializable):
    """Incremental read section from packaged read metadata.

    ``field`` names the node attribute used for watermarks; ``filter_field`` names the
    GraphQL filter argument (e.g. ``modifiedAt_gte``). They are distinct and both required
    when ``incremental`` is present in JSON.
    """

    kind: str
    field: str
    filter_field: str


@dataclass(kw_only=True)
class PaginationMetaData(Serializable):
    """Static pagination read definition from metadata."""

    kind: str
    first: int | None = None


@dataclass(kw_only=True)
class MetaData(Serializable):
    """Operation-level metadata plus packaged GraphQL query template."""

    name: str
    type: str
    description: str
    fields: list[MetaField]
    query: str
    pattern: str | None = None
    incremental: IncrementalMetaData | None = None
    pagination: PaginationMetaData | None = None


@dataclass(frozen=True, kw_only=True)
class EntryPointMetaData:
    """Container for all loaded operations for one entrypoint."""

    entrypoint: str
    operations: dict[OperationType, MetaData]

    def get(self, *, operation: OperationType) -> MetaData:
        """Return metadata for a specific operation.

        Args:
            operation: Operation to resolve.

        Returns:
            Metadata for the requested operation.

        Raises:
            NotSupportedError: If metadata for the operation is unavailable.
        """
        metadata = self.operations.get(operation)
        if metadata is not None:
            return metadata
        available = ", ".join(sorted(item.value for item in self.operations)) or "none"
        raise NotSupportedError(
            message=(
                f"Metadata operation '{operation.value}' is unavailable for "
                f"entrypoint '{self.entrypoint}'. Available operations: {available}."
            ),
        )

    @property
    def read(self) -> MetaData | None:
        """Metadata for read operation, when available."""
        return self.operations.get(OperationType.READ)

    @property
    def create(self) -> MetaData | None:
        """Metadata for create operation, when available."""
        return self.operations.get(OperationType.CREATE)

    @property
    def update(self) -> MetaData | None:
        """Metadata for update operation, when available."""
        return self.operations.get(OperationType.UPDATE)

    @property
    def delete(self) -> MetaData | None:
        """Metadata for delete operation, when available."""
        return self.operations.get(OperationType.DELETE)


@dataclass(kw_only=True)
class IntrospectionService:
    """Load and cache entrypoint metadata for dataset lifecycle reuse."""

    entrypoint: str
    _metadata: EntryPointMetaData | None = field(default=None, init=False, repr=False)

    @property
    def metadata(self) -> EntryPointMetaData:
        """Loaded entrypoint metadata.

        Returns:
            Cached metadata snapshot for the configured entrypoint.
        """
        return self.load()

    def load(self) -> EntryPointMetaData:
        """Load metadata snapshot if needed and return cached value.

        Returns:
            Cached metadata snapshot for the configured entrypoint.
        """
        if self._metadata is None:
            self._metadata = _load_entrypoint_metadata(self.entrypoint)
        return self._metadata

    def load_metadata(self, *, operation: OperationType) -> MetaData:
        """Return metadata for a single operation.

        Args:
            operation: Operation to resolve.

        Returns:
            Metadata for the requested operation.
        """
        return self.load().get(operation=operation)


def _load_entrypoint_metadata(entrypoint: str) -> EntryPointMetaData:
    """Load all available operations for an entrypoint from packaged assets.

    Args:
        entrypoint: Dataset entrypoint (supports nested paths).

    Returns:
        EntryPointMetaData containing all discovered operations.

    Raises:
        ValidationError: If entrypoint is missing.
        NotSupportedError: If the entrypoint is unsupported.
    """
    if not entrypoint.strip():
        raise ValidationError(message="Dataset entrypoint must be provided.")

    operations: dict[OperationType, MetaData] = {}
    for operation in OperationType:
        metadata = _load_operation_metadata(entrypoint=entrypoint, operation=operation)
        if metadata is not None:
            operations[operation] = metadata

    if operations:
        return EntryPointMetaData(entrypoint=entrypoint, operations=operations)

    raise NotSupportedError(
        message=f"Entrypoint '{entrypoint}' is not supported by this provider.",
    )


def _load_operation_metadata(*, entrypoint: str, operation: OperationType) -> MetaData | None:
    """Load metadata and query template for one operation.

    Args:
        entrypoint: Dataset entrypoint.
        operation: Operation to load.

    Returns:
        Parsed MetaData when assets exist, otherwise ``None``.

    Raises:
        ValidationError: If payload is malformed.
    """
    payload_and_query = _read_operation_assets(entrypoint=entrypoint, operation=operation)
    if payload_and_query is None:
        return None
    payload, query = payload_and_query
    if not isinstance(payload, dict):
        raise ValidationError(
            message="Metadata payload is invalid: expected a JSON object.",
        )
    try:
        payload["query"] = query
        return MetaData.deserialize(payload)
    except Exception as error:
        raise ValidationError(
            message=(f"Metadata payload is invalid for entrypoint '{entrypoint}' and operation '{operation.value}': {error}"),
        ) from error


def _read_operation_assets(*, entrypoint: str, operation: OperationType) -> tuple[dict[str, Any], str] | None:
    """Read raw metadata and query files from package assets.

    Args:
        entrypoint: Dataset entrypoint.
        operation: Operation to load.

    Returns:
        Tuple of (metadata_json_dict, query_graphql_text) when found,
        otherwise ``None``.
    """
    asset_texts = _read_operation_asset_texts(entrypoint=entrypoint, operation=operation)
    if asset_texts is None:
        return None
    metadata_raw, query_raw = asset_texts
    return json.loads(metadata_raw), query_raw


@lru_cache(maxsize=256)
def _read_operation_asset_texts(*, entrypoint: str, operation: OperationType) -> tuple[str, str] | None:
    """Read raw metadata/query text files with process-local memoization.

    Args:
        entrypoint: Dataset entrypoint.
        operation: Operation to load.

    Returns:
        Tuple of (metadata_json_text, query_graphql_text) when found,
        otherwise ``None``.
    """
    candidates = (entrypoint, entrypoint.lower())
    for candidate in candidates:
        path_tokens = [token for token in candidate.split("/") if token]
        try:
            metadata_raw = (
                files("ds_provider_xledger_py_lib")
                .joinpath("assets", *path_tokens, operation.value, "metadata.json")
                .read_text(encoding="utf-8")
            )
            query_raw = (
                files("ds_provider_xledger_py_lib")
                .joinpath("assets", *path_tokens, operation.value, "query.graphql")
                .read_text(encoding="utf-8")
            )
            return metadata_raw, query_raw
        except FileNotFoundError:
            continue
    return None
