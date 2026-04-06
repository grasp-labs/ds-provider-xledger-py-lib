"""
**File:** ``test_assets_contract.py``
**Region:** ``tests/assets``

Description
-----------
Contract tests for packaged metadata and GraphQL query assets.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import ds_provider_xledger_py_lib
from ds_provider_xledger_py_lib.enums import OperationType
from ds_provider_xledger_py_lib.utils.introspection import MetaData

ASSETS_ROOT = Path(ds_provider_xledger_py_lib.__file__).resolve().parent / "assets"
SUPPORTED_OPERATIONS = {item.value for item in OperationType}


def _discover_operation_dirs() -> list[Path]:
    """Return all operation directories that contain packaged assets."""
    return sorted(path for path in ASSETS_ROOT.rglob("*") if path.is_dir() and path.name in SUPPORTED_OPERATIONS)


def _discover_read_operation_dirs() -> list[Path]:
    """Return operation directories specifically for read contracts."""
    return [path for path in _discover_operation_dirs() if path.name == OperationType.READ.value]


def _read_text(path: Path) -> str:
    """Read UTF-8 text from an asset file path."""
    return path.read_text(encoding="utf-8")


def _remove_known_placeholders(query: str) -> str:
    """Remove supported template placeholders before structural validation."""
    return query.replace("{{ FIELDS }}", "field").replace("{{ DBIDS }}", "[]")


def _assert_balanced_braces(query: str) -> None:
    """Assert brace pairs are balanced after placeholder normalization."""
    normalized = _remove_known_placeholders(query)
    assert normalized.count("{") == normalized.count("}")


@pytest.mark.parametrize("operation_dir", _discover_operation_dirs(), ids=lambda path: str(path.relative_to(ASSETS_ROOT)))
def test_each_operation_directory_contains_required_asset_files(operation_dir: Path) -> None:
    """
    It ensures every operation directory contains both metadata and query assets.
    """
    assert (operation_dir / "metadata.json").exists()
    assert (operation_dir / "query.graphql").exists()


@pytest.mark.parametrize("operation_dir", _discover_operation_dirs(), ids=lambda path: str(path.relative_to(ASSETS_ROOT)))
def test_all_metadata_files_are_valid_and_consistent(operation_dir: Path) -> None:
    """
    It validates metadata JSON structure and operation consistency.
    """
    metadata_path = operation_dir / "metadata.json"
    operation = operation_dir.name
    raw = _read_text(metadata_path)
    parsed = json.loads(raw)

    assert isinstance(parsed, dict)
    assert parsed.get("type") == operation
    assert isinstance(parsed.get("name"), str) and parsed["name"].strip()
    assert isinstance(parsed.get("description"), str)
    assert isinstance(parsed.get("fields"), list)

    for field in parsed["fields"]:
        assert isinstance(field, dict)
        assert isinstance(field.get("name"), str) and field["name"].strip()
        assert isinstance(field.get("type"), str) and field["type"].strip()
        assert isinstance(field.get("description"), str)


@pytest.mark.parametrize("operation_dir", _discover_operation_dirs(), ids=lambda path: str(path.relative_to(ASSETS_ROOT)))
def test_all_metadata_files_are_deserializable(operation_dir: Path) -> None:
    """
    It validates that every metadata payload can be deserialized into MetaData.
    """
    metadata_payload = json.loads(_read_text(operation_dir / "metadata.json"))
    query = _read_text(operation_dir / "query.graphql")

    deserialized = MetaData.deserialize(
        {
            **metadata_payload,
            "query": query,
        }
    )

    assert deserialized.type == operation_dir.name
    assert deserialized.query == query


@pytest.mark.parametrize("operation_dir", _discover_operation_dirs(), ids=lambda path: str(path.relative_to(ASSETS_ROOT)))
def test_all_query_files_are_non_empty_and_structurally_valid(operation_dir: Path) -> None:
    """
    It validates base GraphQL query file health for all operation assets.
    """
    query_path = operation_dir / "query.graphql"
    query = _read_text(query_path)

    assert query.strip(), f"Query file is empty: {query_path}"
    assert "{{ FIELDS }}" in query or "{{ DBIDS }}" in query
    _assert_balanced_braces(query)


@pytest.mark.parametrize("operation_dir", _discover_read_operation_dirs(), ids=lambda path: str(path.relative_to(ASSETS_ROOT)))
def test_read_queries_follow_connection_pattern(operation_dir: Path) -> None:
    """
    It enforces the expected connection-style structure for read queries.
    """
    metadata = json.loads(_read_text(operation_dir / "metadata.json"))
    query = _read_text(operation_dir / "query.graphql")
    normalized = _remove_known_placeholders(query)
    pagination_first = metadata["pagination"]["first"]

    assert isinstance(pagination_first, int) and pagination_first > 0
    assert re.search(rf"{re.escape(metadata['name'])}\s*\(\s*\)\s*\{{", normalized), (
        f"Read query must use an empty connection argument list; page size is "
        f"metadata.pagination.first (here {pagination_first}) applied at runtime for {operation_dir}"
    )
    assert "first:" not in normalized, (
        f"Do not embed first in read query templates; use metadata.pagination.first for {operation_dir}"
    )
    assert metadata["name"] in normalized, f"Metadata name not found in query for {operation_dir}"
    assert "pageInfo" in normalized
    assert "hasNextPage" in normalized
    assert "hasPreviousPage" in normalized
    assert "edges" in normalized
    assert "cursor" in normalized
    assert "node" in normalized


@pytest.mark.parametrize("operation_dir", _discover_read_operation_dirs(), ids=lambda path: str(path.relative_to(ASSETS_ROOT)))
def test_read_metadata_uses_normalized_instruction_contract(operation_dir: Path) -> None:
    """
    It enforces normalized read instruction keys across read metadata assets.
    """
    metadata = json.loads(_read_text(operation_dir / "metadata.json"))

    assert metadata.get("pattern") in {"direct", "delta"}

    incremental = metadata.get("incremental")
    assert incremental is None or isinstance(incremental, dict)
    if isinstance(incremental, dict):
        assert incremental.get("kind") in {"time_field", "token", "version"}
        assert isinstance(incremental.get("field"), str) and incremental["field"].strip()
        assert isinstance(incremental.get("filter_field"), str) and incremental["filter_field"].strip()

    pagination = metadata.get("pagination")
    assert isinstance(pagination, dict)
    assert pagination.get("kind") in {"cursor", "page", "offset"}
