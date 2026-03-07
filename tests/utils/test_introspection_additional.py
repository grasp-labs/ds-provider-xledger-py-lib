"""
**File:** ``test_introspection_additional.py``
**Region:** ``tests/utils``

Description
-----------
Additional branch and error-path tests for introspection helpers.
"""

from __future__ import annotations

import pytest
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError, ValidationError

from ds_provider_xledger_py_lib.enums import OperationType
from ds_provider_xledger_py_lib.utils.introspection import (
    EntryPointMetaData,
    MetaData,
    MetaField,
    _load_entrypoint_metadata,
    _load_operation_metadata,
    _read_operation_assets,
)


def test_load_entrypoint_metadata_raises_for_blank_entrypoint() -> None:
    """
    It raises ValidationError when entrypoint is blank.
    """
    with pytest.raises(ValidationError, match="entrypoint must be provided"):
        _load_entrypoint_metadata("  ")


def test_load_entrypoint_metadata_raises_when_no_operations_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises NotSupportedError when no operation assets exist.
    """
    monkeypatch.setattr(
        "ds_provider_xledger_py_lib.utils.introspection._load_operation_metadata",
        lambda **_: None,
    )

    with pytest.raises(NotSupportedError, match="is not supported"):
        _load_entrypoint_metadata("missing_entrypoint")


def test_entrypoint_metadata_get_raises_with_available_operations_message() -> None:
    """
    It reports available operations when requested operation is absent.
    """
    metadata = EntryPointMetaData(
        entrypoint="items",
        operations={
            OperationType.READ: MetaData(
                name="items",
                type="read",
                description="",
                fields=[MetaField(name="dbId", type="string", description="", default=True)],
                query="query { items { edges { node { dbId } } } }",
            )
        },
    )

    with pytest.raises(NotSupportedError) as exc_info:
        metadata.get(operation=OperationType.DELETE)

    assert "Available operations: read." in exc_info.value.message
    assert metadata.read is not None
    assert metadata.create is None
    assert metadata.update is None
    assert metadata.delete is None


def test_load_operation_metadata_rejects_non_object_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises ValidationError when metadata payload is not a JSON object.
    """
    monkeypatch.setattr(
        "ds_provider_xledger_py_lib.utils.introspection._read_operation_assets",
        lambda **_: (["invalid"], "query { x }"),
    )

    with pytest.raises(ValidationError, match="expected a JSON object"):
        _load_operation_metadata(entrypoint="items", operation=OperationType.READ)


def test_load_operation_metadata_wraps_deserialization_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It wraps metadata deserialization failures with entrypoint/operation context.
    """
    monkeypatch.setattr(
        "ds_provider_xledger_py_lib.utils.introspection._read_operation_assets",
        lambda **_: ({"name": "bad"}, "query { x }"),
    )

    with pytest.raises(ValidationError, match="entrypoint 'items' and operation 'read'"):
        _load_operation_metadata(entrypoint="items", operation=OperationType.READ)


def test_read_operation_assets_returns_none_when_text_files_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It returns None when raw asset text files cannot be found.
    """
    monkeypatch.setattr(
        "ds_provider_xledger_py_lib.utils.introspection._read_operation_asset_texts",
        lambda **_: None,
    )

    assert _read_operation_assets(entrypoint="items", operation=OperationType.READ) is None
