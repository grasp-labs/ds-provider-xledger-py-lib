"""
**File:** ``test_introspection.py``
**Region:** ``tests/utils``

Description
-----------
Unit tests for introspection asset loading, caching, and unsupported entrypoints.
"""

from __future__ import annotations

import pytest
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError

from ds_provider_xledger_py_lib.enums import OperationType
from ds_provider_xledger_py_lib.utils.introspection import IntrospectionService


def test_load_reads_packaged_asset_and_caches() -> None:
    """
    It loads metadata once, reuses cache, and resolves read/create contracts.
    """
    service = IntrospectionService(
        entrypoint="customers",
    )

    first = service.load()
    second = service.load()

    assert first is second
    assert first.entrypoint == "customers"

    read_meta = first.get(operation=OperationType.READ)
    assert read_meta.name == "customers"
    assert read_meta.type == "read"
    assert any(field.name == "dbId" and field.default for field in read_meta.fields)

    create_meta = service.load_metadata(operation=OperationType.CREATE)
    assert create_meta.name == "addCustomers"
    assert create_meta.type == "create"
    assert isinstance(create_meta.fields, list)
    assert any(field.name == "company_code" for field in create_meta.fields)


def test_load_raises_when_entrypoint_asset_is_missing() -> None:
    """
    It raises NotSupportedError when the configured entrypoint has no assets.
    """
    service = IntrospectionService(
        entrypoint="definitely_missing_entrypoint",
    )
    with pytest.raises(NotSupportedError):
        service.load()
