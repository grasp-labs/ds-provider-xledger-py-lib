"""
**File:** ``test_enums.py``
**Region:** ``tests``

Description
-----------
Unit tests for provider enums and string-based enum semantics.
"""

from __future__ import annotations

from ds_provider_xledger_py_lib.enums import ResourceType


def test_resource_type_linked_service_value() -> None:
    """
    It exposes the correct linked service type value.
    """
    assert ResourceType.LINKED_SERVICE == "ds.resource.linked-service.xledger"
    assert isinstance(ResourceType.LINKED_SERVICE, str)


def test_resource_type_dataset_value() -> None:
    """
    It exposes the correct dataset type value.
    """
    assert ResourceType.DATASET == "ds.resource.dataset.xledger"
    assert ResourceType.DATASET_ATTACHMENT == "ds.resource.dataset.xledger-attachment"
    assert isinstance(ResourceType.DATASET, str)


def test_resource_type_enum_membership() -> None:
    """
    It allows checking enum membership.
    """
    assert ResourceType.LINKED_SERVICE in ResourceType
    assert ResourceType.DATASET in ResourceType
    assert ResourceType.DATASET_ATTACHMENT in ResourceType


def test_resource_type_enum_comparison() -> None:
    """
    It supports equality comparison with strings.
    """
    assert ResourceType.LINKED_SERVICE == "ds.resource.linked-service.xledger"
    assert ResourceType.DATASET == "ds.resource.dataset.xledger"
    assert ResourceType.DATASET_ATTACHMENT == "ds.resource.dataset.xledger-attachment"
    assert ResourceType.LINKED_SERVICE != ResourceType.DATASET
    assert ResourceType.LINKED_SERVICE != ResourceType.DATASET_ATTACHMENT
