"""
**File:** ``enums.py``
**Region:** ``ds_provider_xledger_py_lib/enums``

Constants for Xledger provider.

Example:
    >>> ResourceType.DATASET_XLEDGER
    'ds.resource.dataset.xledger'
    >>> ResourceType.DATASET_ATTACHMENT
    'ds.resource.dataset.xledger_attachment'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants for Xledger provider.
    """

    LINKED_SERVICE = "ds.resource.linked_service.xledger"
    DATASET = "ds.resource.dataset.xledger"
    DATASET_ATTACHMENT = "ds.resource.dataset.xledger_attachment"


class OperationType(StrEnum):
    """
    Constants for Xledger operation types.
    """

    READ = "read"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
