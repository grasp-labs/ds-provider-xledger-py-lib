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
    """Read operation"""
    CREATE = "create"
    """Create operation"""
    UPDATE = "update"
    """Update operation"""
    DELETE = "delete"
    """Delete operation"""


class ObjectStatus(StrEnum):
    """
    Constants for Xledger object status.
    """

    ALL = "ALL"
    """All objects"""
    OPEN = "OPEN"
    """Open objects"""
    CLOSED = "CLOSED"
    """Closed objects"""


class OwnerSet(StrEnum):
    """
    Constants for Xledger owner set.
    """

    CURRENT = "CURRENT"
    """Current owner set"""
    UPPER = "UPPER"
    """Upper owner set"""
    LOWER = "LOWER"
    """Lower owner set"""
    MINE = "MINE"
    """Mine owner set"""
    ALL = "ALL"
    """All owner sets"""
