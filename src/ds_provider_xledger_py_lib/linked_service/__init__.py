"""
**File:** ``__init__.py``
**Region:** ``ds_provider_xledger_py_lib/linked_service``

Description
-----------
This module implements a linked service for Xledger GraphQL API.
"""

from .xledger import XledgerLinkedService, XledgerLinkedServiceSettings

__all__ = [
    "XledgerLinkedService",
    "XledgerLinkedServiceSettings",
]
