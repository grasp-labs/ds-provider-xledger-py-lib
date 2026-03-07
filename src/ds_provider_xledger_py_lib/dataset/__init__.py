"""
**File:** ``__init__.py``
**Region:** ``ds_provider_xledger_py_lib/dataset``

Description
-----------
This module implements a dataset for Xledger GraphQL APIs.
"""

from .xledger import XledgerDataset, XledgerDatasetSettings

__all__ = [
    "XledgerDataset",
    "XledgerDatasetSettings",
]
