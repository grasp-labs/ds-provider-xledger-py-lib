"""
**File:** ``__init__.py``
**Region:** ``ds-provider-xledger-py-lib``

Description
-----------
A Python package from the ds-provider-xledger-py-lib library.

Example
-------
.. code-block:: python

    from ds_provider_xledger_py_lib import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

__version__ = version("ds-provider-xledger-py-lib")
__all__ = ["__version__"]
