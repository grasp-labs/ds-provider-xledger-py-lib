"""
**File:** ``01_linked_service_connect.py``
**Region:** ``examples/01_linked_service_connect``

Example 01: Connect to Xledger with GraphQL using a linked service.

This example demonstrates how to:
- Create a Xledger linked service
- Creates a connection using GraphQL
- Test the connection
"""

from __future__ import annotations

import logging
from uuid import uuid4

from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException

from ds_provider_xledger_py_lib.linked_service.xledger import (
    XledgerLinkedService,
    XledgerLinkedServiceSettings,
)

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)


def main() -> None:
    """Main function demonstrating Xledger linked service connection."""
    linked_service = XledgerLinkedService(
        id=uuid4(),
        name="test-name",
        version="1.0.0",
        settings=XledgerLinkedServiceSettings(
            host="https://demo.xledger.net/graphql",
            token="your_token",
            timeout=60,
        ),
    )

    try:
        logger.debug("Testing connection to Xledger GraphQL API...")
        success, message = linked_service.test_connection()
        if success:
            logger.debug("Connection test successful: %s", message)
        else:
            raise ResourceException(message=message)
    except ResourceException as exc:
        logger.error("Failed to connect to Xledger GraphQL API: %s", exc.message)
        logger.error("Exception: %s", exc.__dict__)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        raise


if __name__ == "__main__":
    main()
