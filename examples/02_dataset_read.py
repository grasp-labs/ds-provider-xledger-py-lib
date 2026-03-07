"""
**File:** ``02_dataset_read.py``
**Region:** ``examples/02_dataset_read``

Example 02: Initialize Xledger dataset and run read workflow.

This example demonstrates how to:
- Create and connect a Xledger linked service
- Create a Xledger dataset for a configured entrypoint
- Inspect introspected query entrypoints loaded in ``__post_init__``
- Execute ``read()`` using the current dataset implementation
"""

from __future__ import annotations

import logging
import os
from uuid import uuid4

from dotenv import load_dotenv
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException

from ds_provider_xledger_py_lib.dataset import XledgerDataset, XledgerDatasetSettings
from ds_provider_xledger_py_lib.dataset.xledger import XledgerReadSettings
from ds_provider_xledger_py_lib.linked_service import XledgerLinkedService, XledgerLinkedServiceSettings

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)

load_dotenv()


def main() -> None:
    """Main function demonstrating Xledger dataset read setup."""
    linked_service = XledgerLinkedService(
        id=uuid4(),
        name="xledger-linked-service",
        version="1.0.0",
        settings=XledgerLinkedServiceSettings(
            host="https://demo.xledger.net/graphql",
            token=os.getenv("XLEDGER_TOKEN", ""),
            timeout=60,
        ),
    )

    dataset = XledgerDataset(
        id=uuid4(),
        name="xledger-dataset-read",
        version="1.0.0",
        linked_service=linked_service,
        settings=XledgerDatasetSettings(
            entrypoint="employees",
            read=XledgerReadSettings(
                columns=[
                    "dbId",
                    "code",
                    "description",
                ],
                pagination=True,
                first=1000,
            ),
        ),
    )
    try:
        linked_service.connect()
        dataset.read()
    except ResourceException as exc:
        logger.error("Dataset read failed: %s", exc.message)
        logger.error("Exception: %s", exc.__dict__)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        raise
    finally:
        linked_service.close()

    logger.debug("Dataset read completed. Output type: %s", type(dataset.output).__name__)
    logger.debug("Checkpoint: %s", dataset.checkpoint)
    logger.debug("Output: %s", dataset.output)


if __name__ == "__main__":
    main()
