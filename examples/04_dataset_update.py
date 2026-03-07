"""
**File:** ``04_dataset_update.py``
**Region:** ``examples/04_dataset_update``

Example 04: Initialize Xledger dataset and run update workflow.

This example demonstrates how to:
- Create and connect a Xledger linked service
- Create a Xledger dataset for a configured entrypoint
- Populate ``dataset.input`` with rows to update
- Execute ``update()`` and inspect ``dataset.output``
"""

from __future__ import annotations

import logging
import os
from uuid import uuid4

from dotenv import load_dotenv
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException
import pandas as pd

from ds_provider_xledger_py_lib.dataset import XledgerDataset, XledgerDatasetSettings
from ds_provider_xledger_py_lib.dataset.xledger import XledgerUpdateSettings
from ds_provider_xledger_py_lib.linked_service import XledgerLinkedService, XledgerLinkedServiceSettings

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)

load_dotenv()


def main() -> None:
    """Main function demonstrating Xledger employee update setup."""
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
        name="xledger-dataset-update",
        version="1.0.0",
        linked_service=linked_service,
        settings=XledgerDatasetSettings(
            entrypoint="employees",
            update=XledgerUpdateSettings(
                return_columns=[
                    "dbId",
                    "code",
                    "description",
                ]
            ),
        ),
    )
    try:
        linked_service.connect()
        dataset.input = pd.DataFrame(
            [
                {
                    "dbId": 52259057,
                    "code": "JON-DOE",
                    "description": "Jon Doe (Updated from SDK example)",
                }
            ]
        )
        dataset.update()
    except ResourceException as exc:
        logger.error("Dataset update failed: %s", exc.message)
        logger.error("Exception: %s", exc.__dict__)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        raise
    finally:
        linked_service.close()

    logger.debug("Dataset update completed. Output type: %s", type(dataset.output).__name__)
    logger.debug("Output: %s", dataset.output.head())


if __name__ == "__main__":
    main()
