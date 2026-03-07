"""
**File:** ``05_dataset_delete.py``
**Region:** ``examples/05_dataset_delete``

Example 05: Initialize Xledger dataset and run delete workflow.

This example demonstrates how to:
- Create and connect a Xledger linked service
- Create a Xledger dataset for a configured entrypoint
- Populate ``dataset.input`` with rows to delete
- Execute ``delete()`` and inspect ``dataset.output``
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
from ds_provider_xledger_py_lib.dataset.xledger import XledgerDeleteSettings
from ds_provider_xledger_py_lib.linked_service import XledgerLinkedService, XledgerLinkedServiceSettings

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)

load_dotenv()


def main() -> None:
    """Main function demonstrating Xledger bank account delete setup."""
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
        name="xledger-dataset-delete",
        version="1.0.0",
        linked_service=linked_service,
        settings=XledgerDatasetSettings(
            entrypoint="bankAccounts",
            delete=XledgerDeleteSettings(
                return_columns=[
                    "numAffected",
                    "skippedDbIds",
                ]
            ),
        ),
    )
    try:
        linked_service.connect()
        dataset.input = pd.DataFrame(
            [
                {
                    "dbId": "123456",
                }
            ]
        )
        dataset.delete()
    except ResourceException as exc:
        logger.error("Dataset delete failed: %s", exc.message)
        logger.error("Exception: %s", exc.__dict__)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        raise
    finally:
        linked_service.close()

    logger.debug("Dataset delete completed. Output type: %s", type(dataset.output).__name__)
    logger.debug("Output: %s", dataset.output.head())


if __name__ == "__main__":
    main()
