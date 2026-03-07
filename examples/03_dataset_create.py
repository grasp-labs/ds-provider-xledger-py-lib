"""
**File:** ``03_dataset_create.py``
**Region:** ``examples/03_dataset_create``

Example 03: Initialize Xledger dataset and run create workflow.

This example demonstrates how to:
- Create and connect a Xledger linked service
- Create a Xledger dataset for a configured entrypoint
- Populate ``dataset.input`` with rows to create
- Execute ``create()`` and inspect ``dataset.output``
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
from ds_provider_xledger_py_lib.dataset.xledger import XledgerCreateSettings
from ds_provider_xledger_py_lib.linked_service import XledgerLinkedService, XledgerLinkedServiceSettings

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)

load_dotenv()


def main() -> None:
    """Main function demonstrating Xledger employee create setup."""
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
        name="xledger-dataset-create",
        version="1.0.0",
        linked_service=linked_service,
        settings=XledgerDatasetSettings(
            entrypoint="employees",
            create=XledgerCreateSettings(
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
                    "code": "JON-DOE",
                    "description": "Jon Doe - Software Consultant",
                    "firstName": "Jon",
                    "lastName": "Doe",
                    "companyName": "Grasp Labs",
                    "phone": "+47 400 00 000",
                    "contactPhoneList": "+47 400 00 000",
                    "contactEmailList": "jon.doe@example.com",
                    "contactAddress": "Karl Johans gate 1, 0154 Oslo",
                    "dateFrom": "2026-01-01",
                    "employmentFrom": "2026-01-01",
                    "overview": "Primary consultant profile created from SDK example",
                }
            ]
        )
        dataset.create()
    except ResourceException as exc:
        logger.error("Dataset create failed: %s", exc.message)
        logger.error("Exception: %s", exc.__dict__)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        raise
    finally:
        linked_service.close()

    logger.debug("Dataset create completed. Output type: %s", type(dataset.output).__name__)
    logger.debug("Output: %s", dataset.output.head())


if __name__ == "__main__":
    main()
