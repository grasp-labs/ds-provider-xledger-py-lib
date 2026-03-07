# ds-provider-xledger-py-lib

A Python package from the ds-common library collection.

## Installation

Install the package using pip:

```bash
pip install ds-provider-xledger-py-lib
```

Or using uv (recommended):

```bash
uv pip install ds-provider-xledger-py-lib
```

## Quick Start

```python
from ds_provider_xledger_py_lib import __version__

print(f"ds-provider-xledger-py-lib version: {__version__}")
```

## Features

- Linked service support for Xledger GraphQL authentication and connectivity checks
- Dataset operations for read, create, update, and delete workflows
- Pagination and checkpoint support for resilient read operations
- Structured GraphQL error mapping into provider-specific exceptions

## Usage

### 1) Test linked service connection

```python
from uuid import uuid4

from ds_provider_xledger_py_lib.linked_service import (
    XledgerLinkedService,
    XledgerLinkedServiceSettings,
)

linked_service = XledgerLinkedService(
    id=uuid4(),
    name="xledger-linked-service",
    version="1.0.0",
    settings=XledgerLinkedServiceSettings(
        host="https://demo.xledger.net/graphql",
        token="YOUR_XLEDGER_TOKEN",
        timeout=60,
    ),
)

success, message = linked_service.test_connection()
print(success, message)
```

### 2) Read data from an entrypoint

```python
from uuid import uuid4

from ds_provider_xledger_py_lib.dataset import XledgerDataset, XledgerDatasetSettings
from ds_provider_xledger_py_lib.dataset.xledger import XledgerReadSettings
from ds_provider_xledger_py_lib.linked_service import XledgerLinkedService, XledgerLinkedServiceSettings

linked_service = XledgerLinkedService(
    id=uuid4(),
    name="xledger-linked-service",
    version="1.0.0",
    settings=XledgerLinkedServiceSettings(
        host="https://demo.xledger.net/graphql",
        token="YOUR_XLEDGER_TOKEN",
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
            columns=["dbId", "code", "description"],
            pagination=True,
            first=1000,
        ),
    ),
)

linked_service.connect()
dataset.read()
print(dataset.output.head())
```

### 3) Update rows

```python
from uuid import uuid4

import pandas as pd
from ds_provider_xledger_py_lib.dataset import XledgerDataset, XledgerDatasetSettings
from ds_provider_xledger_py_lib.dataset.xledger import XledgerUpdateSettings
from ds_provider_xledger_py_lib.linked_service import XledgerLinkedService, XledgerLinkedServiceSettings

linked_service = XledgerLinkedService(
    id=uuid4(),
    name="xledger-linked-service",
    version="1.0.0",
    settings=XledgerLinkedServiceSettings(
        host="https://demo.xledger.net/graphql",
        token="YOUR_XLEDGER_TOKEN",
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
            return_columns=["dbId", "code", "description"],
        ),
    ),
)

dataset.input = pd.DataFrame(
    [{"dbId": 1, "code": "EMP-001", "description": "Updated employee"}]
)
linked_service.connect()
dataset.update()
print(dataset.output)
```

For complete runnable scripts, see the repository's `examples/` folder.

## Requirements

- Python 3.11 or higher

## Optional Dependencies

- ds-protocol-http-py-lib>=0.1.0-beta.4,<1.0.0
- ds-resource-plugin-py-lib>=0.1.0-rc.2,<1.0.0
- ds-common-logger-py-lib>=0.1.0-alpha.5,<1.0.0

## Documentation

Full documentation is available at:

- [GitHub Repository](https://github.com/grasp-labs/ds-provider-xledger-py-lib)
- [Documentation Site](https://grasp-labs.github.io/ds-provider-xledger-py-lib/)

## Development

To contribute or set up a development environment:

```bash
# Clone the repository
git clone https://github.com/grasp-labs/ds-provider-xledger-py-lib.git
cd ds-provider-xledger-py-lib

# Install development dependencies
uv sync --all-extras --dev

# Run tests
make test
```

See the [README](https://github.com/grasp-labs/ds-provider-xledger-py-lib#readme)
for more information.

## License

This package is licensed under the Apache License 2.0.
See the [LICENSE-APACHE](https://github.com/grasp-labs/ds-provider-xledger-py-lib/blob/main/LICENSE-APACHE)
file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/grasp-labs/ds-provider-xledger-py-lib/issues)
- **Releases**: [GitHub Releases](https://github.com/grasp-labs/ds-provider-xledger-py-lib/releases)
