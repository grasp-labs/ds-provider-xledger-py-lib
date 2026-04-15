"""
**File:** ``xledger.py``
**Region:** ``ds_provider_xledger_py_lib/dataset``

Description
-----------
Xledger dataset implementation.

This module follows the same high-level architecture as the HTTP
protocol dataset:

- linked service owns transport and authentication
- dataset owns operation intent and contract behavior
- serializer builds GraphQL query strings from tabular input
- deserializer converts API response content to ``pandas.DataFrame``
"""

from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetSettings, TabularDataset
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, DeleteError, ReadError, UpdateError
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError, ResourceException

from ..enums import ObjectStatus, OperationType, OwnerSet, ResourceType
from ..linked_service.xledger import XledgerLinkedService
from ..serde.deserializer import XledgerDeserializer
from ..serde.serializer import XledgerSerializer
from ..utils.graphql import raise_for_graphql_errors
from ..utils.introspection import IntrospectionService
from .engines.read import ReadEngine

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class XledgerReadSettings(DatasetSettings):
    """Settings for Xledger read operations."""

    first: int | None = None
    """Page size. Use ``None`` to fall back to ``metadata.pagination.first`` when set."""
    last: int | None = None
    """The last record to return."""
    before: str | None = None
    """The cursor to return the previous page of results."""
    after: str | None = None
    """The cursor to return the next page of results."""
    filter: dict[str, Any] | None = None
    """The filter to apply to the query."""
    owner_set: OwnerSet | None = None
    """The owner set to return."""
    object_status: ObjectStatus | None = None
    """The object status to return."""
    columns: list[str] | None = None
    """The columns to return."""
    limit: int | None = None
    """Dataset read-scope cap on total rows collected across paginated ``read()`` execution."""


@dataclass(kw_only=True)
class XledgerCreateSettings(DatasetSettings):
    """Settings for Xledger create operations."""

    return_columns: list[str] | None = None
    """The columns to return."""


@dataclass(kw_only=True)
class XledgerUpdateSettings(DatasetSettings):
    """Settings for Xledger update operations."""

    return_columns: list[str] | None = None
    """The columns to return."""


@dataclass(kw_only=True)
class XledgerDeleteSettings(DatasetSettings):
    """Settings for Xledger delete operations."""

    return_columns: list[str] | None = None
    """The columns to return."""


@dataclass(kw_only=True)
class XledgerDatasetSettings(DatasetSettings):
    """Settings for Xledger dataset operations."""

    entrypoint: str
    """Xledger entrypoint name targeted by dataset operations."""

    read: XledgerReadSettings = field(default_factory=XledgerReadSettings)
    """Settings for Xledger read operations."""
    create: XledgerCreateSettings = field(default_factory=XledgerCreateSettings)
    """Settings for Xledger create operations."""
    update: XledgerUpdateSettings = field(default_factory=XledgerUpdateSettings)
    """Settings for Xledger update operations."""
    delete: XledgerDeleteSettings = field(default_factory=XledgerDeleteSettings)
    """Settings for Xledger delete operations."""


XledgerDatasetSettingsType = TypeVar(
    "XledgerDatasetSettingsType",
    bound=XledgerDatasetSettings,
)
XledgerLinkedServiceType = TypeVar(
    "XledgerLinkedServiceType",
    bound=XledgerLinkedService[Any],
)


@dataclass(kw_only=True)
class XledgerDataset(
    TabularDataset[
        XledgerLinkedServiceType,
        XledgerDatasetSettingsType,
        XledgerSerializer,
        XledgerDeserializer,
    ],
    Generic[XledgerLinkedServiceType, XledgerDatasetSettingsType],
):
    """Tabular dataset for Xledger GraphQL operations."""

    linked_service: XledgerLinkedServiceType
    settings: XledgerDatasetSettingsType
    serializer: XledgerSerializer | None = field(default_factory=XledgerSerializer)
    deserializer: XledgerDeserializer | None = field(default_factory=XledgerDeserializer)
    introspection: IntrospectionService = field(init=False, repr=False, metadata={"serialize": False})

    def __post_init__(self) -> None:
        self.serializer = XledgerSerializer()
        self.deserializer = XledgerDeserializer()
        self.introspection = IntrospectionService(entrypoint=self.settings.entrypoint)
        self.introspection.load()

    @property
    def supports_checkpoint(self) -> bool:
        """Whether this dataset supports checkpointing."""
        return True

    @property
    def type(self) -> ResourceType:
        """Return the dataset resource type."""
        return ResourceType.DATASET

    def read(self) -> None:
        """Execute a GraphQL query and store the result in ``self.output``.

        Raises:
            AuthenticationError: If authentication fails.
            AuthorizationError: If authorization fails.
            ConnectionError: If the transport cannot reach the endpoint.
            ReadError: If query execution fails.
        """

        logger.debug("Sending GraphQL read requests to Xledger")
        if not self.deserializer:
            raise ReadError(
                message="Deserializer is not set",
                status_code=500,
                details={"type": self.type.value},
            )

        reader = ReadEngine(
            connection=self.linked_service.connection,
            host=self.linked_service.settings.host,
            deserializer=self.deserializer,
            metadata=self.introspection.metadata.get(operation=OperationType.READ),
        )

        try:
            reader.execute(
                read_settings=self.settings.read,
                checkpoint=self.checkpoint,
            )
        except ResourceException as exc:
            exc.details.update({"type": self.type.value})
            raise ReadError(
                message=exc.message,
                status_code=exc.status_code,
                details=exc.details,
            ) from exc
        finally:
            self.output = pd.concat(reader.output, ignore_index=True) if reader.output else pd.DataFrame()
            self.checkpoint = reader.checkpoint.serialize()

    def create(self) -> None:
        """Execute a GraphQL mutation built from ``self.input``.

        For empty input, this method is a no-op and returns successfully.

        Raises:
            AuthenticationError: If authentication fails.
            AuthorizationError: If authorization fails.
            ConnectionError: If the transport cannot reach the endpoint.
            CreateError: If mutation execution fails.
        """
        if self.input.empty:
            logger.debug("Input is empty, skipping create operation")
            self.output = self.input.copy()
            return

        if not self.serializer or not self.deserializer:
            raise CreateError(
                message="Serializer or deserializer is not set",
                status_code=500,
                details={"type": self.type.value},
            )

        logger.debug("Sending GraphQL create request to Xledger")
        try:
            payload = self.serializer(
                self.input,
                operation=OperationType.CREATE,
                metadata=self.introspection.metadata.get(operation=OperationType.CREATE),
                operation_settings=self.settings.create,
            )
            response = self.linked_service.connection.post(
                url=self.linked_service.settings.host,
                json=payload,
            )
            raise_for_graphql_errors(body=response.json())
        except ResourceException as exc:
            exc.details.update({"type": self.type.value})
            raise CreateError(
                message=exc.message,
                status_code=exc.status_code,
                details=exc.details,
            ) from exc

        logger.debug("Deserializing response to dataframe")
        self.output = self.deserializer(
            response.json(),
            metadata=self.introspection.metadata.get(operation=OperationType.CREATE),
            operation_settings=self.settings.create,
        )

    def update(self) -> None:
        """Execute a GraphQL mutation built from ``self.input``.

        For empty input, this method is a no-op and returns successfully.

        Raises:
            AuthenticationError: If authentication fails.
            AuthorizationError: If authorization fails.
            ConnectionError: If the transport cannot reach the endpoint.
            UpdateError: If update operation fails.
        """
        if self.input.empty:
            logger.debug("Input is empty, skipping update operation")
            self.output = self.input.copy()
            return

        if not self.serializer or not self.deserializer:
            raise UpdateError(
                message="Serializer or deserializer is not set",
                status_code=500,
                details={"type": self.type.value},
            )

        logger.debug("Sending GraphQL update request to Xledger")
        try:
            payload = self.serializer(
                self.input,
                operation=OperationType.UPDATE,
                metadata=self.introspection.metadata.get(operation=OperationType.UPDATE),
                operation_settings=self.settings.update,
            )
            response = self.linked_service.connection.post(
                url=self.linked_service.settings.host,
                json=payload,
            )
            raise_for_graphql_errors(body=response.json())
        except ResourceException as exc:
            exc.details.update({"type": self.type.value})
            raise UpdateError(
                message=exc.message,
                status_code=exc.status_code,
                details=exc.details,
            ) from exc

        logger.debug("Deserializing response to dataframe")
        self.output = self.deserializer(
            response.json(),
            metadata=self.introspection.metadata.get(operation=OperationType.UPDATE),
            operation_settings=self.settings.update,
        )

    def delete(self) -> None:
        """Execute a GraphQL mutation built from ``self.input``.

        For empty input, this method is a no-op and returns successfully.

        Raises:
            AuthenticationError: If authentication fails.
            AuthorizationError: If authorization fails.
            ConnectionError: If the transport cannot reach the endpoint.
            DeleteError: If delete operation fails.
        """
        if self.input.empty:
            logger.debug("Input is empty, skipping delete operation")
            self.output = self.input.copy()
            return

        if not self.serializer or not self.deserializer:
            raise DeleteError(
                message="Serializer or deserializer is not set",
                status_code=500,
                details={"type": self.type.value},
            )

        logger.debug("Sending GraphQL delete request to Xledger")
        try:
            payload = self.serializer(
                self.input,
                operation=OperationType.DELETE,
                metadata=self.introspection.metadata.get(operation=OperationType.DELETE),
                operation_settings=self.settings.delete,
            )
            response = self.linked_service.connection.post(
                url=self.linked_service.settings.host,
                json=payload,
            )
            raise_for_graphql_errors(body=response.json())
        except ResourceException as exc:
            exc.details.update({"type": self.type.value})
            raise DeleteError(
                message=exc.message,
                status_code=exc.status_code,
                details=exc.details,
            ) from exc

        logger.debug("Deserializing response to dataframe")
        self.output = self.deserializer(
            response.json(),
            metadata=self.introspection.metadata.get(operation=OperationType.DELETE),
            operation_settings=self.settings.delete,
        )

    def rename(self) -> NoReturn:
        """Rename is not supported by this dataset."""
        raise NotSupportedError("Rename operation is not supported for Xledger dataset")

    def upsert(self) -> NoReturn:
        """Upsert is not supported by this dataset."""
        raise NotSupportedError("Upsert operation is not supported for Xledger dataset")

    def purge(self) -> NoReturn:
        """Purge is not supported by this dataset."""
        raise NotSupportedError("Purge operation is not supported for Xledger dataset")

    def list(self) -> NoReturn:
        """List is not supported by this dataset."""
        raise NotSupportedError("List operation is not supported for Xledger dataset")

    def close(self) -> None:
        """Close the linked-service connection."""
        self.linked_service.close()
