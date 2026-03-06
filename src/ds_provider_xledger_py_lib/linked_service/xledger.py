"""Xledger linked service implementation.

This module provides a linked service for the Xledger GraphQL API. It
reuses the shared HTTP protocol provider for retries, timeouts, and
rate limiting, while exposing a provider-specific settings model.

Example:
    >>> linked_service = XledgerLinkedService(
    ...     settings=XledgerLinkedServiceSettings(
    ...         host="https://demo.xledger.net/graphql",
    ...         token="my-token",
    ...         timeout=60,
    ...     ),
    ... )
    >>> linked_service.connect()
    >>> linked_service.connection.post(
    ...     linked_service.settings.host,
    ...     json={"query": "query { __typename }"},
    ... )
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_protocol_http_py_lib.utils.http.config import HttpConfig, RetryConfig
from ds_protocol_http_py_lib.utils.http.provider import Http
from ds_protocol_http_py_lib.utils.http.token_bucket import TokenBucket
from ds_resource_plugin_py_lib.common.resource.linked_service import (
    LinkedService,
    LinkedServiceSettings,
)
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    ConnectionError,
)

from .. import __version__
from ..enums import ResourceType
from ..utils.graphql import raise_for_graphql_errors


@dataclass(kw_only=True)
class XledgerLinkedServiceSettings(LinkedServiceSettings):
    """Connection settings for Xledger GraphQL API.

    Attributes:
        host: Full GraphQL endpoint URL.
        token: API token used for Xledger ``Authorization`` header.
        headers: Optional additional default headers.
        timeout: Default request timeout in seconds.
    """

    host: str = field(default="https://demo.xledger.net/graphql")
    token: str = field(metadata={"mask": True})
    headers: dict[str, str] | None = None
    timeout: int | float = 60


XledgerLinkedServiceSettingsType = TypeVar(
    "XledgerLinkedServiceSettingsType",
    bound=XledgerLinkedServiceSettings,
)


@dataclass(kw_only=True)
class XledgerLinkedService(
    LinkedService[XledgerLinkedServiceSettingsType],
    Generic[XledgerLinkedServiceSettingsType],
):
    """Linked service for Xledger GraphQL API."""

    settings: XledgerLinkedServiceSettingsType

    _http: Http | None = field(default=None, init=False, repr=False, metadata={"serialize": False})

    @property
    def type(self) -> ResourceType:
        """Return the resource type for this linked service.

        Returns:
            ResourceType: The linked-service resource identifier enum.
        """
        return ResourceType.LINKED_SERVICE

    @property
    def connection(self) -> Http:
        """Return the established HTTP provider instance.

        Returns:
            Http: Configured HTTP provider used by datasets.

        Raises:
            ConnectionError: If ``connect()`` has not been called.
        """
        if self._http is None:
            raise ConnectionError(
                message="Session is not initialized",
                details={"type": self.type.value},
            )
        return self._http

    def _init_http(self) -> Http:
        """Build the internal HTTP provider.

        Returns:
            Http: A configured HTTP provider instance with retry and
            token-bucket rate limiting enabled.
        """
        retry_config = RetryConfig(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "DELETE", "PATCH"),
            raise_on_status=True,
            respect_retry_after_header=True,
        )
        config = HttpConfig(
            headers=dict(self.settings.headers or {}),
            timeout_seconds=self.settings.timeout,
            user_agent=f"ds-provider-xledger-py-lib/{__version__}",
            retry=retry_config,
        )
        token_bucket = TokenBucket(rps=10, capacity=20)
        return Http(config=config, bucket=token_bucket)

    def connect(self) -> None:
        """Connect to Xledger by preparing an authenticated HTTP session.

        This method is idempotent. It initializes the HTTP provider on
        first use and updates default headers with bearer authentication.

        Raises:
            AuthenticationError: If the token is missing or blank.
        """
        token = self.settings.token.strip()
        if not token:
            raise AuthenticationError(
                message="Xledger token is missing",
                details={"type": self.type.value, "host": self.settings.host},
            )

        if self._http is None:
            self._http = self._init_http()

        self._http.session.headers.update(
            {
                "Authorization": f"token {token}",
                "Content-Type": "application/json",
            }
        )

    def test_connection(self) -> tuple[bool, str]:
        """Run a lightweight GraphQL probe to verify connectivity.

        Returns:
            tuple[bool, str]: ``(True, "")`` on success, otherwise
            ``(False, error_message)``.
        """
        try:
            if self._http is None:
                self.connect()

            response = self.connection.post(
                url=self.settings.host,
                json={"query": "query { __typename }"},
            )
            raise_for_graphql_errors(body=response.json())
            return True, ""
        except Exception as exc:
            return False, str(exc)

    def close(self) -> None:
        """Close and clear the underlying HTTP provider.

        This method is safe to call multiple times.
        """
        if self._http is not None:
            self._http.close()
            self._http = None
