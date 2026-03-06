from __future__ import annotations

from uuid import uuid4

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    ConnectionError,
)

from ds_provider_xledger_py_lib.linked_service.xledger import (
    XledgerLinkedService,
    XledgerLinkedServiceSettings,
)


def test_connect_sets_authorization_header() -> None:
    """
    It sets the bearer token header when connecting.
    """
    linked_service = XledgerLinkedService(
        id=uuid4(),
        name="xledger",
        version="1.0.0",
        settings=XledgerLinkedServiceSettings(
            host="https://demo.xledger.net/graphql",
            token="token-value",
        ),
    )

    linked_service.connect()

    assert linked_service.connection.session.headers["Authorization"] == "Bearer token-value"
    assert linked_service.connection.session.headers["Content-Type"] == "application/json"


def test_connect_raises_authentication_error_when_token_missing() -> None:
    """
    It raises AuthenticationError when token is empty.
    """
    linked_service = XledgerLinkedService(
        id=uuid4(),
        name="xledger",
        version="1.0.0",
        settings=XledgerLinkedServiceSettings(
            host="https://demo.xledger.net/graphql",
            token=" ",
        ),
    )

    with pytest.raises(AuthenticationError):
        linked_service.connect()


def test_connection_raises_before_connect() -> None:
    """
    It raises ConnectionError before connect() has initialized a session.
    """
    linked_service = XledgerLinkedService(
        id=uuid4(),
        name="xledger",
        version="1.0.0",
        settings=XledgerLinkedServiceSettings(
            host="https://demo.xledger.net/graphql",
            token="token-value",
        ),
    )

    with pytest.raises(ConnectionError):
        _ = linked_service.connection
