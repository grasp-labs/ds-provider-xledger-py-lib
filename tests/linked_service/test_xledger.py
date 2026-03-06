"""
**File:** ``test_xledger.py``
**Region:** ``tests/linked_service``

Description
-----------
Tests for the Xledger linked-service implementation.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import uuid4

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    ConnectionError,
)

from ds_provider_xledger_py_lib.enums import ResourceType
from ds_provider_xledger_py_lib.linked_service.xledger import (
    XledgerLinkedService,
    XledgerLinkedServiceSettings,
)


class _FakeHttp:
    def __init__(
        self,
        *,
        should_raise: bool = False,
        response_body: dict[str, object] | None = None,
    ) -> None:
        self.should_raise = should_raise
        self.response_body = response_body or {"data": {"__typename": "Query"}}
        self.closed = False
        self.session = type("Session", (), {"headers": {}})()

    def post(self, *, url: str, json: dict[str, object]) -> object:
        _ = (url, json)
        if self.should_raise:
            raise RuntimeError("probe failed")
        response_body = self.response_body

        class _FakeResponse:
            def json(self) -> dict[str, object]:
                return response_body

        return _FakeResponse()

    def close(self) -> None:
        self.closed = True


def test_connect_sets_authorization_header() -> None:
    """
    It sets the Xledger token header when connecting.
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

    assert linked_service.connection.session.headers["Authorization"] == "token token-value"
    assert linked_service.connection.session.headers["Content-Type"] == "application/json"


def test_type_returns_linked_service_resource_type() -> None:
    """
    It exposes the expected linked-service ResourceType.
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

    assert linked_service.type == ResourceType.LINKED_SERVICE


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


def test_connect_is_idempotent_and_reuses_http_instance() -> None:
    """
    It does not recreate the HTTP provider on repeated connect calls.
    """
    linked_service = XledgerLinkedService(
        id=uuid4(),
        name="xledger",
        version="1.0.0",
        settings=XledgerLinkedServiceSettings(
            host="https://demo.xledger.net/graphql",
            token="first-token",
        ),
    )

    linked_service._http = cast("Any", _FakeHttp())
    initial_http = linked_service._http
    linked_service.connect()

    linked_service.settings.token = "second-token"
    linked_service.connect()

    assert linked_service._http is initial_http
    assert linked_service.connection.session.headers["Authorization"] == "token second-token"


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


def test_test_connection_returns_true_on_probe_success() -> None:
    """
    It returns (True, "") when the probe request succeeds.
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
    linked_service._http = cast("Any", _FakeHttp())

    result = linked_service.test_connection()

    assert result == (True, "")


def test_test_connection_returns_false_on_probe_error() -> None:
    """
    It returns (False, error) when the probe request fails.
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
    linked_service._http = cast("Any", _FakeHttp(should_raise=True))

    ok, message = linked_service.test_connection()

    assert ok is False
    assert "probe failed" in message


def test_test_connection_returns_false_on_graphql_payload_errors() -> None:
    """
    It returns (False, error) when GraphQL payload reports errors.
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
    linked_service._http = cast(
        "Any",
        _FakeHttp(
            response_body={
                "errors": [
                    {
                        "message": "Bad format for API Token",
                    }
                ]
            }
        ),
    )

    ok, message = linked_service.test_connection()

    assert ok is False
    assert "Bad format for API Token" in message


def test_close_closes_http_and_resets_connection() -> None:
    """
    It closes the underlying HTTP provider and resets internal state.
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
    fake_http = _FakeHttp()
    linked_service._http = cast("Any", fake_http)

    linked_service.close()

    assert fake_http.closed is True
    assert linked_service._http is None
