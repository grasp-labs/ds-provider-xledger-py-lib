"""
**File:** ``test_graphql_utils.py``
**Region:** ``tests``

Description
-----------
Unit tests for GraphQL error normalization helpers.
"""

from __future__ import annotations

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError

from ds_provider_xledger_py_lib.errors import (
    BurstRequestException,
    ConcurrentRequestException,
    InvalidMutationException,
    InvalidQueryException,
    OutOfCreditException,
    TimeOutException,
    UnhandledXledgerException,
)
from ds_provider_xledger_py_lib.utils.graphql import (
    map_graphql_errors_to_exception,
    raise_for_graphql_errors,
)


def test_raise_for_graphql_errors_returns_body_when_valid() -> None:
    """
    It returns the body unchanged when no GraphQL errors exist.
    """
    body = {"data": {"ok": True}}

    result = raise_for_graphql_errors(
        body=body,
    )

    assert result == body


def test_raise_for_graphql_errors_raises_for_non_object_body() -> None:
    """
    It raises UnhandledXledgerException for invalid response body shape.
    """
    with pytest.raises(UnhandledXledgerException, match="response is not a JSON object"):
        raise_for_graphql_errors(
            body=["not-an-object"],
        )


def test_raise_for_graphql_errors_maps_invalid_query() -> None:
    """
    It maps invalid query-like failures to InvalidQueryException.
    """
    with pytest.raises(InvalidQueryException, match=r"Invalid Xledger query\."):
        raise_for_graphql_errors(
            body={"errors": [{"message": "Cannot query field `foo` on type `Query`"}]},
        )


def test_raise_for_graphql_errors_maps_invalid_mutation() -> None:
    """
    It maps invalid mutation-like failures to InvalidMutationException.
    """
    with pytest.raises(InvalidMutationException, match=r"Invalid Xledger argument\."):
        raise_for_graphql_errors(
            body={
                "errors": [
                    {
                        "message": "Invalid mutation payload",
                        "extensions": {"code": "INVALID_VALUE"},
                    }
                ]
            },
        )


def test_raise_for_graphql_errors_maps_bad_api_token_to_authentication_error() -> None:
    """
    It maps API token format payload errors to AuthenticationError.
    """
    with pytest.raises(AuthenticationError, match="Bad format for API Token"):
        raise_for_graphql_errors(
            body={
                "errors": [
                    {
                        "message": 'Bad format for API Token. Expected header: "Authorization: token <your-token>"',
                    }
                ]
            },
        )


@pytest.mark.parametrize(
    ("error_payload", "expected_exception"),
    [
        (
            {
                "message": "Not enough credits",
                "code": "BAD_REQUEST.INSUFFICIENT_CREDITS",
            },
            OutOfCreditException,
        ),
        (
            {
                "message": "Your query timed out.",
            },
            TimeOutException,
        ),
        (
            {
                "message": "Too many requests",
                "code": "BAD_REQUEST.CONCURRENCY_LIMIT_REACHED",
            },
            ConcurrentRequestException,
        ),
        (
            {
                "message": "Too many requests",
                "code": "BAD_REQUEST.BURST_RATE_LIMIT_REACHED",
            },
            BurstRequestException,
        ),
    ],
)
def test_raise_for_graphql_errors_maps_domain_errors(
    error_payload: dict[str, str],
    expected_exception: type[Exception],
) -> None:
    """
    It maps common Xledger domain failures to typed dataset exceptions.
    """
    with pytest.raises(expected_exception):
        raise_for_graphql_errors(
            body={"errors": [error_payload]},
        )


def test_raise_for_graphql_errors_maps_unhandled_error() -> None:
    """
    It maps unknown GraphQL failures to UnhandledXledgerException.
    """
    with pytest.raises(UnhandledXledgerException):
        raise_for_graphql_errors(
            body={"errors": [{"message": "Unexpected backend failure"}]},
        )


def test_map_graphql_errors_to_exception_empty_errors_fallback() -> None:
    """
    It returns the default unhandled exception for empty error lists.
    """
    exc = map_graphql_errors_to_exception(errors=[])

    assert isinstance(exc, UnhandledXledgerException)
    assert exc.message == "Unhandled Xledger exception occurred."


def test_map_graphql_errors_to_exception_uses_top_level_status_code() -> None:
    """
    It propagates top-level status code from unmatched error payload.
    """
    exc = map_graphql_errors_to_exception(
        errors=[
            {
                "message": "Unexpected backend failure",
                "status_code": 418,
            }
        ]
    )

    assert isinstance(exc, UnhandledXledgerException)
    assert exc.status_code == 418


def test_map_graphql_errors_to_exception_uses_extension_status_code() -> None:
    """
    It propagates extension status code when top-level status is absent.
    """
    exc = map_graphql_errors_to_exception(
        errors=[
            {
                "message": "Unexpected backend failure",
                "extensions": {"httpStatus": 429},
            }
        ]
    )

    assert isinstance(exc, UnhandledXledgerException)
    assert exc.status_code == 429


def test_map_graphql_errors_to_exception_handles_non_dict_error() -> None:
    """
    It safely handles non-dict error entries and falls back to unhandled.
    """
    exc = map_graphql_errors_to_exception(errors=["boom"])

    assert isinstance(exc, UnhandledXledgerException)
    assert exc.message == "Unknown Error"


def test_map_graphql_errors_to_exception_checks_all_errors_for_match() -> None:
    """
    It keeps scanning errors until a known mapping rule matches.
    """
    exc = map_graphql_errors_to_exception(
        errors=[
            {"message": "Unexpected backend failure"},
            {"message": "Cannot query field `foo` on type `Query`"},
        ]
    )

    assert isinstance(exc, InvalidQueryException)
    assert exc.message == "Invalid Xledger query."
