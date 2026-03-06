"""
**File:** ``test_rules.py``
**Region:** ``tests/utils``

Description
-----------
Unit tests for GraphQLErrorRuleBook matching and resolution.
"""

from __future__ import annotations

from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError

from ds_provider_xledger_py_lib.errors import (
    InvalidMutationException,
    InvalidQueryException,
    OutOfCreditException,
)
from ds_provider_xledger_py_lib.utils.rules import GraphQLErrorRuleBook


def test_rulebook_resolves_by_error_code() -> None:
    """
    It resolves a known error code to the expected exception.
    """
    resolved = GraphQLErrorRuleBook.resolve(
        code="BAD_REQUEST.INSUFFICIENT_CREDITS",
        extension_code="UNHANDLED_ERROR",
        error_message="Not enough credits",
    )

    assert resolved is not None
    assert resolved.exc_cls is OutOfCreditException
    assert resolved.message == "Insufficient Xledger credits."


def test_rulebook_resolves_by_extension_code() -> None:
    """
    It resolves extension code INVALID_VALUE to mutation exception.
    """
    resolved = GraphQLErrorRuleBook.resolve(
        code="UNHANDLED_ERROR",
        extension_code="INVALID_VALUE",
        error_message="Invalid mutation payload",
    )

    assert resolved is not None
    assert resolved.exc_cls is InvalidMutationException
    assert resolved.message == "Invalid Xledger argument."


def test_rulebook_resolves_by_message_keyword() -> None:
    """
    It resolves query field errors using message keyword matching.
    """
    resolved = GraphQLErrorRuleBook.resolve(
        code="UNHANDLED_ERROR",
        extension_code="UNHANDLED_ERROR",
        error_message="Cannot query field `foo` on type `Query`",
    )

    assert resolved is not None
    assert resolved.exc_cls is InvalidQueryException
    assert resolved.message == "Invalid Xledger query."


def test_rulebook_resolves_api_token_format_error_to_authentication_error() -> None:
    """
    It resolves bad API token format payload to AuthenticationError.
    """
    resolved = GraphQLErrorRuleBook.resolve(
        code="UNHANDLED_ERROR",
        extension_code="UNHANDLED_ERROR",
        error_message='Bad format for API Token. Expected header: "Authorization: token <your-token>"',
    )

    assert resolved is not None
    assert resolved.exc_cls is AuthenticationError
    assert "Bad format for API Token" in resolved.message


def test_rulebook_returns_none_when_unmatched() -> None:
    """
    It returns None when no rule matches the error payload.
    """
    resolved = GraphQLErrorRuleBook.resolve(
        code="UNHANDLED_ERROR",
        extension_code="UNHANDLED_ERROR",
        error_message="Unknown backend failure",
    )

    assert resolved is None
