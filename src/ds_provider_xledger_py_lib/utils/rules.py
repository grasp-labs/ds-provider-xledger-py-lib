"""
**File:** ``rules.py``
**Region:** ``ds_provider_xledger_py_lib/utils``

Description
-----------
GraphQL error mapping rule definitions for the Xledger provider.
"""

from __future__ import annotations

from typing import NamedTuple

from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError

from ..errors import (
    BurstRequestException,
    ConcurrentRequestException,
    InvalidMutationException,
    InvalidQueryException,
    OutOfCreditException,
    TimeOutException,
)


class Rule(NamedTuple):
    """Simple rule for mapping GraphQL errors to provider exceptions."""

    exc_cls: type[Exception]
    code: str | None = None
    extension_code: str | None = None
    message_keywords: tuple[str, ...] = ()
    default_message: str | None = None


class ResolvedRule(NamedTuple):
    """Resolved rule payload returned by the rulebook."""

    exc_cls: type[Exception]
    message: str


class GraphQLErrorRuleBook:
    """Registry for GraphQL-to-provider exception mapping rules."""

    _rules: tuple[Rule, ...] = (
        Rule(
            exc_cls=OutOfCreditException,
            code="BAD_REQUEST.INSUFFICIENT_CREDITS",
            default_message="Insufficient Xledger credits.",
        ),
        Rule(
            exc_cls=TimeOutException,
            message_keywords=("your query timed out", "timed out", "timeout"),
            default_message="Query timed out.",
        ),
        Rule(
            exc_cls=BurstRequestException,
            code="BAD_REQUEST.BURST_RATE_LIMIT_REACHED",
            default_message="Too many requests error.",
        ),
        Rule(
            exc_cls=ConcurrentRequestException,
            code="BAD_REQUEST.CONCURRENCY_LIMIT_REACHED",
            default_message="Too many concurrent requests error.",
        ),
        Rule(
            exc_cls=AuthenticationError,
            message_keywords=("bad format for api token",),
        ),
        Rule(
            exc_cls=InvalidQueryException,
            message_keywords=("cannot query field",),
            default_message="Invalid Xledger query.",
        ),
        Rule(
            exc_cls=InvalidMutationException,
            extension_code="INVALID_VALUE",
            message_keywords=("argument",),
            default_message="Invalid Xledger argument.",
        ),
    )

    @classmethod
    def resolve(
        cls,
        *,
        code: str,
        extension_code: str,
        error_message: str,
    ) -> ResolvedRule | None:
        """Resolve a GraphQL error to a mapped provider exception.

        Args:
            code: The error code to resolve.
            extension_code: The extension code to resolve.
            error_message: The error message to resolve.

        Returns:
            The resolved rule.
        """
        message_lower = error_message.lower()
        for rule in cls._rules:
            if cls._matches(
                rule=rule,
                message_lower=message_lower,
                code=code,
                extension_code=extension_code,
            ):
                message = rule.default_message or error_message
                return ResolvedRule(exc_cls=rule.exc_cls, message=message)
        return None

    @staticmethod
    def _matches(
        *,
        rule: Rule,
        message_lower: str,
        code: str,
        extension_code: str,
    ) -> bool:
        """Return True when the current error matches the provided rule.

        Args:
            rule: The rule to match.
            message_lower: The lowercased error message to match.
            code: The error code to match.
            extension_code: The extension code to match.

        Returns:
            True when the current error matches the provided rule.
        """
        if rule.code and code == rule.code:
            return True
        if rule.extension_code and extension_code == rule.extension_code:
            return True
        return bool(rule.message_keywords and any(keyword in message_lower for keyword in rule.message_keywords))
