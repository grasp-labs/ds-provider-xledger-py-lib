"""
**File:** ``graphql.py``
**Region:** ``ds_provider_xledger_py_lib/utils``

Description
-----------
Helpers for normalizing GraphQL responses and errors.
"""

from __future__ import annotations

from typing import Any

from ds_common_logger_py_lib import Logger

from ..errors import UnhandledXledgerException
from .rules import GraphQLErrorRuleBook

logger = Logger.get_logger(__name__, package=True)


def raise_for_graphql_errors(
    *,
    body: Any,
) -> dict[str, Any]:
    """Raise typed DS exceptions for GraphQL payload errors.

    GraphQL servers may return HTTP 200 while reporting failures under an
    ``errors`` field in the response payload.

    Args:
        body: The GraphQL response body to inspect.

    Returns:
        The original GraphQL response body when no error is present.
    """
    if not isinstance(body, dict):
        logger.error(
            "Invalid GraphQL response payload type received: %s",
            type(body).__name__,
        )
        raise UnhandledXledgerException(
            message="GraphQL response is not a JSON object",
        )

    errors = body.get("errors")
    if isinstance(errors, list) and errors:
        logger.warning(
            "GraphQL response contains %d error(s); mapping to typed exception.",
            len(errors),
        )
        raise map_graphql_errors_to_exception(errors=errors)
    return body


def map_graphql_errors_to_exception(
    *,
    errors: list[Any],
) -> Exception:
    """Map GraphQL payload errors to typed Xledger dataset exceptions.

    Args:
        errors: The GraphQL errors to map to exceptions.

    Returns:
        The mapped exception.
    """
    for error in errors:
        error_message, error_code, extension_code = _parse_error(error)
        status_code = _extract_status_code(error)
        resolved_rule = GraphQLErrorRuleBook.resolve(
            code=error_code,
            extension_code=extension_code,
            error_message=error_message,
        )
        if resolved_rule is not None:
            logger.warning(
                "Mapped GraphQL error to %s (matched_by=%s).",
                resolved_rule.exc_cls.__name__,
                resolved_rule.matched_by,
            )
            return _build_exception(
                resolved_rule.exc_cls,
                message=resolved_rule.message,
                status_code=status_code,
            )
        logger.error(
            "Unhandled GraphQL error mapping (code=%s, extension_code=%s).",
            error_code,
            extension_code,
        )
        return _build_exception(
            UnhandledXledgerException,
            message=error_message,
            status_code=status_code,
        )

    logger.error("GraphQL error list was empty; returning fallback exception.")
    return _build_exception(
        UnhandledXledgerException,
        message="Unhandled Xledger error occurred.",
    )


def _build_exception(
    exc_cls: type[Exception],
    *,
    message: str,
    status_code: int | None = None,
) -> Exception:
    """Instantiate an exception using class defaults when status is missing.

    Args:
        exc_cls: The exception class to instantiate.
        message: The message to include in the exception.
        status_code: The HTTP-like status code to include in the exception.
    """
    kwargs: dict[str, Any] = {"message": message}
    if status_code is not None:
        kwargs["status_code"] = status_code
    return exc_cls(**kwargs)


def _parse_error(error: Any) -> tuple[str, str, str]:
    """Extract message/code metadata from a GraphQL error object.

    Args:
        error: The GraphQL error to parse.

    Returns:
        A tuple containing the message, code, and extension code.
    """
    payload = error if isinstance(error, dict) else {}
    message = str(payload.get("message", "Unknown Error"))
    code = str(payload.get("code", "UNHANDLED_ERROR")).upper()
    extension_code = str(payload.get("extensions", {}).get("code", "UNHANDLED_ERROR")).upper()
    return message, code, extension_code


def _extract_status_code(error: Any) -> int | None:
    """Best-effort extraction of HTTP-like status code from GraphQL error.

    Args:
        error: The GraphQL error to extract the status code from.

    Returns:
        The HTTP-like status code from the GraphQL error.
    """
    if not isinstance(error, dict):
        return None

    for key in ("status_code", "status", "http_status", "httpStatus"):
        value = error.get(key)
        if isinstance(value, int):
            return value

    extensions = error.get("extensions")
    if isinstance(extensions, dict):
        for key in ("status_code", "status", "http_status", "httpStatus"):
            value = extensions.get(key)
            if isinstance(value, int):
                return value

    return None
