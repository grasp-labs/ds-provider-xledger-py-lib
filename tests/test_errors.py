"""
**File:** ``test_errors.py``
**Region:** ``tests``

Description
-----------
Unit tests validating default and custom values for provider exceptions.
"""

from __future__ import annotations

import pytest

from ds_provider_xledger_py_lib.errors import (
    BurstRequestException,
    ConcurrentRequestException,
    InvalidMutationException,
    InvalidQueryException,
    OutOfCreditException,
    TimeOutException,
    UnhandledXledgerException,
)


@pytest.mark.parametrize(
    ("exc_cls", "message", "code", "status_code"),
    [
        (InvalidQueryException, "Invalid Xledger Query.", "DS_XLEDGER_INVALID_QUERY_ERROR", 400),
        (InvalidMutationException, "Invalid Xledger Mutation.", "DS_XLEDGER_INVALID_MUTATION_ERROR", 400),
        (OutOfCreditException, "User is out of credits in Xledger.", "DS_XLEDGER_OUT_OF_CREDIT_ERROR", 400),
        (TimeOutException, "Request timed out.", "DS_XLEDGER_TIMEOUT_ERROR", 408),
        (ConcurrentRequestException, "Too many concurrent requests.", "DS_XLEDGER_CONCURRENT_REQUEST_ERROR", 429),
        (BurstRequestException, "Too many requests.", "DS_XLEDGER_BURST_REQUEST_ERROR", 429),
        (UnhandledXledgerException, "Unhandled Xledger exception occurred.", "DS_XLEDGER_UNHANDLED_ERROR", 500),
    ],
)
def test_exception_defaults(
    exc_cls: type[Exception],
    message: str,
    code: str,
    status_code: int,
) -> None:
    """
    It exposes expected default values for each custom exception.
    """
    exc = exc_cls()

    assert exc.message == message
    assert exc.code == code
    assert exc.status_code == status_code


def test_exception_allows_custom_values() -> None:
    """
    It keeps caller-provided message/code/status/details values.
    """
    details = {"trace_id": "abc-123"}
    exc = InvalidQueryException(
        message="Custom",
        code="X_CUSTOM",
        status_code=422,
        details=details,
    )

    assert exc.message == "Custom"
    assert exc.code == "X_CUSTOM"
    assert exc.status_code == 422
    assert exc.details == details
