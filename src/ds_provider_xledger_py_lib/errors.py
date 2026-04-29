"""
**File:** ``errors.py``
**Region:** ``ds_provider_xledger_py_lib``

Description
-----------
Xledger-specific exceptions.
"""

from __future__ import annotations

from typing import Any

from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DatasetException,
    ReadError,
    UpdateError,
)


class InvalidQueryException(ReadError):
    """Raised when a GraphQL query is invalid."""

    def __init__(
        self,
        message: str = "Invalid Xledger Query.",
        code: str = "DS_XLEDGER_INVALID_QUERY_ERROR",
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class InvalidMutationException(CreateError, UpdateError):
    """Raised when a GraphQL mutation is invalid."""

    def __init__(
        self,
        message: str = "Invalid Xledger Mutation.",
        code: str = "DS_XLEDGER_INVALID_MUTATION_ERROR",
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class IncrementalFieldMissingException(ReadError):
    """Raised when incremental metadata expects a field that is absent from every node."""

    def __init__(
        self,
        message: str,
        code: str = "DS_XLEDGER_INCREMENTAL_FIELD_MISSING",
        status_code: int = 502,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class InvalidIncrementalWatermarkException(ReadError):
    """Raised when an incremental watermark value is not valid for the configured strategy."""

    def __init__(
        self,
        message: str,
        code: str = "DS_XLEDGER_INCREMENTAL_WATERMARK_INVALID",
        status_code: int = 500,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class UnsupportedIncrementalKindException(ReadError):
    """Raised when incremental metadata uses an unsupported ``kind``."""

    def __init__(
        self,
        message: str,
        code: str = "DS_XLEDGER_INCREMENTAL_KIND_UNSUPPORTED",
        status_code: int = 500,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class OutOfCreditException(DatasetException):
    """Raised when the user has no available Xledger credits."""

    def __init__(
        self,
        message: str = "User is out of credits in Xledger.",
        code: str = "DS_XLEDGER_OUT_OF_CREDIT_ERROR",
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class TimeOutException(DatasetException):
    """Raised when the request times out."""

    def __init__(
        self,
        message: str = "Request timed out.",
        code: str = "DS_XLEDGER_TIMEOUT_ERROR",
        status_code: int = 408,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class ConcurrentRequestException(DatasetException):
    """Raised when there are too many concurrent requests."""

    def __init__(
        self,
        message: str = "Too many concurrent requests.",
        code: str = "DS_XLEDGER_CONCURRENT_REQUEST_ERROR",
        status_code: int = 429,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class BurstRequestException(DatasetException):
    """Raised when request burst/rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Too many requests.",
        code: str = "DS_XLEDGER_BURST_REQUEST_ERROR",
        status_code: int = 429,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)


class UnhandledXledgerException(DatasetException):
    """Raised when an unclassified Xledger exception occurs."""

    def __init__(
        self,
        message: str = "Unhandled Xledger exception occurred.",
        code: str = "DS_XLEDGER_UNHANDLED_ERROR",
        status_code: int = 500,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)
