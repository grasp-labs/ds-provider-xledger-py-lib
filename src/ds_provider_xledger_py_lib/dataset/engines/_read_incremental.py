"""
Incremental read helpers for Xledger read execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ...errors import InvalidIncrementalWatermarkException, UnsupportedIncrementalKindException

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ...utils.introspection import IncrementalMetaData
    from ._read_checkpoint import Checkpoint

_LOGICAL_FILTER_KEYS = ("and", "or")

FilterDict = dict[str, Any]


def greatest_incremental_value(values: Sequence[Any], *, kind: str) -> Any | None:
    """Return the greatest watermark among observed values for the given strategy.

    Args:
        values: Non-empty sequence of observed watermark candidates (nulls should be
            excluded by callers).
        kind: Incremental strategy from metadata (e.g. ``time_field``).

    Returns:
        The winning original value from ``values``, or ``None`` when ``values`` is empty.

    Raises:
        InvalidIncrementalWatermarkException: When ``time_field`` values are not
            strings or parsing fails.
        UnsupportedIncrementalKindException: When ``kind`` is not supported.
    """
    if not values:
        return None
    if kind == "time_field":
        return _greatest_time_field_value(values)
    raise UnsupportedIncrementalKindException(
        message=f"Unsupported incremental kind: {kind!r}",
        details={"kind": kind},
    )


def _parse_iso8601_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp string to an aware UTC datetime for comparison."""
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        msg = f"Unparseable time_field watermark: {value!r}"
        raise InvalidIncrementalWatermarkException(message=msg, details={"value": value}) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _greatest_time_field_value(values: Sequence[Any]) -> Any:
    """Return the original value that sorts last by parsed UTC ``datetime``."""
    if not values:
        return None
    parsed: list[tuple[datetime, Any]] = []
    for raw in values:
        if not isinstance(raw, str):
            msg = f"time_field watermarks must be strings, got {type(raw).__name__}"
            raise InvalidIncrementalWatermarkException(message=msg, details={"value": raw})
        parsed.append((_parse_iso8601_timestamp(raw), raw))
    return max(parsed, key=lambda item: item[0])[1]


def compose_incremental_filter(
    *,
    existing_filter: FilterDict | None,
    checkpoint: Checkpoint,
    incremental: IncrementalMetaData | None,
) -> FilterDict | None:
    """Apply checkpoint precedence for the incremental boundary.

    Args:
        existing_filter: User-provided filter from read settings.
        checkpoint: Read checkpoint containing persisted continuation state.
        incremental: Incremental section from read metadata, when configured.

    Returns:
        The effective filter with checkpoint boundary applied. When no
        incremental section or watermark exists, the original filter is
        returned unchanged.
    """
    if incremental is None:
        return existing_filter

    boundary = checkpoint.incremental.value
    if boundary is None:
        return existing_filter

    cleaned_filter = remove_incremental_boundary(
        existing_filter=existing_filter,
        incremental=incremental,
    )
    incremental_filter = {incremental.filter_field: boundary}
    if cleaned_filter is None:
        return incremental_filter
    return {"and": [cleaned_filter, incremental_filter]}


def remove_incremental_boundary(
    *,
    existing_filter: FilterDict | None,
    incremental: IncrementalMetaData,
) -> FilterDict | None:
    """Remove existing boundary clauses for the incremental field/operator.

    Args:
        existing_filter: User-provided filter from read settings.
        incremental: Incremental section from read metadata.

    Returns:
        A cleaned filter where the incremental boundary for the configured filter
        key has been removed. Empty filters are returned as ``None``.

    Note:
        For ``and`` / ``or`` lists, nested filter objects are cleaned recursively.
        Non-mapping entries are passed through unchanged (order preserved).
    """
    if existing_filter is None:
        return None

    cleaned_filter: FilterDict = {}
    for key, value in existing_filter.items():
        if key == incremental.filter_field:
            continue

        if key in _LOGICAL_FILTER_KEYS and isinstance(value, list):
            cleaned_items: list[Any] = []
            for item in value:
                if isinstance(item, dict):
                    cleaned = remove_incremental_boundary(existing_filter=item, incremental=incremental)
                    if cleaned is not None:
                        cleaned_items.append(cleaned)
                else:
                    cleaned_items.append(item)
            if cleaned_items:
                cleaned_filter[key] = cleaned_items
            continue

        if key == "not" and isinstance(value, dict):
            cleaned_not = remove_incremental_boundary(
                existing_filter=value,
                incremental=incremental,
            )
            if cleaned_not:
                cleaned_filter[key] = cleaned_not
            continue

        cleaned_filter[key] = value

    return cleaned_filter or None
