"""
**File:** ``test_read_incremental.py``
**Region:** ``tests/dataset/engines``

Description
-----------
Unit tests for incremental filter composition and checkpoint boundary handling.
"""

from __future__ import annotations

import pytest

from ds_provider_xledger_py_lib.dataset.engines._read_checkpoint import Checkpoint
from ds_provider_xledger_py_lib.dataset.engines._read_incremental import (
    compose_incremental_filter,
    greatest_incremental_value,
    remove_incremental_boundary,
)
from ds_provider_xledger_py_lib.errors import (
    InvalidIncrementalWatermarkException,
    UnsupportedIncrementalKindException,
)
from ds_provider_xledger_py_lib.utils.introspection import (
    IncrementalMetaData,
)


def _sample_incremental() -> IncrementalMetaData:
    return IncrementalMetaData(kind="time_field", field="modifiedAt", filter_field="modifiedAt_gte")


def test_compose_incremental_filter_returns_unchanged_without_incremental_metadata() -> None:
    """
    It leaves the filter unchanged when incremental is not configured.
    """
    checkpoint = Checkpoint.deserialize({"incremental": {"value": "t0"}, "pagination": {"value": None}})
    user_filter = {"isActive": {"eq": True}}

    assert (
        compose_incremental_filter(
            existing_filter=user_filter,
            checkpoint=checkpoint,
            incremental=None,
        )
        == user_filter
    )


def test_compose_incremental_filter_returns_unchanged_without_watermark() -> None:
    """
    It leaves the filter unchanged when no incremental watermark is stored.
    """
    incremental = _sample_incremental()
    checkpoint = Checkpoint.deserialize({"incremental": {"value": None}, "pagination": {"value": None}})

    user_filter = {"isActive": {"eq": True}}

    assert (
        compose_incremental_filter(
            existing_filter=user_filter,
            checkpoint=checkpoint,
            incremental=incremental,
        )
        == user_filter
    )


def test_compose_incremental_filter_replaces_user_boundary_and_wraps_with_and() -> None:
    """
    It strips prior boundary clauses and applies the checkpoint watermark.
    """
    incremental = _sample_incremental()
    checkpoint = Checkpoint.deserialize({"incremental": {"value": "2025-02-01T00:00:00Z"}, "pagination": {"value": None}})
    existing = {
        "modifiedAt_gte": "2024-01-01T00:00:00Z",
        "isActive": {"eq": True},
    }

    result = compose_incremental_filter(
        existing_filter=existing,
        checkpoint=checkpoint,
        incremental=incremental,
    )

    assert result == {
        "AND": [
            {"isActive": {"eq": True}},
            {"modifiedAt_gte": "2025-02-01T00:00:00Z"},
        ],
    }


def test_compose_incremental_filter_returns_only_boundary_when_filter_was_empty() -> None:
    """
    It yields a single-clause filter when the user filter had only the boundary key.
    """
    incremental = _sample_incremental()
    checkpoint = Checkpoint.deserialize({"incremental": {"value": "2025-02-01T00:00:00Z"}, "pagination": {"value": None}})

    assert compose_incremental_filter(
        existing_filter={"modifiedAt_gte": "2024-01-01T00:00:00Z"},
        checkpoint=checkpoint,
        incremental=incremental,
    ) == {"modifiedAt_gte": "2025-02-01T00:00:00Z"}


def test_compose_incremental_filter_flattens_existing_top_level_and() -> None:
    """
    It appends incremental boundary directly to an existing top-level AND list.
    """
    incremental = _sample_incremental()
    checkpoint = Checkpoint.deserialize({"incremental": {"value": "2025-02-01T00:00:00Z"}, "pagination": {"value": None}})
    existing = {
        "AND": [
            {"dbId": 25080390},
            {"code_gte": "200"},
        ],
    }

    result = compose_incremental_filter(
        existing_filter=existing,
        checkpoint=checkpoint,
        incremental=incremental,
    )

    assert result == {
        "AND": [
            {"dbId": 25080390},
            {"code_gte": "200"},
            {"modifiedAt_gte": "2025-02-01T00:00:00Z"},
        ],
    }


def test_remove_incremental_boundary_strips_nested_and_or_clauses() -> None:
    """
    It removes incremental keys inside logical filter groups.
    """
    incremental = _sample_incremental()
    existing = {
        "AND": [
            {"modifiedAt_gte": "2024-01-01T00:00:00Z", "code": {"eq": "A"}},
            {"modifiedAt_gte": "2024-06-01T00:00:00Z"},
        ],
        "OR": [
            {"modifiedAt_gte": "2024-01-01T00:00:00Z"},
            {"status": {"eq": "OPEN"}},
        ],
    }

    cleaned = remove_incremental_boundary(existing_filter=existing, incremental=incremental)

    assert cleaned == {
        "AND": [{"code": {"eq": "A"}}],
        "OR": [{"status": {"eq": "OPEN"}}],
    }


def test_remove_incremental_boundary_drops_empty_logical_groups() -> None:
    """
    It omits logical keys when every nested clause was only the boundary.
    """
    incremental = _sample_incremental()
    existing = {
        "AND": [{"modifiedAt_gte": "2024-01-01T00:00:00Z"}],
    }

    assert remove_incremental_boundary(existing_filter=existing, incremental=incremental) is None


def test_remove_incremental_boundary_returns_none_for_none_filter() -> None:
    """
    It returns None when there is no user filter to clean.
    """
    incremental = _sample_incremental()

    assert remove_incremental_boundary(existing_filter=None, incremental=incremental) is None


def test_remove_incremental_boundary_preserves_non_dict_items_in_logical_lists() -> None:
    """
    It passes through non-mapping entries inside and/or lists (order preserved).
    """
    incremental = _sample_incremental()
    existing = {
        "AND": [
            "not-a-dict",
            {"modifiedAt_gte": "2024-01-01T00:00:00Z", "code": {"eq": "A"}},
        ],
    }

    cleaned = remove_incremental_boundary(existing_filter=existing, incremental=incremental)

    assert cleaned == {"AND": ["not-a-dict", {"code": {"eq": "A"}}]}


def test_greatest_incremental_value_empty_returns_none() -> None:
    """
    It returns None for an empty sequence.
    """
    assert greatest_incremental_value((), kind="time_field") is None


def test_greatest_incremental_value_time_field_picks_latest_utc() -> None:
    """
    It compares ISO-8601 timestamps chronologically, not lexicographically.
    """
    values = [
        "2025-01-15T12:00:00Z",
        "2025-02-01T00:00:00+00:00",
        "2025-01-20T00:00:00Z",
    ]
    assert greatest_incremental_value(values, kind="time_field") == "2025-02-01T00:00:00+00:00"


def test_greatest_incremental_value_time_field_rejects_non_string() -> None:
    """
    It raises when time_field values are not strings.
    """
    with pytest.raises(InvalidIncrementalWatermarkException, match="strings"):
        greatest_incremental_value([123], kind="time_field")


def test_greatest_incremental_value_unknown_kind_raises() -> None:
    """
    It raises for unsupported incremental kinds.
    """
    with pytest.raises(UnsupportedIncrementalKindException, match="Unsupported incremental kind"):
        greatest_incremental_value(["a"], kind="unknown_kind")
