"""
**File:** ``test_deserializer.py``
**Region:** ``tests/serde``

Description
-----------
Unit tests for GraphQL response deserialization and output column resolution.
"""

from __future__ import annotations

import pytest

from ds_provider_xledger_py_lib.dataset.xledger import XledgerCreateSettings, XledgerReadSettings
from ds_provider_xledger_py_lib.enums import OperationType
from ds_provider_xledger_py_lib.errors import IncrementalFieldMissingException, InvalidIncrementalWatermarkException
from ds_provider_xledger_py_lib.serde.deserializer import (
    XledgerDeserializer,
    _connection_payload,
    _resolve_output_columns,
    _root_payload,
)
from ds_provider_xledger_py_lib.utils.introspection import IncrementalMetaData, MetaData, MetaField


@pytest.fixture
def read_metadata_incremental(read_metadata: MetaData) -> MetaData:
    """Read metadata with a typical ``time_field`` incremental section."""
    read_metadata.incremental = IncrementalMetaData(
        kind="time_field",
        field="modifiedAt",
        filter_field="modifiedAt_gte",
    )
    return read_metadata


def test_deserializer_returns_empty_frame_when_root_missing(read_metadata: MetaData) -> None:
    """
    It returns an empty dataframe with selected columns for missing root payload.
    """
    result = XledgerDeserializer()(
        {"data": {"other": {}}},
        metadata=read_metadata,
        operation_settings=XledgerReadSettings(columns=["dbId", "name"]),
    )

    assert result.empty is True
    assert list(result.columns) == ["dbId", "name"]


def test_deserializer_flattens_edges_into_selected_columns(read_metadata: MetaData) -> None:
    """
    It converts connection edges into a tabular dataframe.
    """
    payload = {
        "data": {
            "items": {
                "edges": [
                    {"cursor": "c1", "node": {"dbId": "1", "name": "A", "company": {"code": "C01"}}},
                    {"cursor": "c2", "node": {"dbId": "2", "name": "B", "company": {"code": "C02"}}},
                ],
                "pageInfo": {"hasNextPage": False},
            }
        }
    }

    result = XledgerDeserializer()(
        payload,
        metadata=read_metadata,
        operation_settings=XledgerReadSettings(columns=["dbId", "company_code"]),
    )

    assert result.to_dict(orient="records") == [
        {"dbId": "1", "company_code": "C01"},
        {"dbId": "2", "company_code": "C02"},
    ]


def test_deserializer_returns_empty_frame_when_connection_edges_are_null(read_metadata: MetaData) -> None:
    """
    It treats connection payloads with null edges as empty read results.
    """
    payload = {
        "data": {
            "items": {
                "edges": None,
                "pageInfo": {"hasNextPage": False},
            }
        }
    }

    result = XledgerDeserializer()(
        payload,
        metadata=read_metadata,
        operation_settings=XledgerReadSettings(),
    )

    assert result.empty is True
    assert list(result.columns) == ["dbId", "name"]


def test_deserializer_normalizes_non_edge_root_payload(create_metadata: MetaData) -> None:
    """
    It normalizes non-read operation payload roots into one-row dataframe.
    """
    payload = {"data": {"addItems": {"dbId": "7", "name": "Ada"}}}
    result = XledgerDeserializer()(
        payload,
        metadata=create_metadata,
        operation_settings=XledgerCreateSettings(),
    )

    assert result["dbId"].tolist() == ["7"]
    assert result["name"].tolist() == ["Ada"]
    assert result["company_code"].isna().all()


def test_get_next_and_end_cursor_cover_missing_and_present_paths(read_metadata: MetaData) -> None:
    """
    It reads hasNextPage/cursor and handles missing page information safely.
    """
    full_payload = {
        "data": {
            "items": {
                "edges": [{"cursor": "c1", "node": {"dbId": "1"}}],
                "pageInfo": {"hasNextPage": True},
            }
        }
    }
    empty_payload = {"data": {"items": {"edges": []}}}

    deserializer = XledgerDeserializer()
    assert deserializer.get_next(full_payload, metadata=read_metadata) is True
    assert deserializer.get_end_cursor(full_payload, metadata=read_metadata) == "c1"
    assert deserializer.get_next(empty_payload, metadata=read_metadata) is False
    assert deserializer.get_end_cursor(empty_payload, metadata=read_metadata) is None


def test_resolve_output_columns_prefers_explicit_settings(read_metadata: MetaData) -> None:
    """
    It prioritizes explicit settings columns over metadata defaults.
    """
    columns = _resolve_output_columns(
        metadata=read_metadata,
        operation_settings=XledgerReadSettings(columns=["name", "dbId"]),
    )

    assert columns == ["name", "dbId"]


def test_resolve_output_columns_uses_default_read_fields() -> None:
    """
    It picks metadata default fields for read operations.
    """
    metadata = MetaData(
        name="items",
        type=OperationType.READ.value,
        description="",
        fields=[
            MetaField(name="dbId", type="string", description="", default=True),
            MetaField(name="name", type="string", description="", default=True),
            MetaField(name="company_code", type="string", description="", default=False),
        ],
        query="query { items { edges { node { dbId } } } }",
    )

    assert _resolve_output_columns(metadata=metadata, operation_settings=XledgerReadSettings()) == ["dbId", "name"]


def test_root_and_connection_payload_helpers_handle_invalid_shapes(read_metadata: MetaData) -> None:
    """
    They return None/empty connection when payload structure is invalid.
    """
    payload = {"data": "not-a-dict"}
    assert _root_payload(payload=payload, metadata=read_metadata) is None
    assert _connection_payload(payload=payload, metadata=read_metadata) == {}


def test_get_incremental_watermark_returns_none_when_all_values_are_null(
    read_metadata_incremental: MetaData,
) -> None:
    """
    It returns None when the incremental field is present in every node but all values are null.
    """
    payload = {
        "data": {
            "items": {
                "edges": [
                    {"cursor": "c1", "node": {"dbId": "1", "modifiedAt": None}},
                    {"cursor": "c2", "node": {"dbId": "2", "modifiedAt": None}},
                ],
                "pageInfo": {"hasNextPage": False},
            }
        }
    }

    result = XledgerDeserializer().get_incremental_watermark(
        payload,
        metadata=read_metadata_incremental,
    )

    assert result is None


def test_get_incremental_watermark_raises_when_field_absent_from_all_nodes(
    read_metadata_incremental: MetaData,
) -> None:
    """
    It raises IncrementalFieldMissingException when the incremental field key is absent from every node.
    """
    payload = {
        "data": {
            "items": {
                "edges": [
                    {"cursor": "c1", "node": {"dbId": "1"}},
                    {"cursor": "c2", "node": {"dbId": "2"}},
                ],
                "pageInfo": {"hasNextPage": False},
            }
        }
    }

    with pytest.raises(IncrementalFieldMissingException, match="modifiedAt"):
        XledgerDeserializer().get_incremental_watermark(
            payload,
            metadata=read_metadata_incremental,
        )


def test_get_incremental_watermark_raises_on_unparseable_time_field(
    read_metadata_incremental: MetaData,
) -> None:
    """
    It raises InvalidIncrementalWatermarkException when time_field values are not valid ISO-8601 strings.
    """
    payload = {
        "data": {
            "items": {
                "edges": [
                    {"cursor": "c1", "node": {"dbId": "1", "modifiedAt": "not-a-timestamp"}},
                ],
                "pageInfo": {"hasNextPage": False},
            }
        }
    }

    with pytest.raises(InvalidIncrementalWatermarkException, match="Unparseable"):
        XledgerDeserializer().get_incremental_watermark(
            payload,
            metadata=read_metadata_incremental,
        )


def test_get_incremental_watermark_returns_latest_time_field_value(
    read_metadata_incremental: MetaData,
) -> None:
    """
    It returns the chronologically latest timestamp for time_field watermarks.
    """
    payload = {
        "data": {
            "items": {
                "edges": [
                    {"cursor": "c1", "node": {"dbId": "1", "modifiedAt": "2025-01-01T00:00:00Z"}},
                    {"cursor": "c2", "node": {"dbId": "2", "modifiedAt": "2025-06-01T00:00:00Z"}},
                ],
                "pageInfo": {"hasNextPage": False},
            }
        }
    }

    result = XledgerDeserializer().get_incremental_watermark(
        payload,
        metadata=read_metadata_incremental,
    )

    assert result == "2025-06-01T00:00:00Z"


def test_get_incremental_watermark_returns_none_without_incremental_metadata(read_metadata: MetaData) -> None:
    """
    It does not derive a watermark when read metadata has no incremental section.
    """
    assert read_metadata.incremental is None
    payload = {
        "data": {
            "items": {
                "edges": [
                    {"cursor": "c1", "node": {"dbId": "1", "modifiedAt": "2025-01-01T00:00:00Z"}},
                ],
                "pageInfo": {"hasNextPage": False},
            }
        }
    }

    result = XledgerDeserializer().get_incremental_watermark(
        payload,
        metadata=read_metadata,
    )

    assert result is None
