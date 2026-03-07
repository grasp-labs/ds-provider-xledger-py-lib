"""
**File:** ``test_deserializer.py``
**Region:** ``tests/serde``

Description
-----------
Unit tests for GraphQL response deserialization and output column resolution.
"""

from __future__ import annotations

from ds_provider_xledger_py_lib.dataset.xledger import XledgerCreateSettings, XledgerReadSettings
from ds_provider_xledger_py_lib.enums import OperationType
from ds_provider_xledger_py_lib.serde.deserializer import (
    XledgerDeserializer,
    _connection_payload,
    _resolve_output_columns,
    _root_payload,
)
from ds_provider_xledger_py_lib.utils.introspection import MetaData, MetaField


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
