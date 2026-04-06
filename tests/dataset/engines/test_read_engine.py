"""
**File:** ``test_read_engine.py``
**Region:** ``tests/dataset/engines``

Description
-----------
Unit tests for read-engine pagination, checkpointing, and stop conditions.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
import pytest

from ds_provider_xledger_py_lib.dataset.engines.read import ReadEngine
from ds_provider_xledger_py_lib.dataset.xledger import XledgerReadSettings
from ds_provider_xledger_py_lib.enums import ObjectStatus
from ds_provider_xledger_py_lib.serde.deserializer import XledgerDeserializer
from ds_provider_xledger_py_lib.utils.introspection import IncrementalMetaData

if TYPE_CHECKING:
    from ds_provider_xledger_py_lib.utils.introspection import MetaData
    from tests.conftest import FakeHttp


def _read_body(
    *,
    has_next_page: bool,
    cursor: str | None,
    db_id: str,
    node_fields: dict[str, object] | None = None,
) -> dict[str, object]:
    """Build a small GraphQL read response body for pagination tests."""
    edges = []
    if cursor is not None:
        node = {"dbId": db_id, "name": f"Name-{db_id}"}
        if node_fields:
            node.update(node_fields)
        edges.append({"cursor": cursor, "node": node})
    return {
        "data": {
            "items": {
                "edges": edges,
                "pageInfo": {"hasNextPage": has_next_page},
            }
        }
    }


def _concat_output(output: list[pd.DataFrame]) -> pd.DataFrame:
    """Materialize read-engine page frames into a single dataframe for assertions."""
    return pd.concat(output, ignore_index=True) if output else pd.DataFrame()


def test_execute_reads_single_page_when_metadata_is_not_paginated(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It reads one page and stops when read metadata does not paginate.
    """
    read_metadata.pagination = None
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=True, cursor="c1", db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(first=2), checkpoint=None)

    assert len(fake_http.requests) == 1
    assert _concat_output(engine.output).to_dict(orient="records") == [{"dbId": "1", "name": "Name-1"}]
    assert engine.checkpoint.pagination.value == "c1"


def test_execute_paginates_and_resumes_from_checkpoint(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It resumes with checkpoint cursor and concatenates multiple pages.
    """
    fake_http = fake_http_factory(
        response_bodies=[
            _read_body(has_next_page=True, cursor="c2", db_id="2"),
            _read_body(has_next_page=False, cursor="c3", db_id="3"),
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(
        read_settings=XledgerReadSettings(first=1),
        checkpoint={"incremental": {"value": None}, "pagination": {"value": "resume-cursor"}},
    )

    assert len(fake_http.requests) == 2
    assert "first: 1" in fake_http.requests[0]["json"]["query"]
    assert 'after: "resume-cursor"' in fake_http.requests[0]["json"]["query"]
    assert "first: 1" in fake_http.requests[1]["json"]["query"]
    assert 'after: "c2"' in fake_http.requests[1]["json"]["query"]
    assert _concat_output(engine.output)["dbId"].tolist() == ["2", "3"]
    assert engine.checkpoint.pagination.value is None


def test_execute_stops_when_response_has_next_without_end_cursor(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It stops safely when hasNextPage is true but edges are missing.
    """
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=True, cursor=None, db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(), checkpoint=None)

    assert len(fake_http.requests) == 1
    assert isinstance(engine.output, list)
    assert len(engine.output) == 1
    assert engine.output[0].empty is True
    assert engine.checkpoint.pagination.value is None


def test_execute_stops_on_repeated_cursor(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It stops pagination when the cursor repeats to avoid infinite loops.
    """
    fake_http = fake_http_factory(
        response_bodies=[
            _read_body(has_next_page=True, cursor="same", db_id="1"),
            _read_body(has_next_page=True, cursor="same", db_id="2"),
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(), checkpoint=None)

    assert len(fake_http.requests) == 2
    assert _concat_output(engine.output)["dbId"].tolist() == ["1", "2"]
    assert engine.checkpoint.pagination.value is None


def test_execute_with_pagination_uses_single_request_when_no_next_page(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It executes a single request when pagination is enabled but no next page exists.
    """
    fake_http = fake_http_factory(
        response_bodies=[
            _read_body(
                has_next_page=False,
                cursor="c1",
                db_id="1",
                node_fields={"modifiedAt": "2025-01-01T00:00:00Z"},
            )
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(), checkpoint=None)

    assert len(fake_http.requests) == 1
    assert _concat_output(engine.output)["dbId"].tolist() == ["1"]


def test_execute_renders_enum_query_arguments_without_quotes(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It keeps enum arguments unquoted when rendering the read query.
    """
    fake_http = fake_http_factory(
        response_bodies=[
            _read_body(
                has_next_page=False,
                cursor="c1",
                db_id="1",
                node_fields={"modifiedAt": "2025-01-01T00:00:00Z"},
            )
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(
        read_settings=XledgerReadSettings(
            object_status=ObjectStatus.OPEN,
        ),
        checkpoint=None,
    )

    rendered_query = fake_http.requests[0]["json"]["query"]
    assert "objectStatus: OPEN" in rendered_query
    assert 'objectStatus: "OPEN"' not in rendered_query


def test_execute_uses_checkpoint_precedence_for_incremental_boundary(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It replaces existing incremental boundary filter with checkpoint boundary.
    """
    read_metadata.incremental = IncrementalMetaData(
        kind="time_field",
        field="modifiedAt",
        filter_field="modifiedAt_gte",
    )
    fake_http = fake_http_factory(
        response_bodies=[
            _read_body(
                has_next_page=False,
                cursor="c1",
                db_id="1",
                node_fields={"modifiedAt": "2025-01-01T00:00:00Z"},
            )
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(
        read_settings=XledgerReadSettings(
            filter={
                "modifiedAt_gte": "2024-01-01T00:00:00Z",
                "isActive": {"eq": True},
            },
        ),
        checkpoint={"incremental": {"value": "2025-01-01T00:00:00Z"}, "pagination": {"value": None}},
    )

    rendered_query = fake_http.requests[0]["json"]["query"]
    assert 'modifiedAt_gte: "2025-01-01T00:00:00Z"' in rendered_query
    assert 'modifiedAt_gte: "2024-01-01T00:00:00Z"' not in rendered_query


def test_execute_queries_incremental_field_when_using_metadata_defaults(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It adds the incremental field to the query while keeping metadata defaults as output columns.
    """
    read_metadata.incremental = IncrementalMetaData(
        kind="time_field",
        field="modifiedAt",
        filter_field="modifiedAt_gte",
    )
    fake_http = fake_http_factory(
        response_bodies=[
            {
                "data": {
                    "items": {
                        "edges": [
                            {
                                "cursor": "c1",
                                "node": {
                                    "dbId": "1",
                                    "name": "Name-1",
                                    "modifiedAt": "2025-01-01T00:00:00Z",
                                },
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(
        read_settings=XledgerReadSettings(),
        checkpoint=None,
    )

    rendered_query = fake_http.requests[0]["json"]["query"]
    output = _concat_output(engine.output)

    assert "dbId" in rendered_query
    assert "name" in rendered_query
    assert "modifiedAt" in rendered_query
    assert output.columns.tolist() == ["dbId", "name"]
    assert output.to_dict(orient="records") == [{"dbId": "1", "name": "Name-1"}]
    assert engine.checkpoint.incremental.value == "2025-01-01T00:00:00Z"


def test_execute_persists_max_incremental_watermark_across_pages(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It stores the chronologically greatest watermark across all pages when the scope completes.
    """
    read_metadata.incremental = IncrementalMetaData(
        kind="time_field",
        field="modifiedAt",
        filter_field="modifiedAt_gte",
    )
    fake_http = fake_http_factory(
        response_bodies=[
            _read_body(
                has_next_page=True,
                cursor="c1",
                db_id="1",
                node_fields={"modifiedAt": "2025-01-01T00:00:00Z"},
            ),
            _read_body(
                has_next_page=False,
                cursor="c2",
                db_id="2",
                node_fields={"modifiedAt": "2025-06-01T00:00:00Z"},
            ),
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(first=10), checkpoint=None)

    assert len(fake_http.requests) == 2
    assert engine.checkpoint.incremental.value == "2025-06-01T00:00:00Z"
    assert engine.checkpoint.pagination.value is None


def test_execute_stops_at_limit_and_preserves_pagination_checkpoint(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It stops at the configured row limit and keeps pagination state for continuation.
    """
    fake_http = fake_http_factory(
        response_bodies=[
            _read_body(has_next_page=True, cursor="c1", db_id="1"),
            _read_body(has_next_page=True, cursor="c2", db_id="2"),
        ]
    )
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(
        read_settings=XledgerReadSettings(first=5, limit=1),
        checkpoint=None,
    )

    assert len(fake_http.requests) == 1
    assert "first: 1" in fake_http.requests[0]["json"]["query"]
    assert engine.checkpoint.incremental.value is None
    assert engine.checkpoint.pagination.value == "c1"


def test_execute_returns_without_dispatch_when_limit_non_positive(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It skips HTTP when the configured row limit is zero or negative.
    """
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=False, cursor="c1", db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(limit=0), checkpoint=None)

    assert len(fake_http.requests) == 0
    assert engine.output == []


def test_execute_raises_when_page_size_cannot_be_resolved(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It raises when neither read settings nor metadata supply pagination.first.
    """
    read_metadata.pagination = None
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=False, cursor="c1", db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    with pytest.raises(RuntimeError, match="non-null page size"):
        engine.execute(
            read_settings=replace(XledgerReadSettings(), first=None),
            checkpoint=None,
        )


def test_execute_uses_metadata_pagination_first_when_settings_first_is_unset(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It uses metadata pagination.first as the request-size default.
    """
    assert read_metadata.pagination is not None
    read_metadata.pagination.first = 77
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=False, cursor="c1", db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(
        read_settings=XledgerReadSettings(first=None),
        checkpoint=None,
    )

    assert "first: 77" in fake_http.requests[0]["json"]["query"]
