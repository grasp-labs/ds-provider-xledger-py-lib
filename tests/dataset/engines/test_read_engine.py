"""
**File:** ``test_read_engine.py``
**Region:** ``tests/dataset/engines``

Description
-----------
Unit tests for read-engine pagination, checkpointing, and stop conditions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pandas as pd

from ds_provider_xledger_py_lib.dataset.engines.read import ReadEngine
from ds_provider_xledger_py_lib.dataset.xledger import XledgerReadSettings
from ds_provider_xledger_py_lib.enums import ObjectStatus
from ds_provider_xledger_py_lib.serde.deserializer import XledgerDeserializer

if TYPE_CHECKING:
    from ds_provider_xledger_py_lib.utils.introspection import MetaData
    from tests.conftest import FakeHttp


def _read_body(*, has_next_page: bool, cursor: str | None, db_id: str) -> dict[str, object]:
    """Build a small GraphQL read response body for pagination tests."""
    edges = []
    if cursor is not None:
        edges.append({"cursor": cursor, "node": {"dbId": db_id, "name": f"Name-{db_id}"}})
    return {
        "data": {
            "items": {
                "edges": edges,
                "pageInfo": {"hasNextPage": has_next_page},
            }
        }
    }


def test_execute_reads_single_page_when_pagination_disabled(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It reads one page and stops when pagination is disabled.
    """
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=True, cursor="c1", db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(first=2, pagination=False), checkpoint=None)

    assert len(fake_http.requests) == 1
    assert engine.output.to_dict(orient="records") == [{"dbId": "1", "name": "Name-1"}]
    assert engine.checkpoint.after == "c1"
    assert engine.checkpoint.has_next_page is True


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
        read_settings=XledgerReadSettings(first=1, pagination=True),
        checkpoint={"after": "resume-cursor"},
    )

    assert len(fake_http.requests) == 2
    assert "first: 1" in fake_http.requests[0]["json"]["query"]
    assert 'after: "resume-cursor"' in fake_http.requests[0]["json"]["query"]
    assert "first: 1" in fake_http.requests[1]["json"]["query"]
    assert 'after: "c2"' in fake_http.requests[1]["json"]["query"]
    assert engine.output["dbId"].tolist() == ["2", "3"]
    assert engine.checkpoint.after == "c3"
    assert engine.checkpoint.has_next_page is False


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

    engine.execute(read_settings=XledgerReadSettings(pagination=True), checkpoint=None)

    assert len(fake_http.requests) == 1
    assert isinstance(engine.output, pd.DataFrame)
    assert engine.output.empty is True
    assert engine.checkpoint.after is None
    assert engine.checkpoint.has_next_page is True


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

    engine.execute(read_settings=XledgerReadSettings(pagination=True), checkpoint=None)

    assert len(fake_http.requests) == 2
    assert engine.output["dbId"].tolist() == ["1", "2"]
    assert engine.checkpoint.after == "same"


def test_execute_with_pagination_uses_single_request_when_no_next_page(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It executes a single request when pagination is enabled but no next page exists.
    """
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=False, cursor="c1", db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(read_settings=XledgerReadSettings(pagination=True), checkpoint=None)

    assert len(fake_http.requests) == 1
    assert engine.output["dbId"].tolist() == ["1"]


def test_execute_renders_enum_query_arguments_without_quotes(
    read_metadata: MetaData,
    fake_http_factory: type[FakeHttp],
) -> None:
    """
    It keeps enum arguments unquoted when rendering the read query.
    """
    fake_http = fake_http_factory(response_bodies=[_read_body(has_next_page=False, cursor="c1", db_id="1")])
    engine = ReadEngine(
        connection=cast("Any", fake_http),
        host="https://demo.xledger.net/graphql",
        deserializer=XledgerDeserializer(),
        metadata=read_metadata,
    )

    engine.execute(
        read_settings=XledgerReadSettings(
            pagination=True,
            object_status=ObjectStatus.OPEN,
        ),
        checkpoint=None,
    )

    rendered_query = fake_http.requests[0]["json"]["query"]
    assert "objectStatus: OPEN" in rendered_query
    assert 'objectStatus: "OPEN"' not in rendered_query
