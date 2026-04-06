"""
**File:** ``conftest.py``
**Region:** ``tests``

Description
-----------
Shared pytest fixtures and HTTP fakes used across test modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest

from ds_provider_xledger_py_lib.enums import OperationType
from ds_provider_xledger_py_lib.linked_service.xledger import (
    XledgerLinkedService,
    XledgerLinkedServiceSettings,
)
from ds_provider_xledger_py_lib.utils.introspection import MetaData, MetaField, PaginationMetaData


@dataclass
class _FakeResponse:
    body: dict[str, Any]

    def json(self) -> dict[str, Any]:
        return self.body


class FakeHttp:
    """Small in-memory HTTP fake that stores request history."""

    def __init__(self, *, response_bodies: list[dict[str, Any]] | None = None) -> None:
        self.response_bodies = list(response_bodies or [])
        self.requests: list[dict[str, Any]] = []
        self.closed = False
        self.session = type("Session", (), {"headers": {}})()

    def post(self, *, url: str, json: dict[str, Any]) -> _FakeResponse:
        self.requests.append({"url": url, "json": json})
        if self.response_bodies:
            return _FakeResponse(body=self.response_bodies.pop(0))
        return _FakeResponse(body={"data": {}})

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def read_metadata() -> MetaData:
    """Representative read metadata used by query/read-engine tests."""
    return MetaData(
        name="items",
        type=OperationType.READ.value,
        description="Read test metadata",
        fields=[
            MetaField(name="dbId", type="string", description="", default=True),
            MetaField(name="name", type="string", description="", default=True),
            MetaField(name="company_code", type="string", description="", default=False),
        ],
        query=("query { items() { edges { cursor node { {{ FIELDS }} } } pageInfo { hasNextPage } } }"),
        pagination=PaginationMetaData(kind="cursor", first=1000),
    )


@pytest.fixture
def create_metadata() -> MetaData:
    """Representative create metadata with nested field support."""
    return MetaData(
        name="addItems",
        type=OperationType.CREATE.value,
        description="Create test metadata",
        fields=[
            MetaField(name="dbId", type="string", description="", default=True),
            MetaField(name="name", type="string", description="", default=True),
            MetaField(name="company_code", type="string", description="", default=False),
        ],
        query="mutation { addItems(input: $PlaceHolderInput) { items { {{ FIELDS }} } } }",
    )


@pytest.fixture
def update_metadata(create_metadata: MetaData) -> MetaData:
    """Representative update metadata fixture."""
    return MetaData.deserialize(
        {
            **create_metadata.serialize(),
            "name": "updateItems",
            "type": OperationType.UPDATE.value,
            "query": "mutation { updateItems(input: $PlaceHolderInput) { items { {{ FIELDS }} } } }",
        }
    )


@pytest.fixture
def delete_metadata() -> MetaData:
    """Representative delete metadata with DBIDS placeholder."""
    return MetaData(
        name="deleteItems",
        type=OperationType.DELETE.value,
        description="Delete test metadata",
        fields=[
            MetaField(name="dbId", type="string", description="", default=True),
            MetaField(name="name", type="string", description="", default=True),
        ],
        query="mutation { deleteItems(dbids: {{ DBIDS }}) { deletedCount } }",
    )


@pytest.fixture
def fake_http_factory() -> type[FakeHttp]:
    """Expose FakeHttp class for custom response queues per test."""
    return FakeHttp


@pytest.fixture
def linked_service_factory() -> Any:
    """Create linked-service instances pre-wired with FakeHttp."""

    def _factory(fake_http: FakeHttp | None = None) -> XledgerLinkedService[XledgerLinkedServiceSettings]:
        linked_service = XledgerLinkedService(
            id=uuid4(),
            name="xledger",
            version="1.0.0",
            settings=XledgerLinkedServiceSettings(
                host="https://demo.xledger.net/graphql",
                token="token-value",
            ),
        )
        linked_service._http = fake_http or FakeHttp()
        return linked_service

    return _factory
