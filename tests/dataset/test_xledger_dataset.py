"""
**File:** ``test_xledger_dataset.py``
**Region:** ``tests/dataset``

Description
-----------
Unit tests for Xledger dataset orchestration across CRUD lifecycle methods.
"""

from __future__ import annotations

from uuid import uuid4

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DeleteError,
    ReadError,
    UpdateError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError

from ds_provider_xledger_py_lib.dataset.engines.read import Checkpoint
from ds_provider_xledger_py_lib.dataset.xledger import (
    XledgerCreateSettings,
    XledgerDataset,
    XledgerDatasetSettings,
    XledgerDeleteSettings,
    XledgerReadSettings,
    XledgerUpdateSettings,
)
from ds_provider_xledger_py_lib.enums import ResourceType
from ds_provider_xledger_py_lib.linked_service.xledger import XledgerLinkedServiceSettings


def _build_dataset(linked_service_factory: object) -> XledgerDataset:
    """Build a dataset instance with deterministic test settings."""
    linked_service = linked_service_factory()
    return XledgerDataset(
        id=uuid4(),
        name="xledger-dataset",
        version="1.0.0",
        linked_service=linked_service,
        settings=XledgerDatasetSettings(
            entrypoint="bankAccounts",
            read=XledgerReadSettings(first=2, pagination=True),
            create=XledgerCreateSettings(return_columns=["dbId", "name"]),
            update=XledgerUpdateSettings(return_columns=["dbId", "name"]),
            delete=XledgerDeleteSettings(return_columns=["dbId"]),
        ),
    )


def test_dataset_exposes_checkpoint_and_resource_type(linked_service_factory: object) -> None:
    """
    It exposes supports_checkpoint and dataset ResourceType.
    """
    dataset = _build_dataset(linked_service_factory)

    assert dataset.supports_checkpoint is True
    assert dataset.type == ResourceType.DATASET


def test_read_wraps_resource_exception_and_persists_reader_state(
    monkeypatch: pytest.MonkeyPatch,
    linked_service_factory: object,
) -> None:
    """
    It wraps ResourceException as ReadError and still updates output/checkpoint.
    """
    dataset = _build_dataset(linked_service_factory)
    expected_frame = pd.DataFrame([{"dbId": "1", "name": "A"}])

    class _FakeReader:
        def __init__(self, **_: object) -> None:
            self.output = [expected_frame]
            self.checkpoint = Checkpoint(after="c-1", has_next_page=False)

        def execute(self, **_: object) -> None:
            raise ReadError(message="upstream failure", details={"origin": "reader"})

    monkeypatch.setattr("ds_provider_xledger_py_lib.dataset.xledger.ReadEngine", _FakeReader)

    with pytest.raises(ReadError, match="upstream failure"):
        dataset.read()

    assert dataset.output.equals(expected_frame)
    assert dataset.checkpoint == {"after": "c-1", "has_next_page": False}


def test_create_update_delete_are_noop_for_empty_input(linked_service_factory: object) -> None:
    """
    They keep output equal to input when input is empty.
    """
    dataset = _build_dataset(linked_service_factory)
    dataset.input = pd.DataFrame(columns=["dbId", "name"])

    dataset.create()
    assert dataset.output.equals(dataset.input)

    dataset.update()
    assert dataset.output.equals(dataset.input)

    dataset.delete()
    assert dataset.output.equals(dataset.input)


def test_create_raises_when_serializer_is_missing(linked_service_factory: object) -> None:
    """
    It raises CreateError when serializer is unavailable.
    """
    dataset = _build_dataset(linked_service_factory)
    dataset.input = pd.DataFrame([{"dbId": "1", "name": "Alice"}])
    dataset.serializer = None

    with pytest.raises(CreateError, match="Serializer or deserializer is not set"):
        dataset.create()


def test_update_raises_when_deserializer_is_missing(linked_service_factory: object) -> None:
    """
    It raises UpdateError when deserializer is unavailable.
    """
    dataset = _build_dataset(linked_service_factory)
    dataset.input = pd.DataFrame([{"dbId": "1", "name": "Alice"}])
    dataset.deserializer = None

    with pytest.raises(UpdateError, match="Serializer or deserializer is not set"):
        dataset.update()


def test_delete_wraps_resource_exception_from_serializer(linked_service_factory: object) -> None:
    """
    It wraps serializer ResourceException as DeleteError with dataset details.
    """
    dataset = _build_dataset(linked_service_factory)
    dataset.input = pd.DataFrame([{"dbId": "1"}])

    def _broken_serializer(*_: object, **__: object) -> dict[str, object]:
        raise ReadError(message="cannot serialize", details={"phase": "serialize"})

    dataset.serializer = _broken_serializer  # type: ignore[assignment]

    with pytest.raises(DeleteError, match="cannot serialize") as exc_info:
        dataset.delete()

    assert exc_info.value.details.get("type") == ResourceType.DATASET.value
    assert exc_info.value.details.get("phase") == "serialize"


def test_create_success_deserializes_post_response(linked_service_factory: object) -> None:
    """
    It serializes input, posts payload, and deserializes output.
    """
    dataset = _build_dataset(linked_service_factory)
    dataset.input = pd.DataFrame([{"dbId": "7", "name": "Ada"}])

    dataset.serializer = lambda *_args, **_kwargs: {"query": "mutation { ok }", "variables": {"x": 1}}  # type: ignore[assignment]
    dataset.deserializer = lambda *_args, **_kwargs: pd.DataFrame([{"dbId": "7", "name": "Ada"}])  # type: ignore[assignment]

    dataset.create()

    assert len(dataset.linked_service.connection.requests) == 1
    assert dataset.output.to_dict(orient="records") == [{"dbId": "7", "name": "Ada"}]


def test_unsupported_operations_raise_not_supported(linked_service_factory: object) -> None:
    """
    It rejects unsupported dataset operations.
    """
    dataset = _build_dataset(linked_service_factory)

    with pytest.raises(NotSupportedError, match="Rename operation is not supported"):
        dataset.rename()
    with pytest.raises(NotSupportedError, match="Upsert operation is not supported"):
        dataset.upsert()
    with pytest.raises(NotSupportedError, match="Purge operation is not supported"):
        dataset.purge()
    with pytest.raises(NotSupportedError, match="List operation is not supported"):
        dataset.list()


def test_close_delegates_to_linked_service(linked_service_factory: object) -> None:
    """
    It closes the linked-service HTTP provider.
    """
    dataset = _build_dataset(linked_service_factory)
    fake_http = dataset.linked_service.connection
    assert fake_http.closed is False

    dataset.close()

    assert fake_http.closed is True
    assert isinstance(dataset.linked_service.settings, XledgerLinkedServiceSettings)
