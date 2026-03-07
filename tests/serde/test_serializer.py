"""
**File:** ``test_serializer.py``
**Region:** ``tests/serde``

Description
-----------
Unit tests for serializer payload construction and argument passthrough.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from ds_provider_xledger_py_lib.dataset.xledger import XledgerCreateSettings, XledgerUpdateSettings
from ds_provider_xledger_py_lib.enums import OperationType
from ds_provider_xledger_py_lib.serde.serializer import XledgerSerializer

if TYPE_CHECKING:
    import pytest

    from ds_provider_xledger_py_lib.utils.introspection import MetaData


def test_serializer_builds_query_and_variables(monkeypatch: pytest.MonkeyPatch, create_metadata: MetaData) -> None:
    """
    It returns payload composed from query-builder helper outputs.
    """
    captured: dict[str, Any] = {}

    def _fake_build_variables(**kwargs: object) -> dict[str, object]:
        captured["variables_kwargs"] = kwargs
        return {"PlaceHolderInput": [{"clientId": "0"}]}

    def _fake_build_mutation(**kwargs: object) -> str:
        captured["mutation_kwargs"] = kwargs
        return "mutation { addItems { dbId } }"

    monkeypatch.setattr("ds_provider_xledger_py_lib.serde.serializer.build_variables", _fake_build_variables)
    monkeypatch.setattr("ds_provider_xledger_py_lib.serde.serializer.build_mutation", _fake_build_mutation)

    serializer = XledgerSerializer()
    payload = serializer(
        pd.DataFrame([{"dbId": "1", "name": "A"}]),
        operation=OperationType.CREATE,
        metadata=create_metadata,
        operation_settings=XledgerCreateSettings(return_columns=["dbId"]),
    )

    assert payload == {
        "query": "mutation { addItems { dbId } }",
        "variables": {"PlaceHolderInput": [{"clientId": "0"}]},
    }
    assert captured["variables_kwargs"] is not None
    assert captured["mutation_kwargs"] is not None
    assert captured["mutation_kwargs"]["return_fields"] == ["dbId"]


def test_serializer_works_with_default_operation_settings(
    monkeypatch: pytest.MonkeyPatch,
    create_metadata: MetaData,
) -> None:
    """
    It passes None return_fields when operation settings have no return columns.
    """

    def _fake_build_variables(**_: object) -> dict[str, object]:
        return {}

    def _fake_build_mutation(**kwargs: object) -> str:
        assert kwargs["return_fields"] is None
        return "mutation { noop }"

    monkeypatch.setattr("ds_provider_xledger_py_lib.serde.serializer.build_variables", _fake_build_variables)
    monkeypatch.setattr("ds_provider_xledger_py_lib.serde.serializer.build_mutation", _fake_build_mutation)

    payload = XledgerSerializer()(
        pd.DataFrame([{"dbId": "1"}]),
        operation=OperationType.UPDATE,
        metadata=create_metadata,
        operation_settings=XledgerUpdateSettings(),
    )

    assert payload == {"query": "mutation { noop }", "variables": {}}
