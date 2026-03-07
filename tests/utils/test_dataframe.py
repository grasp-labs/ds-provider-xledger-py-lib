"""
**File:** ``test_dataframe.py``
**Region:** ``tests/utils``

Description
-----------
Unit tests for shared dataframe conversion and flattening helpers.
"""

from __future__ import annotations

import pandas as pd

from ds_provider_xledger_py_lib.utils.dataframe import dataframe_to_records, edges_to_dataframe


def test_dataframe_to_records_converts_nullish_values_to_none() -> None:
    """
    It emits JSON-safe records where null-like values become None.
    """
    frame = pd.DataFrame(
        [
            {"id": 1, "name": "Ada", "score": float("nan")},
            {"id": 2, "name": None, "score": 3.2},
        ]
    )

    records = dataframe_to_records(frame)

    assert records == [
        {"id": 1, "name": "Ada", "score": None},
        {"id": 2, "name": None, "score": 3.2},
    ]


def test_edges_to_dataframe_flattens_nodes_and_reindexes_columns() -> None:
    """
    It flattens node payloads and keeps requested column ordering.
    """
    frame = edges_to_dataframe(
        edges=[
            {"node": {"dbId": "1", "company": {"code": "C1"}}},
            {"node": {"dbId": "2", "company": {"code": "C2"}}},
        ],
        columns=["company_code", "dbId"],
    )

    assert list(frame.columns) == ["company_code", "dbId"]
    assert frame.to_dict(orient="records") == [
        {"company_code": "C1", "dbId": "1"},
        {"company_code": "C2", "dbId": "2"},
    ]


def test_edges_to_dataframe_ignores_invalid_edges() -> None:
    """
    It ignores non-dictionary edge entries and non-dictionary nodes.
    """
    frame = edges_to_dataframe(
        edges=[
            {"node": {"dbId": "1"}},
            {"node": "bad-node"},
            "bad-edge",
        ]
    )

    assert frame.to_dict(orient="records") == [{"dbId": "1"}]
