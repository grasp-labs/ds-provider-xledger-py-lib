"""
**File:** ``dataframe.py``
**Region:** ``ds_provider_xledger_py_lib/utils``

Description
-----------
Small dataframe helpers shared by serializer and deserializer.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert dataframe rows to JSON-safe records with ``None`` for nulls.

    Args:
        df: Input dataframe.

    Returns:
        List of row dictionaries.
    """
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        record: dict[str, Any] = {}
        for key, value in row.items():
            record[str(key)] = None if pd.isna(value) else value
        records.append(record)
    return records


def edges_to_dataframe(
    *,
    edges: list[dict[str, Any]],
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Flatten ``edges[].node`` payload into a dataframe.

    Args:
        edges: GraphQL connection edges.
        columns: Optional explicit column order/filter.

    Returns:
        Flattened dataframe.
    """
    nodes = [edge.get("node") for edge in edges if isinstance(edge, dict)]
    normalized_nodes = [node for node in nodes if isinstance(node, dict)]
    df = pd.json_normalize(normalized_nodes, sep="_")
    if columns:
        return df.reindex(columns=columns)
    return df
