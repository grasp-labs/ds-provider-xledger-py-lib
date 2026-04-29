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
    return [{str(k): (None if pd.isna(v) else v) for k, v in record.items()} for record in df.to_dict("records")]


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
    nodes = [node for edge in edges if isinstance(edge, dict) if isinstance(node := edge.get("node"), dict)]
    df = pd.json_normalize(nodes, sep="_")
    if columns:
        return df.reindex(columns=columns)
    return df
