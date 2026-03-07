"""
**File:** ``deserializer.py``
**Region:** ``ds_provider_xledger_py_lib/serde``

Description
-----------
Deserialize GraphQL responses into tabular dataframe outputs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pandas as pd
from ds_resource_plugin_py_lib.common.serde.deserialize.base import DataDeserializer

from ..enums import OperationType
from ..utils.dataframe import edges_to_dataframe

if TYPE_CHECKING:
    from ..utils.introspection import MetaData


class XledgerDeserializer(DataDeserializer):
    """Parse GraphQL responses into dataframes using operation metadata."""

    def __call__(
        self,
        value: Any,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Deserialize a GraphQL response body.

        Args:
            value: GraphQL response body.
            **kwargs: Compatibility kwargs. Requires ``metadata`` and
                ``operation_settings``.

        Returns:
            Parsed dataframe output.
        """
        metadata = cast("MetaData", kwargs["metadata"])
        operation_settings = kwargs["operation_settings"]
        payload = cast("dict[str, Any]", value)

        columns = _resolve_output_columns(metadata=metadata, operation_settings=operation_settings)
        root = _root_payload(payload=payload, metadata=metadata)
        if not isinstance(root, dict):
            return pd.DataFrame(columns=columns)

        edges = root.get("edges")
        if isinstance(edges, list):
            return edges_to_dataframe(edges=edges, columns=columns)
        return pd.json_normalize([root], sep="_").reindex(columns=columns)

    def get_next(  # type: ignore[override]
        self,
        value: Any,
        **kwargs: Any,
    ) -> bool:
        """Return ``hasNextPage`` from a read response payload.

        Args:
            value: GraphQL response body.
            **kwargs: Compatibility kwargs. Requires ``metadata``.

        Returns:
            ``True`` when there is a next page.
        """
        metadata = cast("MetaData", kwargs["metadata"])
        payload = cast("dict[str, Any]", value)

        connection = _connection_payload(payload=payload, metadata=metadata)
        page_info = connection.get("pageInfo")
        if isinstance(page_info, dict):
            return bool(page_info.get("hasNextPage", False))
        return False

    def get_end_cursor(  # type: ignore[override]
        self,
        value: Any,
        **kwargs: Any,
    ) -> str | None:
        """Return last edge cursor from a read response payload.

        Args:
            value: GraphQL response body.
            **kwargs: Compatibility kwargs. Requires ``metadata``.

        Returns:
            Last cursor when available, otherwise ``None``.
        """
        metadata = cast("MetaData", kwargs["metadata"])
        payload = cast("dict[str, Any]", value)

        connection = _connection_payload(payload=payload, metadata=metadata)
        edges = connection.get("edges")
        if isinstance(edges, list) and edges:
            last_edge = edges[-1]
            if isinstance(last_edge, dict):
                cursor = last_edge.get("cursor")
                return str(cursor) if cursor is not None else None
        return None


def _root_payload(*, payload: dict[str, Any], metadata: MetaData) -> Any:
    """Extract operation root payload under ``data[metadata.name]``.

    Args:
        payload: GraphQL response payload.
        metadata: Operation metadata carrying the root field name.

    Returns:
        Root payload object when present; otherwise ``None``.
    """
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    return data.get(metadata.name)


def _connection_payload(*, payload: dict[str, Any], metadata: MetaData) -> dict[str, Any]:
    """Extract standardized connection payload for read operations.

    Args:
        payload: GraphQL response payload.
        metadata: Read operation metadata.

    Returns:
        Connection object when found; otherwise an empty dictionary.
    """
    root = _root_payload(payload=payload, metadata=metadata)
    if isinstance(root, dict):
        return root
    return {}


def _resolve_output_columns(*, metadata: MetaData, operation_settings: Any) -> list[str]:
    """Resolve output columns from operation settings and metadata.

    Args:
        metadata: Loaded operation metadata.
        operation_settings: Operation settings object.

    Returns:
        Ordered output columns to apply in dataframe construction.
    """
    explicit_columns: Any | None = getattr(operation_settings, "return_columns", None) or getattr(
        operation_settings,
        "columns",
        None,
    )
    if explicit_columns is not None:
        return [str(name) for name in explicit_columns]

    if metadata.type == OperationType.READ.value:
        defaults = [field.name for field in metadata.fields if field.default]
        if defaults:
            return defaults

    return [field.name for field in metadata.fields]
