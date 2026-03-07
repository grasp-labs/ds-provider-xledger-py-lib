"""
**File:** ``query_builder.py``
**Region:** ``ds_provider_xledger_py_lib/utils``

Description
-----------
GraphQL query/mutation template rendering helpers.
"""

from __future__ import annotations

import json
import re
from enum import Enum
from typing import TYPE_CHECKING, Any

from ..enums import OperationType
from .dataframe import dataframe_to_records

if TYPE_CHECKING:
    import pandas as pd

    from .introspection import MetaData

_FIELDS_PLACEHOLDER = "{{ FIELDS }}"
_DBIDS_PLACEHOLDER = "{{ DBIDS }}"
_WRITE_OPERATIONS = {OperationType.CREATE, OperationType.UPDATE}
_QUERY_ARG_MAPPINGS = (
    ("first", "first"),
    ("last", "last"),
    ("before", "before"),
    ("after", "after"),
    ("filter", "filter"),
    ("owner_set", "ownerSet"),
    ("object_status", "objectStatus"),
)


def build_variables(*, obj: pd.DataFrame, operation: OperationType, metadata: MetaData) -> dict[str, Any]:
    """Build GraphQL variables payload for an operation.

    Args:
        obj: Input dataframe.
        operation: Dataset operation type.
        metadata: Loaded operation metadata.

    Returns:
        Variables dictionary matching the query template.
    """
    records = dataframe_to_records(obj)
    if operation in _WRITE_OPERATIONS:
        allowed_fields = {field.name for field in metadata.fields}
        return {"PlaceHolderInput": _build_placeholder_input(records=records, allowed_fields=allowed_fields)}
    if operation == OperationType.DELETE:
        return _build_delete_variables(obj=obj, records=records)
    return {}


def build_query(
    *,
    metadata: MetaData,
    **kwargs: Any,
) -> str:
    """Build a rendered GraphQL query for read operations.

    Args:
        metadata: Operation metadata.
        **kwargs: Query keyword arguments. ``fields`` controls selection set.

    Returns:
        GraphQL query text.
    """
    query_kwargs = dict(kwargs)
    requested_fields = query_kwargs.pop("fields", None)
    rendered = _render_template(
        template=metadata.query,
        field_names=_resolve_fields(metadata=metadata, requested_fields=requested_fields),
        variables={},
    )
    return _apply_query_arguments(query=rendered, **query_kwargs)


def build_mutation(
    *,
    metadata: MetaData,
    variables: dict[str, Any],
    return_fields: list[str] | None = None,
) -> str:
    """Build a rendered GraphQL mutation for write operations.

    Args:
        metadata: Operation metadata.
        variables: Resolved variables required by the mutation template.
        return_fields: Optional explicit fields to return.

    Returns:
        GraphQL mutation text.
    """
    return _render_template(
        template=metadata.query,
        field_names=_resolve_fields(metadata=metadata, requested_fields=return_fields),
        variables=variables,
    )


def _render_template(
    *,
    template: str,
    field_names: list[str],
    variables: dict[str, Any],
) -> str:
    """Render supported placeholders in packaged GraphQL templates.

    Args:
        template: Raw GraphQL template from metadata.
        field_names: Field names to include in ``node`` selections.
        variables: Prepared variables payload used for placeholder expansion.

    Returns:
        Query template with known placeholders expanded.
    """
    query = template
    selection = _build_selection_set(field_names)
    query = query.replace(_FIELDS_PLACEHOLDER, selection)
    dbids_literal = json.dumps(variables.get("DBIDS", []))
    query = query.replace(_DBIDS_PLACEHOLDER, dbids_literal)
    return query


def _build_selection_set(fields: list[str]) -> str:
    """Build GraphQL selection set string from flattened field metadata.

    Args:
        fields: Operation fields from metadata.

    Returns:
        GraphQL selection set string.
    """
    tree: dict[str, Any] = {}
    for field_name in fields:
        parts = field_name.split("_")
        cursor = tree
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = cursor.get(parts[-1], {})
    return _format_tree(tree)


def _format_tree(tree: dict[str, Any]) -> str:
    """Format nested selection tree into GraphQL selection syntax.

    Args:
        tree: Nested dictionary representing selection structure.

    Returns:
        Selection set fragment string.
    """
    parts: list[str] = []
    for key in sorted(tree.keys()):
        child = tree[key]
        if isinstance(child, dict) and child:
            parts.append(f"{key} {{ {_format_tree(child)} }}")
        else:
            parts.append(key)
    return " ".join(parts)


def _resolve_fields(
    *,
    metadata: MetaData,
    requested_fields: list[str] | None,
) -> list[str]:
    """Resolve fields to render for query/mutation selection.

    Args:
        metadata: Operation metadata.
        requested_fields: Requested fields to include in the selection set.

    Returns:
        List of fields to include in the selection set.
    """
    if requested_fields is not None:
        return [str(field) for field in requested_fields]

    default_fields = [field.name for field in metadata.fields if field.default]
    if default_fields:
        return default_fields

    return [field.name for field in metadata.fields]


def _apply_query_arguments(
    *,
    query: str,
    **kwargs: Any,
) -> str:
    """Apply provided keyword arguments to the first query argument block.

    Args:
        query: The query string.
        **kwargs: Query keyword arguments.

    Returns:
        The updated query string.
    """
    query_args = _extract_query_args(query)
    for key, arg_name in _QUERY_ARG_MAPPINGS:
        value = kwargs.get(key)
        if value is None:
            continue
        query_args = _upsert_query_arg(
            query_args=query_args,
            arg_name=arg_name,
            value_literal=_to_graphql_literal(value),
        )
    if not re.search(r"\(.*?\)", query, re.S):
        return query
    return re.sub(r"\(.*?\)", f"({query_args})", query, count=1, flags=re.S)


def _extract_query_args(query: str) -> str:
    """Extract query arguments from a query string.

    Args:
        query: The query string.

    Returns:
        The query arguments string.
    """
    match = re.search(r"\((.*?)\)", query, re.S)
    if not match:
        return ""
    return match.group(1).strip()


def _upsert_query_arg(*, query_args: str, arg_name: str, value_literal: str) -> str:
    """Upsert argument value in a query argument block.

    Args:
        query_args: The query arguments string.
        arg_name: GraphQL argument name.
        value_literal: GraphQL literal value.

    Returns:
        Updated query arguments string with arg replaced or appended.
    """
    pattern = rf"{re.escape(arg_name)}\s*:\s*(\{{.*?\}}|\[.*?\]|\".*?\"|[^,\)]+)"
    replacement = f"{arg_name}: {value_literal}"
    if re.search(pattern, query_args, re.S):
        return re.sub(pattern, replacement, query_args, count=1, flags=re.S)
    if not query_args.strip():
        return replacement
    return f"{query_args}, {replacement}"


def _to_graphql_literal(value: Any) -> str:
    """Convert Python values into GraphQL literal fragments.

    Args:
        value: The value to convert.

    Returns:
        The GraphQL literal string.
    """
    if isinstance(value, Enum):
        enum_value = value.value
        enum_token = str(enum_value)
        return enum_token if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", enum_token) else json.dumps(enum_token)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_to_graphql_literal(item) for item in value) + "]"
    if isinstance(value, dict):
        items = [f"{key}: {_to_graphql_literal(item)}" for key, item in value.items()]
        return "{ " + ", ".join(items) + " }"
    return json.dumps(str(value))


def _assign_nested_key(*, target: dict[str, Any], key: str, value: Any) -> None:
    """Assign value to flat or ``a_b`` nested key path.

    Args:
        target: Target dictionary being built.
        key: Field name. Supports flattened relation style (``a_b``).
        value: Field value to assign.
    """
    if "_" not in key:
        target[key] = value
        return

    root_key, nested_key = key.split("_", 1)
    nested_obj = target.get(root_key)
    if not isinstance(nested_obj, dict):
        nested_obj = {}
        target[root_key] = nested_obj
    nested_obj[nested_key] = value


def _build_placeholder_input(*, records: list[dict[str, Any]], allowed_fields: set[str]) -> list[dict[str, Any]]:
    """Build ``PlaceHolderInput`` payload list for create/update operations.

    Args:
        records: List of records to build placeholder input from.
        allowed_fields: Allowed fields to include in the placeholder input.

    Returns:
        List of placeholder input.
    """
    payload: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        node = _build_node(record=record, allowed_fields=allowed_fields)
        payload.append({"clientId": str(index), "node": node})
    return payload


def _build_node(*, record: dict[str, Any], allowed_fields: set[str]) -> dict[str, Any]:
    """Build a filtered GraphQL node from a dataframe row record.

    Args:
        record: Record to build node from.
        allowed_fields: Allowed fields to include in the node.

    Returns:
        Dictionary of node.
    """
    node: dict[str, Any] = {}
    for key, value in record.items():
        if key in allowed_fields and value is not None:
            _assign_nested_key(target=node, key=key, value=value)
    return node


def _build_delete_variables(*, obj: pd.DataFrame, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Build delete operation variables from ``dbId`` values when available.

    Args:
        obj: Input dataframe.
        records: List of records to build delete variables from.

    Returns:
        Dictionary of variables.
    """
    if "dbId" not in obj.columns:
        return {}
    return {"DBIDS": [record["dbId"] for record in records if record.get("dbId") is not None]}
