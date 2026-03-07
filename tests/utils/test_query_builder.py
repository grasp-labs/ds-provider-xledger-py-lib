"""
**File:** ``test_query_builder.py``
**Region:** ``tests/utils``

Description
-----------
Unit tests for GraphQL query/mutation template rendering helper functions.
"""

from __future__ import annotations

import pandas as pd

from ds_provider_xledger_py_lib.enums import ObjectStatus, OperationType, OwnerSet
from ds_provider_xledger_py_lib.utils.introspection import MetaData
from ds_provider_xledger_py_lib.utils.query_builder import (
    _apply_query_arguments,
    _assign_nested_key,
    _extract_query_args,
    _to_graphql_literal,
    _upsert_query_arg,
    build_mutation,
    build_query,
    build_variables,
)


def test_build_variables_for_write_filters_unknown_and_none(create_metadata: MetaData) -> None:
    """
    It builds PlaceHolderInput with nested keys and skips invalid values.
    """
    frame = pd.DataFrame(
        [
            {
                "dbId": "100",
                "name": "Acme",
                "company_code": "C01",
                "ignored": "x",
                "drop_me": None,
            }
        ]
    )

    variables = build_variables(
        obj=frame,
        operation=OperationType.CREATE,
        metadata=create_metadata,
    )

    assert "PlaceHolderInput" in variables
    assert variables["PlaceHolderInput"] == [
        {"clientId": "0", "node": {"dbId": "100", "name": "Acme", "company": {"code": "C01"}}}
    ]


def test_build_variables_for_delete_uses_dbid_values(delete_metadata: MetaData) -> None:
    """
    It creates DBIDS list from non-null dbId values.
    """
    frame = pd.DataFrame([{"dbId": "1"}, {"dbId": None}, {"dbId": "3"}])

    variables = build_variables(
        obj=frame,
        operation=OperationType.DELETE,
        metadata=delete_metadata,
    )

    assert variables == {"DBIDS": ["1", "3"]}


def test_build_variables_for_delete_without_dbid_returns_empty(delete_metadata: MetaData) -> None:
    """
    It returns empty variables when dbId column does not exist.
    """
    frame = pd.DataFrame([{"name": "A"}])
    variables = build_variables(
        obj=frame,
        operation=OperationType.DELETE,
        metadata=delete_metadata,
    )
    assert variables == {}


def test_build_variables_for_read_returns_empty_variables(read_metadata: MetaData) -> None:
    """
    It returns empty variables for read operations.
    """
    frame = pd.DataFrame([{"dbId": "1"}])
    variables = build_variables(
        obj=frame,
        operation=OperationType.READ,
        metadata=read_metadata,
    )
    assert variables == {}


def test_build_query_renders_requested_fields_and_arguments(read_metadata: MetaData) -> None:
    """
    It applies field selection and read arguments to query template.
    """
    query = build_query(
        metadata=read_metadata,
        fields=["name", "company_code"],
        first=10,
        after="cursor-1",
        filter={"name": "Ada"},
        owner_set="main",
        object_status="active",
    )

    assert "node { company { code } name }" in query
    assert "first: 10" in query
    assert 'after: "cursor-1"' in query
    assert 'filter: { name: "Ada" }' in query
    assert 'ownerSet: "main"' in query
    assert 'objectStatus: "active"' in query


def test_build_query_renders_enum_arguments_without_quotes(read_metadata: MetaData) -> None:
    """
    It renders Enum/StrEnum values as GraphQL enum literals.
    """
    query = build_query(
        metadata=read_metadata,
        first=10,
        owner_set=OwnerSet.CURRENT,
        object_status=ObjectStatus.OPEN,
    )

    assert "first: 10" in query
    assert "ownerSet: CURRENT" in query
    assert "objectStatus: OPEN" in query


def test_build_mutation_renders_fields_and_dbids_token(delete_metadata: MetaData) -> None:
    """
    It expands fields and DBIDS placeholders in mutation templates.
    """
    mutation = build_mutation(
        metadata=delete_metadata,
        variables={"DBIDS": ["10", "20"]},
        return_fields=["dbId"],
    )

    assert 'dbids: ["10", "20"]' in mutation
    assert "{{ DBIDS }}" not in mutation


def test_build_mutation_falls_back_to_all_fields_without_defaults() -> None:
    """
    It uses all metadata fields when no defaults or explicit return fields exist.
    """
    metadata = MetaData.deserialize(
        {
            "name": "addItems",
            "type": "create",
            "description": "metadata with no default fields",
            "fields": [
                {"name": "dbId", "type": "string", "description": "", "default": False},
                {"name": "name", "type": "string", "description": "", "default": False},
            ],
            "query": "mutation { addItems(input: $PlaceHolderInput) { items { {{ FIELDS }} } } }",
        }
    )
    mutation = build_mutation(metadata=metadata, variables={}, return_fields=None)

    assert "items { dbId name }" in mutation


def test_to_graphql_literal_covers_supported_types() -> None:
    """
    It converts core Python values into GraphQL literal strings.
    """
    assert _to_graphql_literal("x") == '"x"'
    assert _to_graphql_literal(True) == "true"
    assert _to_graphql_literal(False) == "false"
    assert _to_graphql_literal(10) == "10"
    assert _to_graphql_literal(1.5) == "1.5"
    assert _to_graphql_literal([1, "a"]) == '[1, "a"]'
    assert _to_graphql_literal({"a": 1, "b": "v"}) == '{ a: 1, b: "v" }'
    assert _to_graphql_literal(object()).startswith('"')


def test_to_graphql_literal_renders_enum_as_unquoted_token() -> None:
    """
    It converts enum values into unquoted GraphQL enum tokens.
    """
    assert _to_graphql_literal(ObjectStatus.OPEN) == "OPEN"


def test_upsert_query_arg_replaces_existing_argument() -> None:
    """
    It replaces an existing argument literal by name.
    """
    updated = _upsert_query_arg(
        query_args='first: 100, after: "a1"',
        arg_name="after",
        value_literal='"b2"',
    )

    assert updated == 'first: 100, after: "b2"'


def test_upsert_query_arg_appends_missing_argument() -> None:
    """
    It appends an argument when it is missing from the query arg block.
    """
    updated = _upsert_query_arg(
        query_args="first: 100",
        arg_name="after",
        value_literal='"cursor-1"',
    )

    assert updated == 'first: 100, after: "cursor-1"'


def test_upsert_query_arg_returns_single_pair_for_empty_args() -> None:
    """
    It returns the inserted key/value pair when args block is empty.
    """
    updated = _upsert_query_arg(
        query_args="",
        arg_name="first",
        value_literal="1000",
    )

    assert updated == "first: 1000"


def test_extract_query_args_returns_empty_without_argument_block() -> None:
    """
    It returns empty string when query has no argument section.
    """
    assert _extract_query_args("query { customers { edges { node { dbId } } } }") == ""


def test_apply_query_arguments_returns_original_query_when_no_arg_block() -> None:
    """
    It leaves the query unchanged when there is no argument block.
    """
    query = "query { customers { edges { node { dbId } } } }"
    updated = _apply_query_arguments(query=query, first=10)

    assert updated == query


def test_assign_nested_key_handles_flat_and_nested_keys() -> None:
    """
    It assigns values to root keys and one-level nested key paths.
    """
    target: dict[str, object] = {}
    _assign_nested_key(target=target, key="name", value="Ada")
    _assign_nested_key(target=target, key="company_code", value="C01")
    _assign_nested_key(target=target, key="company_vat", value="VAT-7")

    assert target == {"name": "Ada", "company": {"code": "C01", "vat": "VAT-7"}}
