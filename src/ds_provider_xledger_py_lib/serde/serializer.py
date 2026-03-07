"""
**File:** ``serializer.py``
**Region:** ``ds_provider_xledger_py_lib/serde``

Description
-----------
Serialize dataset input dataframes into GraphQL request payloads.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.serde.serialize.base import DataSerializer

from ..utils.query_builder import build_mutation, build_variables

if TYPE_CHECKING:
    from ..enums import OperationType
    from ..utils.introspection import MetaData

logger = Logger.get_logger(__name__, package=True)


class XledgerSerializer(DataSerializer):
    """Build GraphQL payloads from dataframe input and loaded metadata."""

    def __call__(
        self,
        obj: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Serialize dataframe into GraphQL payload.

        Args:
            obj: Input dataframe.
            **kwargs: Compatibility kwargs. Requires ``operation`` and
                ``metadata`` and ``operation_settings``.

        Returns:
            Payload in the form ``{"query": query, "variables": variables}``.
        """
        operation = cast("OperationType", kwargs["operation"])
        metadata = cast("MetaData", kwargs["metadata"])
        operation_settings = kwargs["operation_settings"]
        return_fields = getattr(operation_settings, "return_columns", None)

        variables = build_variables(
            obj=obj,
            operation=operation,
            metadata=metadata,
        )
        query = build_mutation(
            metadata=metadata,
            variables=variables,
            return_fields=return_fields,
        )
        logger.debug("Query: %s", query)
        logger.debug("Variables: %s", variables)
        return {"query": query, "variables": variables}
