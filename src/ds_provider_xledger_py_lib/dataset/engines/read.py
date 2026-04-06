"""
**File:** ``read.py``
**Region:** ``ds_provider_xledger_py_lib/dataset/engines``

Description
-----------
Read engine for paginated GraphQL execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, cast

from ds_common_logger_py_lib import Logger

from ...utils.graphql import raise_for_graphql_errors
from ...utils.query_builder import build_query, resolve_query_fields
from ._read_checkpoint import Checkpoint
from ._read_incremental import (
    compose_incremental_filter,
    greatest_incremental_value,
)

if TYPE_CHECKING:
    import pandas as pd
    from ds_protocol_http_py_lib.utils.http.provider import Http

    from ...serde.deserializer import XledgerDeserializer
    from ...utils.introspection import IncrementalMetaData, MetaData
    from ..xledger import XledgerReadSettings

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class ReadEngine:
    """Execute Xledger read requests including pagination handling.

    This engine is intentionally stateful:
    - ``output`` accumulates collected rows page-by-page
    - ``checkpoint`` tracks the last valid pagination state
    """

    connection: Http
    host: str
    deserializer: XledgerDeserializer
    metadata: MetaData
    output: list[pd.DataFrame] = field(default_factory=list, init=False)
    checkpoint: Checkpoint = field(default_factory=Checkpoint, init=False)

    def execute(
        self,
        *,
        read_settings: XledgerReadSettings,
        checkpoint: dict[str, Any] | None = None,
    ) -> None:
        """Execute read flow and update ``output``/``checkpoint`` state.

        Args:
            read_settings: Effective read settings for query rendering/pagination.
            checkpoint: Existing checkpoint state to continue from.
        """
        self.output = []
        self.checkpoint = Checkpoint.deserialize(checkpoint or {})

        if read_settings.limit is not None and read_settings.limit <= 0:
            logger.warning("Read limit is non-positive; returning without dispatching requests.")
            return

        seen_cursors: set[str] = set()
        incremental = self.metadata.incremental
        observed_incremental_values: list[Any] = []
        total_rows = 0
        completed_scope = False

        read_settings = self._resolve_effective_read_settings(
            read_settings=read_settings,
            incremental=incremental,
        )
        required_fields = [incremental.field] if incremental else []
        query_fields = resolve_query_fields(
            metadata=self.metadata,
            requested_fields=read_settings.columns,
            required_fields=required_fields,
        )
        logger.debug(
            (
                "Starting read execution "
                "(entrypoint=%s, pattern=%s, request_first=%s, limit=%s, "
                "checkpoint_incremental=%s, checkpoint_pagination=%s, filter=%s, requested_columns=%s, query_fields=%s)."
            ),
            self.metadata.name,
            self.metadata.pattern,
            self._resolve_request_first(read_settings=read_settings, total_rows=0),
            read_settings.limit,
            self.checkpoint.incremental.value,
            self.checkpoint.pagination.value,
            read_settings.filter,
            read_settings.columns,
            query_fields,
        )

        while True:
            request_first = self._resolve_request_first(read_settings=read_settings, total_rows=total_rows)
            payload = self._build_payload(
                read_settings=read_settings,
                query_fields=query_fields,
                request_first=request_first,
            )
            logger.debug(
                "Dispatching read request (after=%s, request_first=%s, filter=%s, query_fields=%s).",
                read_settings.after,
                request_first,
                read_settings.filter,
                query_fields,
            )
            response = self.connection.post(
                url=self.host,
                json=payload,
            )
            body = response.json()
            raise_for_graphql_errors(body=body)

            watermark = self.deserializer.get_incremental_watermark(value=body, metadata=self.metadata)
            if watermark is not None:
                observed_incremental_values.append(watermark)

            frame = self.deserializer(body, metadata=self.metadata, operation_settings=read_settings)
            has_next_page = self.deserializer.get_next(body, metadata=self.metadata)
            end_cursor = self.deserializer.get_end_cursor(body, metadata=self.metadata)

            self.output.append(frame)
            total_rows += len(frame.index)

            self.checkpoint.pagination.value = end_cursor
            logger.debug(
                "Read page processed (rows=%d, total_rows=%d, page_index=%d, has_next_page=%s, end_cursor=%s).",
                len(frame.index),
                total_rows,
                len(self.output),
                has_next_page,
                end_cursor,
            )

            if read_settings.limit is not None and total_rows >= read_settings.limit:
                completed_scope = not has_next_page
                logger.debug(
                    "Stopping read after reaching row limit (limit=%s, total_rows=%d, has_next_page=%s).",
                    read_settings.limit,
                    total_rows,
                    has_next_page,
                )
                break

            if not self._should_continue_pagination(
                has_next_page=has_next_page,
                end_cursor=end_cursor,
            ):
                completed_scope = not has_next_page
                break

            cursor = cast("str", end_cursor)
            if cursor in seen_cursors:
                logger.warning("Detected repeated pagination cursor; stopping to prevent loop.")
                self.checkpoint.pagination.value = None
                break

            seen_cursors.add(cursor)
            read_settings = replace(read_settings, after=cursor, before=None)
            logger.debug(
                "Continuing pagination (next_page_index=%d, next_cursor=%s, seen_cursors=%d).",
                len(self.output),
                cursor,
                len(seen_cursors),
            )

        self._merge_read_checkpoint(
            incremental=incremental,
            observed_incremental_values=observed_incremental_values,
            completed_scope=completed_scope,
        )

    def _resolve_effective_read_settings(
        self,
        *,
        read_settings: XledgerReadSettings,
        incremental: IncrementalMetaData | None,
    ) -> XledgerReadSettings:
        """Resolve effective read settings before the request loop.

        Args:
            read_settings: Caller-provided read settings.
            incremental: Incremental section from read metadata, when configured.

        Returns:
            Read settings updated with resume cursor and checkpoint boundary filter.
        """
        resumed_settings = read_settings
        if self.checkpoint.pagination.value:
            logger.debug("Resuming paginated read from checkpoint cursor.")
            resumed_settings = replace(
                resumed_settings,
                after=self.checkpoint.pagination.value,
                before=None,
            )

        effective_filter = compose_incremental_filter(
            existing_filter=resumed_settings.filter,
            checkpoint=self.checkpoint,
            incremental=incremental,
        )
        return replace(resumed_settings, filter=effective_filter)

    def _build_payload(
        self,
        *,
        read_settings: XledgerReadSettings,
        query_fields: list[str],
        request_first: int,
    ) -> dict[str, Any]:
        """Build the GraphQL payload for one read request.

        Args:
            read_settings: Effective read settings for the current request.
            query_fields: Effective query fields for the read request.
            request_first: Resolved page size for this request.

        Returns:
            GraphQL payload ready to send through the linked service connection.
        """
        return {
            "query": build_query(
                metadata=self.metadata,
                first=request_first,
                last=read_settings.last,
                before=read_settings.before,
                after=read_settings.after,
                filter=read_settings.filter,
                owner_set=read_settings.owner_set,
                object_status=read_settings.object_status,
                fields=query_fields,
            ),
            "variables": {},
        }

    def _should_continue_pagination(
        self,
        *,
        has_next_page: bool,
        end_cursor: str | None,
    ) -> bool:
        """Return whether the engine should request the next page.

        Args:
            has_next_page: Provider-reported pagination state.
            end_cursor: Provider-reported end cursor for the current page.

        Returns:
            ``True`` when pagination should continue, otherwise ``False``.
        """
        if not (self.metadata.pagination and has_next_page and end_cursor):
            if has_next_page and not end_cursor:
                logger.warning("Pagination requested next page but response did not include end_cursor; stopping read.")
            return False
        return True

    def _configured_page_size(self, *, read_settings: XledgerReadSettings) -> int:
        """Resolve the configured page size from read settings or metadata.

        Args:
            read_settings: Effective read settings for the current request.

        Returns:
            The configured page size to request from the source for the next page.

        Raises:
            RuntimeError: If neither settings nor metadata provide a page size.
        """
        if read_settings.first is not None:
            return read_settings.first

        pagination = self.metadata.pagination
        if pagination is not None and pagination.first is not None:
            return pagination.first

        raise RuntimeError("Read pagination requires a non-null page size from settings or metadata.")

    def _resolve_request_first(
        self,
        *,
        read_settings: XledgerReadSettings,
        total_rows: int,
    ) -> int:
        """Resolve the request page size for the next read request.

        Args:
            read_settings: Effective read settings for the current request.
            total_rows: Number of rows already collected in the current read.

        Returns:
            Page size to request from the source for the next page.

        Raises:
            RuntimeError: If neither settings nor metadata provide a page size.
        """
        configured_first = self._configured_page_size(read_settings=read_settings)

        if read_settings.limit is None:
            return configured_first

        remaining_rows = read_settings.limit - total_rows
        return min(configured_first, remaining_rows)

    def _merge_read_checkpoint(
        self,
        *,
        incremental: IncrementalMetaData | None,
        observed_incremental_values: list[Any],
        completed_scope: bool,
    ) -> None:
        """Merge read results into checkpoint state after the read loop.

        Pagination is cleared only when ``completed_scope`` is true (the configured
        scope was fully exhausted). Partial runs (row limit, missing cursor,
        repeated cursor) leave pagination and incremental state as set during the loop.

        Args:
            incremental: Incremental metadata from read metadata, when configured.
            observed_incremental_values: List of observed incremental values from the current read.
            completed_scope: Whether the read completed the configured scope.
        """
        if completed_scope:
            if incremental and observed_incremental_values:
                self.checkpoint.incremental.value = greatest_incremental_value(
                    observed_incremental_values,
                    kind=incremental.kind,
                )
            self.checkpoint.pagination.value = None
        logger.debug(
            "Read completed (completed_scope=%s, next_incremental=%s, next_pagination=%s).",
            completed_scope,
            self.checkpoint.incremental.value,
            self.checkpoint.pagination.value,
        )
