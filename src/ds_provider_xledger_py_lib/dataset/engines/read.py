"""
**File:** ``read.py``
**Region:** ``ds_provider_xledger_py_lib/dataset/engines``

Description
-----------
Read engine for paginated GraphQL execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_common_serde_py_lib.serializable import Serializable

from ...utils.graphql import raise_for_graphql_errors
from ...utils.query_builder import build_query

if TYPE_CHECKING:
    from ds_protocol_http_py_lib.utils.http.provider import Http

    from ...serde.deserializer import XledgerDeserializer
    from ...utils.introspection import MetaData
    from ..xledger import XledgerReadSettings

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class Checkpoint(Serializable):
    """Read checkpoint state used by the read engine."""

    after: str | None = None
    has_next_page: bool = False


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
    output: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
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
        frames: list[pd.DataFrame] = []
        seen_cursors: set[str] = set()
        self.checkpoint = Checkpoint.deserialize(checkpoint or {})
        logger.debug(
            "Starting read execution (pagination_enabled=%s, checkpoint_after=%s, settings=%s).",
            read_settings.pagination,
            self.checkpoint.after,
            read_settings.serialize(),
        )

        initial_after = self.checkpoint.after
        if initial_after:
            logger.debug("Resuming paginated read from checkpoint cursor.")
            read_settings = replace(read_settings, after=initial_after, before=None)

        while True:
            payload = {
                "query": build_query(
                    metadata=self.metadata,
                    **{
                        "first": read_settings.first,
                        "last": read_settings.last,
                        "before": read_settings.before,
                        "after": read_settings.after,
                        "filter": read_settings.filter,
                        "owner_set": read_settings.owner_set,
                        "object_status": read_settings.object_status,
                        "fields": read_settings.columns,
                    },
                ),
                "variables": {},
            }
            logger.debug("Payload: %s", payload)
            response = self.connection.post(
                url=self.host,
                json=payload,
            )
            body = response.json()
            raise_for_graphql_errors(body=body)

            page_frame = self.deserializer(
                body,
                metadata=self.metadata,
                operation_settings=read_settings,
            )
            frames.append(page_frame)
            has_next_page = self.deserializer.get_next(
                body,
                metadata=self.metadata,
            )
            end_cursor = self.deserializer.get_end_cursor(
                body,
                metadata=self.metadata,
            )
            self.checkpoint.after = end_cursor
            self.checkpoint.has_next_page = has_next_page
            self.output = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            logger.debug(
                "Read page processed (rows=%d, total_rows=%d, has_next_page=%s).",
                len(page_frame.index),
                len(self.output.index),
                has_next_page,
            )

            if not (read_settings.pagination and has_next_page and end_cursor):
                if read_settings.pagination and has_next_page and not end_cursor:
                    logger.warning("Pagination requested next page but response did not include end_cursor; stopping read.")
                logger.debug(
                    "Stopping pagination (pagination_enabled=%s, has_next_page=%s, has_end_cursor=%s).",
                    read_settings.pagination,
                    has_next_page,
                    bool(end_cursor),
                )
                break

            if end_cursor in seen_cursors:
                logger.warning("Detected repeated pagination cursor; stopping to prevent loop.")
                break

            seen_cursors.add(end_cursor)
            read_settings = replace(read_settings, after=end_cursor, before=None)
            logger.debug(
                "Continuing pagination (page_index=%d, next_cursor=%s, seen_cursors=%d).",
                len(frames),
                end_cursor,
                len(seen_cursors),
            )
