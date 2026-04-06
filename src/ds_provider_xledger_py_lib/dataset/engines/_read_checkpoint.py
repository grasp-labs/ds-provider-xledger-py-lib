"""
Checkpoint models for Xledger read continuation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ds_common_serde_py_lib.serializable import Serializable


@dataclass(kw_only=True)
class CheckpointIncremental(Serializable):
    """Store the persisted incremental watermark.

    Attributes:
        value: Persisted watermark value for the current incremental scope.
    """

    value: Any = None


@dataclass(kw_only=True)
class CheckpointPagination(Serializable):
    """Store the persisted pagination continuation token.

    Attributes:
        value: Cursor used to resume an unfinished paginated traversal.
    """

    value: str | None = None


@dataclass(kw_only=True)
class Checkpoint(Serializable):
    """Store checkpoint state for resumable reads.

    Attributes:
        incremental: Persisted incremental watermark state.
        pagination: Persisted pagination continuation state.
    """

    incremental: CheckpointIncremental = field(default_factory=CheckpointIncremental)
    pagination: CheckpointPagination = field(default_factory=CheckpointPagination)
