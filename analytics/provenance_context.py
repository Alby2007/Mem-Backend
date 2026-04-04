"""
analytics/provenance_context.py — Thread-local fact provenance tracking.

Allows any code that calls retrieve() to capture the exact set of fact row
IDs consumed during retrieval, without changing the retrieve() return signature.

Usage:
    with ProvenanceContext() as pctx:
        snippet, atoms = retrieve(message, conn)
    consumed = pctx.consumed_ids  # frozenset[int]
"""

from __future__ import annotations

import threading
from typing import Optional

_local = threading.local()


class ProvenanceContext:
    """
    Context manager that records which fact row IDs are accessed
    during a retrieval call.

    Usage:
        with ProvenanceContext() as pctx:
            snippet, atoms = retrieve(message, conn)
        consumed = pctx.consumed_ids  # frozenset[int]
    """

    def __enter__(self) -> "ProvenanceContext":
        _local.consumed_ids = set()
        return self

    def __exit__(self, *_) -> None:
        # Don't clear — caller reads consumed_ids after the with block.
        # A new __enter__ will reset it.
        pass

    @property
    def consumed_ids(self) -> frozenset:
        return frozenset(getattr(_local, 'consumed_ids', None) or set())


def record_fact_access(fact_id: Optional[int]) -> None:
    """
    Called from retrieval._add() for each atom added to results.
    No-op when no ProvenanceContext is active on this thread.
    """
    if fact_id is None:
        return
    ids = getattr(_local, 'consumed_ids', None)
    if ids is not None:
        ids.add(int(fact_id))
