"""
analytics/kb_commitment.py — Sparse Merkle commitment over KB facts.

Computes a deterministic Merkle root over all KB facts for a given ticker
at a point in time. The root can be stored alongside any decision that
consumed those facts, creating a cryptographic commitment to KB state.

No external dependencies — pure hashlib.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from typing import List, Optional, Tuple

_FACTS_QUERY = """
    SELECT id, predicate, object, confidence
    FROM facts
    WHERE UPPER(subject) = ?
    ORDER BY id ASC
"""


def _leaf_hash(fact_id: int, predicate: str, obj: str, confidence: float) -> bytes:
    """SHA-256 of a single fact row."""
    content = f"{fact_id}:{predicate}:{obj}:{confidence:.6f}"
    return hashlib.sha256(content.encode()).digest()


def _merkle_root(leaves: List[bytes]) -> bytes:
    """Build a Merkle tree from leaf hashes and return the root."""
    if not leaves:
        return hashlib.sha256(b"empty").digest()
    if len(leaves) == 1:
        return leaves[0]

    # Pad to power of 2
    n = 1 << math.ceil(math.log2(len(leaves)))
    padded = leaves + [leaves[-1]] * (n - len(leaves))

    layer = padded
    while len(layer) > 1:
        layer = [
            hashlib.sha256(layer[i] + layer[i + 1]).digest()
            for i in range(0, len(layer), 2)
        ]
    return layer[0]


def compute_kb_root(
    ticker: str,
    db_path: str,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[str, str]:
    """
    Compute a Merkle root over all KB facts for *ticker*.

    Returns
    -------
    (root_hex, fact_ids_json)
        root_hex      — 64-char hex SHA-256 Merkle root
        fact_ids_json — JSON array of fact IDs included in snapshot
    """
    _own = conn is None
    _conn = sqlite3.connect(db_path, timeout=10) if _own else conn

    try:
        rows = _conn.execute(_FACTS_QUERY, (ticker.upper(),)).fetchall()
    finally:
        if _own:
            _conn.close()

    if not rows:
        empty_root = hashlib.sha256(f"no_facts:{ticker}".encode()).hexdigest()
        return empty_root, "[]"

    leaves = [_leaf_hash(r[0], r[1], r[2], r[3] or 0.0) for r in rows]
    root = _merkle_root(leaves)

    fact_ids = [r[0] for r in rows]
    return root.hex(), json.dumps(fact_ids)
