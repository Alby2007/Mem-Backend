"""
services/bot_runner.py — Bot Runner

Orchestrates position entry decisions. When a bot enters a position it:
  1. Computes a KB state commitment (Merkle root) for the ticker
  2. Records the prediction alongside the KB provenance in the ledger

This module is the call site that wires kb_commitment into the prediction
ledger. Other services should call `enter_position()` rather than writing
to the ledger directly.

USAGE
=====
    from services.bot_runner import BotRunner

    runner = BotRunner(db_path='trading_knowledge.db')
    result = runner.enter_position(
        ticker='NVDA',
        pattern_type='fvg',
        timeframe='1h',
        entry_price=890.0,
        target_1=920.0,
        target_2=950.0,
        stop_loss=875.0,
        market_regime='risk_on_expansion',
        conviction_tier='high',
    )
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Dict, Optional

_logger = logging.getLogger(__name__)


class BotRunner:
    """Orchestrates position entries with KB provenance tracking."""

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        self.db_path = db_path

    def enter_position(
        self,
        ticker:          str,
        pattern_type:    str,
        timeframe:       Optional[str] = None,
        entry_price:     Optional[float] = None,
        target_1:        Optional[float] = None,
        target_2:        Optional[float] = None,
        stop_loss:       Optional[float] = None,
        p_hit_t1:        Optional[float] = None,
        p_hit_t2:        Optional[float] = None,
        p_stopped_out:   Optional[float] = None,
        market_regime:   Optional[str] = None,
        conviction_tier: Optional[str] = None,
        expires_at:      Optional[str] = None,
        source:          str = 'system',
        conn:            Optional[sqlite3.Connection] = None,
    ) -> Dict:
        """
        Enter a position: compute KB commitment then record the prediction.

        Returns a dict with:
          recorded   — True if the prediction was written successfully
          kb_root    — 64-char hex Merkle root (or None on failure)
          kb_fact_ids — JSON array of fact IDs (or None on failure)
        """
        from analytics.prediction_ledger import PredictionLedger

        # Compute KB state commitment at decision time
        _kb_root: Optional[str] = None
        _kb_fact_ids: Optional[str] = None
        try:
            from analytics.kb_commitment import compute_kb_root
            _kb_root, _kb_fact_ids = compute_kb_root(
                ticker=ticker,
                db_path=self.db_path,
                conn=conn,
            )
        except Exception as _kbc_e:
            _logger.warning('kb_commitment failed for %s: %s', ticker, _kbc_e)

        ledger = PredictionLedger(self.db_path)
        recorded = ledger.record_prediction(
            ticker=ticker,
            pattern_type=pattern_type,
            timeframe=timeframe,
            entry_price=entry_price,
            target_1=target_1,
            target_2=target_2,
            stop_loss=stop_loss,
            p_hit_t1=p_hit_t1,
            p_hit_t2=p_hit_t2,
            p_stopped_out=p_stopped_out,
            market_regime=market_regime,
            conviction_tier=conviction_tier,
            expires_at=expires_at,
            source=source,
            kb_root=_kb_root,
            kb_fact_ids=_kb_fact_ids,
            conn=conn,
        )

        if recorded:
            _logger.info(
                'Position entered: %s %s | kb_root=%s',
                ticker, pattern_type,
                _kb_root[:16] + '...' if _kb_root else 'None',
            )
        else:
            _logger.error('Failed to record prediction for %s %s', ticker, pattern_type)

        return {
            'recorded': recorded,
            'kb_root': _kb_root,
            'kb_fact_ids': _kb_fact_ids,
        }
