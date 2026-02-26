"""
analytics/universe_expander.py — Universe Expander

Resolves a user's interest description into a set of tickers, ETFs,
keywords, and causal relationships using Ollama (LLM). Falls back gracefully
when Ollama is unavailable.

Caps: 20 tickers per expansion request.
      basic tier: 20 total tickers in universe
      pro tier:   100 total tickers in universe
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)

# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class UniverseExpansion:
    sector_label: str
    tickers: List[str]
    etfs: List[str]
    keywords: List[str]
    causal_relationships: List[dict]
    llm_available: bool = True
    error: Optional[str] = None


@dataclass
class ValidationResult:
    valid: List[str]
    rejected: List[str]
    rejection_reasons: Dict[str, str] = field(default_factory=dict)


# ── Tier limits ───────────────────────────────────────────────────────────────

_TIER_UNIVERSE_LIMITS = {
    'basic': 20,
    'pro':   100,
}

_MAX_TICKERS_PER_REQUEST = 20


# ── LLM resolution ────────────────────────────────────────────────────────────

_UK_MARKET_CONTEXT = """
UK market context:
- London Stock Exchange tickers use .L suffix (SHEL.L, AZN.L, BP.L)
- Key sectors: energy (oil majors), mining, financials (banks/insurance),
  pharma, consumer staples, telecoms, property REITs
- FTSE 100 is large cap, FTSE 250 is mid cap (more domestically focused)
- UK retailers heavily exposed to consumer confidence and inflation
- Mining stocks (RIO.L, AAL.L) driven by China demand
- Popular retail trading themes: UK banks, housebuilders, miners,
  defence (BA.L, QQ.L), energy transition
- AIM market for small caps — lower liquidity, higher risk
- Options market less developed than US — pattern detection more
  valuable than options flow for most FTSE names outside top 20
- Use .L suffix for all LSE-listed equities; indices use ^ prefix (^FTSE, ^FTMC)
"""

_RESOLVE_PROMPT = """You are a financial market expert. Given a user's investment interest description,
identify relevant tickers, ETFs, keywords, and causal relationships.

User description: {description}
Market type: {market_type}
{market_context}
Respond with valid JSON only, no markdown fences, matching this exact schema:
{{
  "sector_label": "string — one concise sector/theme label",
  "tickers": ["list of up to 15 relevant equity ticker symbols"],
  "etfs": ["list of up to 5 relevant ETF symbols"],
  "keywords": ["list of up to 10 search keywords"],
  "causal_relationships": [
    {{"cause": "ticker or macro factor", "effect": "ticker or outcome", "direction": "positive|negative", "strength": "strong|moderate|weak"}}
  ]
}}"""


def resolve_interest(
    description: str,
    market_type: str,
    user_id: str,
    db_path: str,
) -> UniverseExpansion:
    """
    Resolve a user's interest description into structured universe data via LLM.
    Falls back to empty lists if Ollama is unavailable.
    """
    try:
        from llm.ollama_client import OllamaClient
        client = OllamaClient()
        market_context = _UK_MARKET_CONTEXT if 'uk' in market_type.lower() or 'london' in market_type.lower() else ''
        prompt = _RESOLVE_PROMPT.format(
            description=description,
            market_type=market_type,
            market_context=market_context,
        )
        raw = client.generate(prompt)
        # Strip markdown fences if present
        raw = raw.strip()
        if raw.startswith('```'):
            lines = raw.split('\n')
            raw = '\n'.join(
                l for l in lines
                if not l.startswith('```')
            )
        data = json.loads(raw)
        tickers = [t.upper().strip() for t in data.get('tickers', []) if t]
        tickers = tickers[:_MAX_TICKERS_PER_REQUEST]
        return UniverseExpansion(
            sector_label=data.get('sector_label', 'unknown'),
            tickers=tickers,
            etfs=[e.upper().strip() for e in data.get('etfs', []) if e],
            keywords=data.get('keywords', []),
            causal_relationships=data.get('causal_relationships', []),
            llm_available=True,
        )
    except ImportError:
        _log.warning('universe_expander: OllamaClient not available')
    except json.JSONDecodeError as exc:
        _log.warning('universe_expander: LLM returned invalid JSON: %s', exc)
    except Exception as exc:
        _log.warning('universe_expander: LLM call failed: %s', exc)

    return UniverseExpansion(
        sector_label='unknown',
        tickers=[],
        etfs=[],
        keywords=[],
        causal_relationships=[],
        llm_available=False,
        error='llm_unavailable',
    )


# ── Ticker validation ─────────────────────────────────────────────────────────

def validate_tickers(
    tickers: List[str],
    market_region: str = 'us',
) -> ValidationResult:
    """
    Validate tickers via yfinance fast_info.
    Criteria: has price, avg volume > 100k, no fetch error.
    Indian equities: auto-append .NS; BSE: .BO.
    """
    try:
        import yfinance as yf
    except ImportError:
        _log.warning('universe_expander.validate_tickers: yfinance not available — accepting all')
        return ValidationResult(valid=tickers, rejected=[])

    valid = []
    rejected = []
    reasons: Dict[str, str] = {}

    for raw_ticker in tickers:
        ticker = raw_ticker.upper().strip()
        # Auto-suffix for Indian equities
        if market_region in ('india', 'in', 'nse') and '.' not in ticker:
            ticker = ticker + '.NS'
        elif market_region in ('bse',) and '.' not in ticker:
            ticker = ticker + '.BO'

        try:
            info = yf.Ticker(ticker).fast_info
            price = getattr(info, 'last_price', None) or getattr(info, 'regularMarketPrice', None)
            volume = getattr(info, 'three_month_average_volume', None) or getattr(info, 'regularMarketVolume', None)

            if price is None or price <= 0:
                rejected.append(raw_ticker)
                reasons[raw_ticker] = 'no_price_data'
                continue
            if volume is not None and volume < 100_000:
                rejected.append(raw_ticker)
                reasons[raw_ticker] = f'low_volume_{int(volume)}'
                continue
            valid.append(ticker)
        except Exception as exc:
            rejected.append(raw_ticker)
            reasons[raw_ticker] = f'fetch_error_{str(exc)[:50]}'

    return ValidationResult(valid=valid, rejected=rejected, rejection_reasons=reasons)


# ── Causal edge seeding ───────────────────────────────────────────────────────

def seed_causal_edges(edges: List[dict], db_path: str) -> int:
    """
    Seed causal edges into knowledge/causal_graph.py with source='user_expansion'.
    Returns count of edges seeded.
    """
    seeded = 0
    try:
        from knowledge.causal_graph import add_causal_edge
        for edge in edges:
            cause     = edge.get('cause', '')
            effect    = edge.get('effect', '')
            direction = edge.get('direction', 'positive')
            strength  = edge.get('strength', 'moderate')
            if cause and effect:
                try:
                    add_causal_edge(
                        db_path=db_path,
                        cause=cause,
                        effect=effect,
                        direction=direction,
                        strength=strength,
                        source='user_expansion',
                    )
                    seeded += 1
                except Exception as exc:
                    _log.warning('seed_causal_edges: failed edge %s->%s: %s', cause, effect, exc)
    except ImportError:
        _log.warning('universe_expander.seed_causal_edges: causal_graph not available')
    return seeded


# ── Bootstrap ticker ─────────────────────────────────────────────────────────

def bootstrap_ticker(ticker: str, db_path: str) -> None:
    """
    Run full bootstrap for a newly promoted ticker (non-blocking when called
    via threading.Thread). Sequence: fundamentals → historical → options →
    signal enrichment → pattern detection.
    """
    _log.info('bootstrap_ticker: starting bootstrap for %s', ticker)
    try:
        from knowledge.graph import TradingKnowledgeGraph
        kg = TradingKnowledgeGraph(db_path)

        # 1. Fundamentals + price via yfinance
        try:
            from ingest.yfinance_adapter import YFinanceAdapter
            yf_adapter = YFinanceAdapter(tickers=[ticker])
            atoms = yf_adapter.fetch()
            for atom in atoms:
                kg.store_atom(
                    subject=atom.subject, predicate=atom.predicate,
                    obj=atom.object, source=atom.source,
                    confidence=atom.confidence, upsert=atom.upsert,
                )
            _log.info('bootstrap_ticker: %s — yfinance done (%d atoms)', ticker, len(atoms))
        except Exception as exc:
            _log.warning('bootstrap_ticker: %s — yfinance failed: %s', ticker, exc)

        # 2. Historical
        try:
            from ingest.historical_adapter import HistoricalBackfillAdapter
            hist = HistoricalBackfillAdapter(tickers=[ticker])
            atoms = hist.fetch()
            for atom in atoms:
                kg.store_atom(
                    subject=atom.subject, predicate=atom.predicate,
                    obj=atom.object, source=atom.source,
                    confidence=atom.confidence, upsert=atom.upsert,
                )
            _log.info('bootstrap_ticker: %s — historical done (%d atoms)', ticker, len(atoms))
        except Exception as exc:
            _log.warning('bootstrap_ticker: %s — historical failed: %s', ticker, exc)

        # 3. Options
        try:
            from ingest.options_adapter import OptionsAdapter
            opts = OptionsAdapter(tickers=[ticker])
            atoms = opts.fetch()
            for atom in atoms:
                kg.store_atom(
                    subject=atom.subject, predicate=atom.predicate,
                    obj=atom.object, source=atom.source,
                    confidence=atom.confidence, upsert=atom.upsert,
                )
            _log.info('bootstrap_ticker: %s — options done (%d atoms)', ticker, len(atoms))
        except Exception as exc:
            _log.warning('bootstrap_ticker: %s — options failed: %s', ticker, exc)

        # 4. Signal enrichment
        try:
            from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
            enrich = SignalEnrichmentAdapter()
            atoms = enrich.fetch()
            for atom in atoms:
                kg.store_atom(
                    subject=atom.subject, predicate=atom.predicate,
                    obj=atom.object, source=atom.source,
                    confidence=atom.confidence, upsert=atom.upsert,
                )
            _log.info('bootstrap_ticker: %s — enrichment done (%d atoms)', ticker, len(atoms))
        except Exception as exc:
            _log.warning('bootstrap_ticker: %s — enrichment failed: %s', ticker, exc)

        # 5. Pattern detection
        try:
            from analytics.pattern_detector import PatternDetector
            from users.user_store import upsert_pattern_signal
            detector = PatternDetector(db_path=db_path, tickers=[ticker])
            signals = detector.detect_all()
            for sig in signals:
                upsert_pattern_signal(db_path, sig.__dict__)
            _log.info('bootstrap_ticker: %s — patterns done (%d signals)', ticker, len(signals))
        except Exception as exc:
            _log.warning('bootstrap_ticker: %s — patterns failed: %s', ticker, exc)

        _log.info('bootstrap_ticker: %s — bootstrap complete', ticker)

    except Exception as exc:
        _log.error('bootstrap_ticker: %s — fatal error: %s', ticker, exc)


def bootstrap_ticker_async(ticker: str, db_path: str) -> None:
    """Launch bootstrap_ticker in a daemon thread (non-blocking)."""
    t = threading.Thread(
        target=bootstrap_ticker,
        args=(ticker, db_path),
        name=f'bootstrap-{ticker}',
        daemon=True,
    )
    t.start()


# ── Bootstrap time estimate ───────────────────────────────────────────────────

def get_extraction_queue_depth(db_path: str) -> int:
    """Return count of pending rows in LLM extraction queue. Returns 0 if table absent."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM llm_extraction_queue WHERE status = 'pending'"
            ).fetchone()
            return row[0] if row else 0
        finally:
            conn.close()
    except Exception:
        return 0


def estimate_bootstrap_seconds(n_tickers: int, db_path: str) -> int:
    """Estimate bootstrap time in seconds for n_tickers newly promoted tickers."""
    base_per_ticker = 8
    queue_depth = get_extraction_queue_depth(db_path)
    queue_delay = min(queue_depth * 2, 60)
    return (n_tickers * base_per_ticker) + queue_delay
