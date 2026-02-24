from .base import BaseIngestAdapter, RawAtom
from .scheduler import IngestScheduler

# Adapters — import errors are non-fatal (missing optional deps)
try:
    from .yfinance_adapter import YFinanceAdapter
except ImportError:
    YFinanceAdapter = None  # type: ignore

try:
    from .fred_adapter import FREDAdapter
except ImportError:
    FREDAdapter = None  # type: ignore

try:
    from .edgar_adapter import EDGARAdapter
except ImportError:
    EDGARAdapter = None  # type: ignore

try:
    from .rss_adapter import RSSAdapter
except ImportError:
    RSSAdapter = None  # type: ignore

try:
    from .signal_enrichment_adapter import SignalEnrichmentAdapter
except ImportError:
    SignalEnrichmentAdapter = None  # type: ignore

try:
    from .historical_adapter import HistoricalBackfillAdapter
except ImportError:
    HistoricalBackfillAdapter = None  # type: ignore

try:
    from .llm_extraction_adapter import LLMExtractionAdapter
except ImportError:
    LLMExtractionAdapter = None  # type: ignore


__all__ = [
    'BaseIngestAdapter', 'RawAtom', 'IngestScheduler',
    'YFinanceAdapter', 'FREDAdapter', 'EDGARAdapter', 'RSSAdapter',
    'SignalEnrichmentAdapter', 'HistoricalBackfillAdapter',
    'LLMExtractionAdapter',
]
