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

try:
    from .edgar_realtime_adapter import EDGARRealtimeAdapter
except ImportError:
    EDGARRealtimeAdapter = None  # type: ignore

try:
    from .options_adapter import OptionsAdapter
except ImportError:
    OptionsAdapter = None  # type: ignore

try:
    from .polygon_options_adapter import PolygonOptionsAdapter
except ImportError:
    PolygonOptionsAdapter = None  # type: ignore

try:
    from .yield_curve_adapter import YieldCurveAdapter
except ImportError:
    YieldCurveAdapter = None  # type: ignore

try:
    from .finra_short_interest_adapter import FINRAShortInterestAdapter
except ImportError:
    FINRAShortInterestAdapter = None  # type: ignore

try:
    from .boe_adapter import BoEAdapter
except ImportError:
    BoEAdapter = None  # type: ignore

try:
    from .eia_adapter import EIAAdapter
except ImportError:
    EIAAdapter = None  # type: ignore

try:
    from .gdelt_adapter import GDELTAdapter
except ImportError:
    GDELTAdapter = None  # type: ignore

try:
    from .usgs_adapter import USGSAdapter
except ImportError:
    USGSAdapter = None  # type: ignore

try:
    from .ucdp_adapter import UCDPAdapter
except ImportError:
    UCDPAdapter = None  # type: ignore

try:
    from .acled_adapter import ACLEDAdapter
except ImportError:
    ACLEDAdapter = None  # type: ignore

try:
    from .insider_adapter import InsiderAdapter
except ImportError:
    InsiderAdapter = None  # type: ignore

try:
    from .lse_flow_adapter import LSEFlowAdapter
except ImportError:
    LSEFlowAdapter = None  # type: ignore

try:
    from .earnings_calendar_adapter import EarningsCalendarAdapter
except ImportError:
    EarningsCalendarAdapter = None  # type: ignore

try:
    from .economic_calendar_adapter import EconomicCalendarAdapter
except ImportError:
    EconomicCalendarAdapter = None  # type: ignore

try:
    from .sector_rotation_adapter import SectorRotationAdapter
except ImportError:
    SectorRotationAdapter = None  # type: ignore

try:
    from .seed_sync import SeedSyncClient
except ImportError:
    SeedSyncClient = None  # type: ignore


__all__ = [
    'BaseIngestAdapter', 'RawAtom', 'IngestScheduler',
    'YFinanceAdapter', 'FREDAdapter', 'EDGARAdapter', 'RSSAdapter',
    'SignalEnrichmentAdapter', 'HistoricalBackfillAdapter',
    'LLMExtractionAdapter', 'EDGARRealtimeAdapter', 'OptionsAdapter',
    'PolygonOptionsAdapter', 'YieldCurveAdapter', 'FINRAShortInterestAdapter',
    'BoEAdapter', 'EIAAdapter', 'GDELTAdapter', 'USGSAdapter', 'UCDPAdapter',
    'ACLEDAdapter', 'InsiderAdapter', 'LSEFlowAdapter',
    'EarningsCalendarAdapter', 'EconomicCalendarAdapter', 'SectorRotationAdapter',
    'SeedSyncClient',
]
