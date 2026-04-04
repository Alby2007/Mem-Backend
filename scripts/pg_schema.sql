-- Trading Galaxy — Postgres schema for the 8 hot tables
-- Run: sudo -u postgres psql trading_galaxy < scripts/pg_schema.sql

-- ohlcv_cache
CREATE TABLE IF NOT EXISTS ohlcv_cache (
    ticker     TEXT    NOT NULL,
    interval   TEXT    NOT NULL,
    ts         TEXT    NOT NULL,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    volume     REAL,
    cached_at  TEXT    NOT NULL,
    PRIMARY KEY (ticker, interval, ts)
);

-- pattern_signals
CREATE TABLE IF NOT EXISTS pattern_signals (
    id                   BIGSERIAL PRIMARY KEY,
    ticker               TEXT    NOT NULL,
    pattern_type         TEXT    NOT NULL,
    direction            TEXT    NOT NULL,
    zone_high            REAL    NOT NULL,
    zone_low             REAL    NOT NULL,
    zone_size_pct        REAL    NOT NULL,
    timeframe            TEXT    NOT NULL,
    formed_at            TEXT    NOT NULL,
    status               TEXT    DEFAULT 'open',
    filled_at            TEXT,
    quality_score        REAL,
    kb_conviction        TEXT,
    kb_regime            TEXT,
    kb_signal_dir        TEXT,
    alerted_users        TEXT    DEFAULT '[]',
    detected_at          TEXT    NOT NULL,
    expires_at           TEXT,
    volume_at_formation  REAL,
    volume_vs_avg        REAL
);
CREATE INDEX IF NOT EXISTS idx_ps_status_quality   ON pattern_signals(status, quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_ps_ticker_status    ON pattern_signals(ticker, status);
CREATE INDEX IF NOT EXISTS idx_ps_timeframe_status ON pattern_signals(timeframe, status);
CREATE INDEX IF NOT EXISTS idx_ps_type_dir_status  ON pattern_signals(pattern_type, direction, status);
CREATE INDEX IF NOT EXISTS idx_ps_formed_at        ON pattern_signals(formed_at);
CREATE INDEX IF NOT EXISTS idx_ps_scan             ON pattern_signals(status, quality_score, pattern_type, direction);
-- Partial index for open patterns (the most common query)
CREATE INDEX IF NOT EXISTS idx_ps_open_quality     ON pattern_signals(quality_score DESC) WHERE status = 'open';

-- facts
CREATE TABLE IF NOT EXISTS facts (
    id                  BIGSERIAL PRIMARY KEY,
    subject             TEXT    NOT NULL,
    predicate           TEXT    NOT NULL,
    object              TEXT    NOT NULL,
    confidence          REAL    DEFAULT 0.5,
    source              TEXT,
    timestamp           TEXT,
    metadata            TEXT,
    confidence_effective REAL,
    hit_count           INTEGER DEFAULT 0,
    conf_n              INTEGER DEFAULT 1,
    conf_var            REAL    DEFAULT 0,
    UNIQUE (subject, predicate, object)
);
CREATE INDEX IF NOT EXISTS idx_facts_subject           ON facts(subject);
CREATE INDEX IF NOT EXISTS idx_facts_predicate         ON facts(predicate);
CREATE INDEX IF NOT EXISTS idx_facts_subject_predicate ON facts(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_facts_upper_subject     ON facts(UPPER(subject), predicate);

-- paper_agent_log
CREATE TABLE IF NOT EXISTS paper_agent_log (
    id          BIGSERIAL PRIMARY KEY,
    user_id     TEXT    NOT NULL,
    event_type  TEXT    NOT NULL,
    ticker      TEXT,
    detail      TEXT,
    created_at  TEXT    NOT NULL,
    bot_id      TEXT
);
CREATE INDEX IF NOT EXISTS idx_pal_user_event ON paper_agent_log(user_id, event_type);
CREATE INDEX IF NOT EXISTS idx_pal_created_at ON paper_agent_log(created_at);
CREATE INDEX IF NOT EXISTS idx_pal_bot_id     ON paper_agent_log(bot_id);

-- paper_bot_equity
CREATE TABLE IF NOT EXISTS paper_bot_equity (
    id             BIGSERIAL PRIMARY KEY,
    bot_id         TEXT    NOT NULL,
    equity_value   REAL    NOT NULL,
    cash_balance   REAL    NOT NULL,
    open_positions INTEGER NOT NULL DEFAULT 0,
    logged_at      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bot_equity_bot ON paper_bot_equity(bot_id, logged_at);

-- signal_calibration (migrated from SQLite — high write + bot-scan read contention)
CREATE TABLE IF NOT EXISTS signal_calibration (
    id                       BIGSERIAL PRIMARY KEY,
    ticker                   TEXT    NOT NULL,
    pattern_type             TEXT    NOT NULL,
    timeframe                TEXT    NOT NULL,
    market_regime            TEXT,
    direction                TEXT,
    sample_size              INTEGER DEFAULT 0,
    hit_rate_t1              REAL,
    hit_rate_t2              REAL,
    hit_rate_t3              REAL,
    stopped_out_rate         REAL,
    avg_time_to_target_hours REAL,
    calibration_confidence   REAL    DEFAULT 0.0,
    last_updated             TEXT    NOT NULL,
    volatility_regime        TEXT,
    sector                   TEXT,
    central_bank_stance      TEXT,
    gdelt_tension_level      TEXT,
    outcome_r_multiple       REAL,
    bot_observations         INTEGER DEFAULT 0,
    user_observations        INTEGER DEFAULT 0,
    UNIQUE(ticker, pattern_type, timeframe, market_regime)
);
CREATE INDEX IF NOT EXISTS idx_calibration_pattern_tf ON signal_calibration(pattern_type, timeframe);
CREATE INDEX IF NOT EXISTS idx_sc_direction           ON signal_calibration(pattern_type, direction, timeframe);
-- Partial unique index for directional cells (direction IS NOT NULL)
CREATE UNIQUE INDEX IF NOT EXISTS idx_sc_directional
    ON signal_calibration(ticker, pattern_type, timeframe, market_regime, direction)
    WHERE direction IS NOT NULL;

-- calibration_observations (companion to signal_calibration)
CREATE TABLE IF NOT EXISTS calibration_observations (
    id            BIGSERIAL PRIMARY KEY,
    ticker        TEXT    NOT NULL,
    pattern_type  TEXT    NOT NULL,
    timeframe     TEXT    NOT NULL,
    market_regime TEXT,
    direction     TEXT,
    outcome       TEXT    NOT NULL,
    source        TEXT    NOT NULL DEFAULT 'user',
    bot_id        TEXT,
    pnl_r         REAL,
    entry_price   REAL,
    exit_price    REAL,
    holding_hours REAL,
    observed_at   TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cal_obs_source ON calibration_observations(source);
CREATE INDEX IF NOT EXISTS idx_cal_obs_cell   ON calibration_observations(ticker, pattern_type, timeframe);

-- strategy_convergence (new — multi-family signal overlap scoring)
CREATE TABLE IF NOT EXISTS strategy_convergence (
    id                BIGSERIAL PRIMARY KEY,
    ticker            TEXT    NOT NULL,
    direction         TEXT    NOT NULL,
    families_active   TEXT    NOT NULL,
    family_count      INTEGER NOT NULL,
    lead_family       TEXT,
    follow_family     TEXT,
    hours_span        REAL,
    convergence_score REAL,
    detected_at       TEXT    NOT NULL,
    expires_at        TEXT,
    outcome_r         REAL,
    outcome_status    TEXT
);
CREATE INDEX IF NOT EXISTS idx_sc_ticker    ON strategy_convergence(ticker);
CREATE INDEX IF NOT EXISTS idx_sc_score     ON strategy_convergence(convergence_score DESC);
CREATE INDEX IF NOT EXISTS idx_sc_expires   ON strategy_convergence(expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sc_ticker_dir ON strategy_convergence(ticker, direction);
