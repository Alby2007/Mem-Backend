-- Trading Galaxy — Postgres schema for the 5 hot tables
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
