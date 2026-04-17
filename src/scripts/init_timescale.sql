-- ─── CS2 Market Analytics — TimescaleDB Init Script (PV-03) ──────────────────
-- Run against a fresh PostgreSQL/TimescaleDB instance before first use.
-- Usage: psql -U cs2user -d cs2 -f scripts/init_timescale.sql
--
-- Column aliases (mapping to task spec):
--   container_name  = market_hash_name  (Steam Market Hash Name)
--   container_type  = item_type
--   system_settings key 'tier:{id}' = tier  (1=Active, 3=Cold)

BEGIN;

-- ─── Extension ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ─── dim_containers ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_containers (
    container_id        TEXT        PRIMARY KEY,
    container_name      TEXT        NOT NULL UNIQUE,   -- market_hash_name
    container_type      TEXT        NOT NULL,           -- item_type
    base_cost           FLOAT8      NOT NULL,
    error_count         INTEGER     NOT NULL DEFAULT 0,
    is_blacklisted      INTEGER     NOT NULL DEFAULT 0  -- 0=active 1=blacklisted
);

CREATE INDEX IF NOT EXISTS ix_dim_containers_name ON dim_containers (container_name);

-- ─── fact_container_prices (hypertable) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_container_prices (
    id                  TEXT        NOT NULL,
    container_id        TEXT        NOT NULL REFERENCES dim_containers (container_id),
    "timestamp"         TIMESTAMPTZ NOT NULL,
    price               FLOAT8,
    mean_price          FLOAT8,
    lowest_price        FLOAT8,
    volume_7d           INTEGER     DEFAULT 0,
    source              TEXT        DEFAULT 'steam_market'
);

SELECT create_hypertable(
    'fact_container_prices',
    'timestamp',
    if_not_exists => TRUE,
    migrate_data  => TRUE
);

-- Regular index for range scans (must come after create_hypertable)
CREATE INDEX IF NOT EXISTS ix_container_price_ts
    ON fact_container_prices (container_id, "timestamp" DESC);

-- Unique constraint: one price snapshot per container per timestamp.
-- Prevents duplicate rows on backfill retry.
-- Must include the partition column (timestamp) — TimescaleDB requirement.
CREATE UNIQUE INDEX IF NOT EXISTS uix_container_price
    ON fact_container_prices (container_id, "timestamp");

-- ─── dim_user_positions ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_user_positions (
    id                  TEXT        PRIMARY KEY,
    container_name      TEXT        NOT NULL,
    buy_price           FLOAT8      NOT NULL,
    quantity            INTEGER     NOT NULL DEFAULT 1,
    buy_date            TIMESTAMPTZ,
    trade_unlock_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_user_positions_name ON dim_user_positions (container_name);

-- ─── fact_portfolio_snapshots ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_portfolio_snapshots (
    id              TEXT        PRIMARY KEY,
    snapshot_date   TIMESTAMPTZ NOT NULL,
    wallet          FLOAT8      NOT NULL,
    inventory       FLOAT8      DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_portfolio_snapshots_date ON fact_portfolio_snapshots (snapshot_date);

-- ─── fact_transactions ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_transactions (
    id              TEXT        PRIMARY KEY,
    trade_date      TIMESTAMPTZ NOT NULL,
    action          TEXT        NOT NULL,  -- BUY / SELL / FLIP
    item_name       TEXT        NOT NULL,
    quantity        INTEGER     DEFAULT 1,
    price           FLOAT8      NOT NULL,
    total           FLOAT8      NOT NULL,
    pnl             FLOAT8,
    listing_id      TEXT,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS ix_transactions_date    ON fact_transactions (trade_date);
CREATE INDEX IF NOT EXISTS ix_transactions_listing ON fact_transactions (listing_id);

-- ─── dim_annual_summary ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_annual_summary (
    year        INTEGER PRIMARY KEY,
    pnl         FLOAT8  NOT NULL,
    notes       TEXT
);

-- ─── fact_portfolio_advice ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_portfolio_advice (
    id                      TEXT        PRIMARY KEY,
    computed_at             TIMESTAMPTZ NOT NULL,
    wallet                  FLOAT8      NOT NULL,
    total_capital           FLOAT8      NOT NULL,
    inventory_value         FLOAT8      NOT NULL,
    flip_budget             FLOAT8      NOT NULL,
    invest_budget           FLOAT8      NOT NULL,
    reserve_amount          FLOAT8      NOT NULL,
    flip_json               TEXT,
    invest_json             TEXT,
    top_flips_json          TEXT,
    top_invests_json        TEXT,
    sell_json               TEXT,
    correlation_warning     TEXT
);

CREATE INDEX IF NOT EXISTS ix_portfolio_advice_computed ON fact_portfolio_advice (computed_at);

-- ─── fact_investment_signals ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_investment_signals (
    id                  TEXT        PRIMARY KEY,
    container_id        TEXT        NOT NULL REFERENCES dim_containers (container_id),
    computed_at         TIMESTAMPTZ NOT NULL,
    verdict             TEXT        NOT NULL,
    score               INTEGER     NOT NULL,
    ratio_signal        TEXT,
    momentum_signal     TEXT,
    trend_signal        TEXT,
    event_signal        TEXT,
    sell_at_loss        INTEGER     NOT NULL DEFAULT 0,
    unrealized_pnl      FLOAT8
);

CREATE INDEX IF NOT EXISTS ix_investment_signal_computed ON fact_investment_signals (computed_at);

-- ─── event_log ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event_log (
    id          TEXT        PRIMARY KEY,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level       TEXT        NOT NULL,
    module      TEXT        NOT NULL,
    message     TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_event_log_ts ON event_log ("timestamp");

-- ─── task_queue ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS task_queue (
    id              TEXT        PRIMARY KEY,
    type            TEXT        NOT NULL,
    priority        INTEGER     NOT NULL DEFAULT 5,
    status          TEXT        NOT NULL DEFAULT 'PENDING',
    payload         JSONB,
    retries         INTEGER     NOT NULL DEFAULT 0,
    deadline_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS ix_task_queue_status_priority ON task_queue (status, priority);

-- ─── worker_registry ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS worker_registry (
    name                TEXT        PRIMARY KEY,
    status              TEXT        NOT NULL DEFAULT 'IDLE',
    last_heartbeat      TIMESTAMPTZ,
    current_task_id     TEXT
);

CREATE INDEX IF NOT EXISTS ix_worker_registry_status ON worker_registry (status);

-- ─── positions ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS positions (
    id                  TEXT        PRIMARY KEY,
    asset_id            BIGINT      NOT NULL,
    classid             TEXT,
    market_id           TEXT,
    is_on_market        INTEGER     NOT NULL DEFAULT 0,
    market_hash_name    TEXT        NOT NULL,
    buy_price           FLOAT8      NOT NULL,
    quantity            INTEGER     NOT NULL DEFAULT 1,
    status              TEXT        NOT NULL DEFAULT 'OPEN',
    opened_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_positions_asset_id  ON positions (asset_id);
CREATE INDEX IF NOT EXISTS ix_positions_classid   ON positions (classid);
CREATE INDEX IF NOT EXISTS ix_positions_market_id ON positions (market_id);
CREATE INDEX IF NOT EXISTS ix_positions_name_status ON positions (market_hash_name, status);

-- ─── system_settings ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS system_settings (
    key         TEXT        PRIMARY KEY,
    value       TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── Views ────────────────────────────────────────────────────────────────────

-- v_net_prices: Steam net price after 15% fee (price / 1.15 − 5 fixed)
CREATE OR REPLACE VIEW v_net_prices AS
SELECT
    fcp.container_id,
    dc.container_name                                   AS market_hash_name,
    fcp."timestamp",
    fcp.price,
    ROUND((fcp.price / 1.15 - 5.0)::NUMERIC, 2)       AS net_price,
    fcp.volume_7d,
    fcp.source
FROM fact_container_prices fcp
JOIN dim_containers dc USING (container_id)
WHERE fcp.price IS NOT NULL;

-- v_analytics: 7-day price volatility per container
--   volatility = (MAX - MIN) / AVG  (coefficient of variation proxy)
CREATE OR REPLACE VIEW v_analytics AS
SELECT
    fcp.container_id,
    dc.container_name                                       AS market_hash_name,
    ROUND(AVG(fcp.price)::NUMERIC, 2)                      AS avg_price,
    ROUND(MIN(fcp.price)::NUMERIC, 2)                      AS min_price,
    ROUND(MAX(fcp.price)::NUMERIC, 2)                      AS max_price,
    ROUND(
        CASE WHEN AVG(fcp.price) > 0
             THEN (MAX(fcp.price) - MIN(fcp.price)) / AVG(fcp.price)
             ELSE NULL
        END::NUMERIC, 4
    )                                                       AS volatility_7d,
    COUNT(*)                                                AS sample_count
FROM fact_container_prices fcp
JOIN dim_containers dc USING (container_id)
WHERE
    fcp.price IS NOT NULL
    AND fcp."timestamp" >= NOW() - INTERVAL '7 days'
GROUP BY fcp.container_id, dc.container_name;

COMMIT;
