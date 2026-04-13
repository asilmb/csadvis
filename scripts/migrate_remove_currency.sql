-- ─── CS2 Market Analytics — Currency-Agnostic Migration ──────────────────────
-- Renames all currency-suffixed columns to neutral names.
-- Safe to run on an existing database; uses IF EXISTS guards throughout.
-- Usage: psql -U cs2user -d cs2 -f scripts/migrate_remove_currency.sql

BEGIN;

-- ─── dim_containers ───────────────────────────────────────────────────────────
ALTER TABLE dim_containers
    RENAME COLUMN base_cost_kzt TO base_cost;

-- ─── fact_container_prices ────────────────────────────────────────────────────
ALTER TABLE fact_container_prices
    RENAME COLUMN price_kzt        TO price;

ALTER TABLE fact_container_prices
    RENAME COLUMN mean_kzt         TO mean_price;

ALTER TABLE fact_container_prices
    RENAME COLUMN lowest_price_kzt TO lowest_price;

-- ─── dim_user_positions ───────────────────────────────────────────────────────
ALTER TABLE dim_user_positions
    RENAME COLUMN buy_price_kzt TO buy_price;

-- ─── fact_portfolio_snapshots ─────────────────────────────────────────────────
ALTER TABLE fact_portfolio_snapshots
    RENAME COLUMN wallet_kzt    TO wallet;

ALTER TABLE fact_portfolio_snapshots
    RENAME COLUMN inventory_kzt TO inventory;

ALTER TABLE fact_portfolio_snapshots
    DROP COLUMN IF EXISTS rate_kzt_usd;

-- ─── fact_transactions ────────────────────────────────────────────────────────
ALTER TABLE fact_transactions
    RENAME COLUMN price_kzt TO price;

ALTER TABLE fact_transactions
    RENAME COLUMN total_kzt TO total;

ALTER TABLE fact_transactions
    RENAME COLUMN pnl_kzt   TO pnl;

ALTER TABLE fact_transactions
    DROP COLUMN IF EXISTS pnl_usd;

ALTER TABLE fact_transactions
    DROP COLUMN IF EXISTS rate_kzt_usd;

-- ─── dim_annual_summary ───────────────────────────────────────────────────────
ALTER TABLE dim_annual_summary
    RENAME COLUMN pnl_kzt TO pnl;

-- ─── fact_portfolio_advice ────────────────────────────────────────────────────
ALTER TABLE fact_portfolio_advice
    RENAME COLUMN wallet_kzt          TO wallet;

ALTER TABLE fact_portfolio_advice
    RENAME COLUMN inventory_value_kzt TO inventory_value;

-- ─── fact_investment_signals ──────────────────────────────────────────────────
ALTER TABLE fact_investment_signals
    RENAME COLUMN unrealized_pnl_kzt TO unrealized_pnl;

-- ─── Recreate views with renamed columns ─────────────────────────────────────

DROP VIEW IF EXISTS v_net_prices;
CREATE VIEW v_net_prices AS
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

DROP VIEW IF EXISTS v_analytics;
CREATE VIEW v_analytics AS
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
