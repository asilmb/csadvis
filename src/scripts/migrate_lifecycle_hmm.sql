-- ─── LC-1: dynamic behavioral lifecycle (HMM + hysteresis) ───────────────────
-- Adds columns to dim_containers needed by the new behavioral lifecycle
-- classifier (replaces age-based NEW/ACTIVE/AGING/LEGACY/DEAD).
-- Idempotent — safe to run multiple times.
-- Usage: psql -U cs2user -d cs2 -f src/scripts/migrate_lifecycle_hmm.sql

ALTER TABLE dim_containers
    ADD COLUMN IF NOT EXISTS current_lifecycle_phase   VARCHAR(40),
    ADD COLUMN IF NOT EXISTS lifecycle_updated_at      TIMESTAMP,
    ADD COLUMN IF NOT EXISTS expected_return_1m        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS expected_return_3m        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS expected_return_6m        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS forecast_invalidated_at   TIMESTAMP;
