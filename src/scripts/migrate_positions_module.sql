-- ─── CS2 Market Analytics — Positions module migration ───────────────────────
-- Adds: transaction_groups, investment_positions, position_transaction_groups
--       FK transaction_group_id on fact_transactions
-- Safe to run on existing DB (IF NOT EXISTS / IF EXISTS guards throughout).
-- Usage: psql -U cs2user -d cs2 -f src/scripts/migrate_positions_module.sql

BEGIN;

-- ─── Enum types ───────────────────────────────────────────────────────────────

DO $$ BEGIN
    CREATE TYPE transactiondirection AS ENUM ('BUY', 'SELL');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE investmentpositiontype AS ENUM ('flip', 'investment');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE investmentpositionstatus AS ENUM ('hold', 'on_sale', 'sold');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE linkstatus AS ENUM ('undefined', 'defined', 'skipped');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ─── transaction_groups ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS transaction_groups (
    id                   VARCHAR(36)              PRIMARY KEY,
    name                 VARCHAR(300)             NOT NULL,
    direction            transactiondirection     NOT NULL,
    item_name            VARCHAR(200)             NOT NULL,
    container_id         VARCHAR(36)              REFERENCES dim_containers(container_id) ON DELETE SET NULL,
    count                INTEGER                  NOT NULL,
    price                FLOAT                    NOT NULL,
    date_from            TIMESTAMP                NOT NULL,
    date_to              TIMESTAMP                NOT NULL,
    trade_ban_expires_at TIMESTAMP,
    created_at           TIMESTAMP                NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_transaction_groups_item_name ON transaction_groups(item_name);

-- ─── investment_positions ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS investment_positions (
    id                   VARCHAR(36)                PRIMARY KEY,
    name                 VARCHAR(300)               NOT NULL,
    container_id         VARCHAR(36)                NOT NULL REFERENCES dim_containers(container_id),
    position_type        investmentpositiontype     NOT NULL,
    fixation_count       INTEGER                    NOT NULL,
    current_count        INTEGER                    NOT NULL,
    buy_price            FLOAT                      NOT NULL,
    sale_target_price    FLOAT                      NOT NULL,
    status               investmentpositionstatus   NOT NULL DEFAULT 'hold',
    opened_at            TIMESTAMP                  NOT NULL DEFAULT NOW(),
    closed_at            TIMESTAMP,
    balance_influence    FLOAT
);

CREATE INDEX IF NOT EXISTS ix_investment_positions_container_id ON investment_positions(container_id);
CREATE INDEX IF NOT EXISTS ix_investment_positions_status ON investment_positions(status);

-- ─── position_transaction_groups ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS position_transaction_groups (
    id                   VARCHAR(36)   PRIMARY KEY,
    position_id          VARCHAR(36)   REFERENCES investment_positions(id) ON DELETE SET NULL,
    transaction_group_id VARCHAR(36)   NOT NULL UNIQUE REFERENCES transaction_groups(id),
    link_status          linkstatus    NOT NULL DEFAULT 'undefined',
    linked_at            TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_ptg_link_status ON position_transaction_groups(link_status);

-- ─── fact_transactions — add FK column ───────────────────────────────────────

ALTER TABLE fact_transactions
    ADD COLUMN IF NOT EXISTS transaction_group_id VARCHAR(36)
        REFERENCES transaction_groups(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS ix_fact_transactions_group_id ON fact_transactions(transaction_group_id);

COMMIT;
