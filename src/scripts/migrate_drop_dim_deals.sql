-- ─── CS2 Market Analytics — Drop dim_deals ───────────────────────────────────
-- DimDeal/DealStatus removed in favour of the new TransactionGroup +
-- InvestmentPosition module. Table may not exist on fresh installs.
-- Usage: psql -U cs2user -d cs2 -f src/scripts/migrate_drop_dim_deals.sql

BEGIN;

DROP TABLE IF EXISTS dim_deals CASCADE;
DROP TYPE  IF EXISTS dealstatus;

COMMIT;
