-- ─── CS2 Market Analytics — Add linked_asset_ids to investment_positions ──────
-- Stores JSON array of Steam asset_ids bound to an Armory Pass position.
-- Usage: psql -U cs2user -d cs2 -f src/scripts/migrate_ap_assets.sql

ALTER TABLE investment_positions
    ADD COLUMN IF NOT EXISTS linked_asset_ids TEXT NOT NULL DEFAULT '[]';
