-- ─── CS2 Market Analytics — Add armorypass position type ─────────────────────
-- Extends investmentpositiontype enum with 'armorypass' value.
-- Safe to run multiple times (IF NOT EXISTS via DO block check).
-- Usage: psql -U cs2user -d cs2 -f src/scripts/migrate_armorypass_type.sql

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum
        WHERE enumlabel = 'armorypass'
          AND enumtypid = 'investmentpositiontype'::regtype
    ) THEN
        ALTER TYPE investmentpositiontype ADD VALUE 'armorypass';
    END IF;
END $$;
