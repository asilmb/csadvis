#!/usr/bin/env bash
# backup_db.sh — PostgreSQL backup with rotation (PV-07)
#
# Creates a compressed pg_dump and stores it under /app/backups (named volume).
# Retains only the 7 most-recent daily archives; older files are deleted.
#
# Environment variables (read from container env or .env):
#   POSTGRES_USER     (default: cs2user)
#   POSTGRES_PASSWORD (default: cs2pass)
#   POSTGRES_DB       (default: cs2)
#   DB_HOST           (default: db)
#   DB_PORT           (default: 5432)
#
# Usage (inside worker container):
#   bash /app/scripts/backup_db.sh
#
# Exit codes:
#   0 — success
#   1 — pg_dump failed

set -euo pipefail

PGUSER="${POSTGRES_USER:-cs2user}"
PGPASSWORD_VAL="${POSTGRES_PASSWORD:-cs2pass}"
PGDB="${POSTGRES_DB:-cs2}"
PGHOST="${DB_HOST:-db}"
PGPORT="${DB_PORT:-5432}"

BACKUP_DIR="/app/backups"
KEEP_DAYS=7

mkdir -p "${BACKUP_DIR}"

TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")
DUMP_FILE="${BACKUP_DIR}/cs2_${TIMESTAMP}.dump"
ARCHIVE="${BACKUP_DIR}/cs2_${TIMESTAMP}.tar.gz"

echo "[backup] Starting pg_dump for database '${PGDB}' on ${PGHOST}:${PGPORT} ..."

PGPASSWORD="${PGPASSWORD_VAL}" pg_dump \
    --host="${PGHOST}" \
    --port="${PGPORT}" \
    --username="${PGUSER}" \
    --format=custom \
    --no-password \
    "${PGDB}" \
    --file="${DUMP_FILE}"

if [ $? -ne 0 ]; then
    echo "[backup] ERROR: pg_dump failed — aborting." >&2
    rm -f "${DUMP_FILE}"
    exit 1
fi

# Pack into .tar.gz and remove the raw dump
tar -czf "${ARCHIVE}" -C "${BACKUP_DIR}" "$(basename "${DUMP_FILE}")"
rm -f "${DUMP_FILE}"

ARCHIVE_SIZE=$(du -sh "${ARCHIVE}" | cut -f1)
echo "[backup] Archive created: ${ARCHIVE} (${ARCHIVE_SIZE})"

# ── Rotation: keep only the last KEEP_DAYS archives ──────────────────────────
ARCHIVE_COUNT=$(find "${BACKUP_DIR}" -maxdepth 1 -name "cs2_*.tar.gz" | wc -l)

if [ "${ARCHIVE_COUNT}" -gt "${KEEP_DAYS}" ]; then
    DELETE_COUNT=$(( ARCHIVE_COUNT - KEEP_DAYS ))
    echo "[backup] Rotating — deleting ${DELETE_COUNT} old archive(s) (keeping ${KEEP_DAYS}) ..."
    find "${BACKUP_DIR}" -maxdepth 1 -name "cs2_*.tar.gz" \
        | sort \
        | head -n "${DELETE_COUNT}" \
        | xargs rm -f
fi

REMAINING=$(find "${BACKUP_DIR}" -maxdepth 1 -name "cs2_*.tar.gz" | wc -l)
echo "[backup] Done — ${REMAINING} archive(s) stored in ${BACKUP_DIR}"
