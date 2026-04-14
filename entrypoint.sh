#!/bin/sh
# entrypoint.sh — container startup for CS2 Market Analytics Platform
# Runs as non-root user appuser (uid=1001).
set -e

# ── 1. Critical environment variable check ────────────────────────────────────
# STEAM_LOGIN_SECURE is required for market price fetching.
# Warn clearly but do NOT abort — read-only analytics (backfill, dashboard) work without it.
if [ -z "${STEAM_LOGIN_SECURE}" ]; then
    echo "WARNING: STEAM_LOGIN_SECURE is not set. Live price fetching will be disabled." >&2
fi

# Abort when no PostgreSQL connection info is available at all.
if [ -z "${POSTGRES_USER}" ] && [ -z "${DATABASE_URL}" ]; then
    echo "ERROR: Neither POSTGRES_USER nor DATABASE_URL is set. Cannot connect to PostgreSQL." >&2
    exit 1
fi

# ── 2. DB initialisation / migrations ─────────────────────────────────────────
echo "INFO: Connecting to PostgreSQL at ${DB_HOST:-db}:${DB_PORT:-5432}/${POSTGRES_DB:-cs2} ..."
python - <<'EOF'
from src.domain.connection import init_db
init_db()
print("INFO: Database ready.")
EOF

# ── 3. Dispatch to requested service ─────────────────────────────────────────
# CMD is passed as arguments to this script.
# docker-compose service `app`    → CMD ["app"]
# docker-compose service `worker` → CMD ["worker"]

SERVICE="${1:-app}"
shift

case "$SERVICE" in
    app)
        echo "INFO: Starting app service (API + Dashboard) ..."
        exec python -m cli start
        ;;
    worker)
        echo "INFO: Starting worker service (TaskWorker + Watchdog) ..."
        exec python -m cli worker "$@"
        ;;
    *)
        echo "INFO: Running custom command: $*"
        exec "$@"
        ;;
esac
