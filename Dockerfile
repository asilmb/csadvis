# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps: gcc + libpq-dev for psycopg2, libffi-dev for brotlicffi/lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Runtime libpq (required by psycopg2-binary at import time)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd --gid 1001 appuser \
 && useradd --uid 1001 --gid appuser --no-create-home --shell /bin/false appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY . .

# Persistent data lives in a Docker volume (not in the image layer)
RUN mkdir -p /data/logs \
 && chown -R appuser:appuser /app /data

# Logging: Python output unbuffered → goes directly to stdout/stderr
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    DATABASE_PATH=/data/cs2_analytics.db \
    LOG_DIR=/data/logs

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
 && chown appuser:appuser /entrypoint.sh

USER appuser

EXPOSE 8000 8050

ENTRYPOINT ["/entrypoint.sh"]
