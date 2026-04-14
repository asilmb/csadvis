# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install production dependencies into an isolated venv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev


# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Runtime system dep: libpq5 required by psycopg2-binary at import time
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --gid 1001 appuser \
 && useradd --uid 1001 --gid appuser --no-create-home --shell /bin/false appuser

WORKDIR /app

# Virtualenv from builder
COPY --from=builder /app/.venv /app/.venv

# Application source
COPY src/       src/
COPY config.py  .
COPY infra/     infra/
COPY seed/      seed/
COPY scheduler/ scheduler/

RUN mkdir -p /app/logs \
 && chown -R appuser:appuser /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PATH="/app/.venv/bin:$PATH" \
    LOG_DIR=/app/logs

USER appuser

EXPOSE 8000 8050

ENTRYPOINT ["python", "src/main.py"]
CMD ["api"]
