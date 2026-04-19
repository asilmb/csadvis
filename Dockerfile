# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install production dependencies into an isolated venv
COPY pyproject.toml uv.lock ./
RUN uv sync --no-install-project --no-dev && \
    uv pip install cryptography>=42.0.0 --python /app/.venv/bin/python 2>/dev/null || \
    /app/.venv/bin/pip install "cryptography>=42.0.0"

# ── Stage 1b: dev/test dependency builder ─────────────────────────────────────
FROM builder AS builder-dev

RUN uv sync --no-install-project


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

RUN mkdir -p /app/logs \
 && chown -R appuser:appuser /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/src \
    PATH="/app/.venv/bin:$PATH" \
    LOG_DIR=/app/logs

USER appuser

EXPOSE 8000 8050

ENTRYPOINT ["python", "src/main.py"]
CMD ["api"]

# ── Stage 3: test runner ───────────────────────────────────────────────────────
FROM python:3.12-slim AS test

RUN apt-get update && apt-get install -y --no-install-recommends libpq5 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder-dev /app/.venv /app/.venv

COPY src/       src/
COPY tests/     tests/
COPY config.py  .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/src \
    PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["pytest"]
CMD ["tests/", "-v", "--tb=short", "-m", "not e2e and not slow"]
