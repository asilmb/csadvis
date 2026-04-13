# Overview

A Python-based **CS2 container investment analytics platform** for personal use.
Tracks prices of CS2 **weapon cases and capsules** on Steam Market, generates investment signals,
advises on portfolio allocation, and displays everything in a Plotly Dash dashboard.

**NOT** a skin-opening EV calculator. Focus is purely on **containers as tradeable assets**.

## How to Run

| What | Command |
|------|---------|
| All-in-one (Docker) | `docker compose up` |
| App service (API + Dashboard) | `python -m cli start` |
| Celery worker | `python -m cli worker [--workers N] [--beat]` |
| Status | `python -m cli status` |
| Monitor queue | `python -m cli monitor` |
| Validate prices | `python -m cli validate-prices [--top N]` |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Backend API | FastAPI + Uvicorn |
| Dashboard | Plotly Dash + Dash Bootstrap Components |
| Database | PostgreSQL 16 (TimescaleDB extension) via SQLAlchemy 2.x ORM |
| Task Queue | Celery + Redis |
| Broker/Cache | Redis 7 (task queue, stealth-block state, nameid cache, cookie store) |
| Scraper | Steam Market Search API (async httpx) |
| HTTP | curl_cffi (Steam Market — TLS impersonation, Chrome 120) + httpx (inventory, wallet, scraper) |
| Data | numpy, scipy, pandas |
| Config | pydantic-settings (.env file) |
| Logging | structlog (JSON format in containers) |
| Tests | pytest |
| Containers | Docker Compose (db, redis, app, worker, beat) |

## Docker Services

| Service | Role | Ports |
|---------|------|-------|
| `db` | TimescaleDB (PostgreSQL 16) | 5432 |
| `redis` | Broker + state cache | 6379 |
| `app` | FastAPI + Dash | 8000, 8050 |
| `worker` | Celery worker (price fetching, inventory) | — |
| `beat` | Celery Beat scheduler | — |
