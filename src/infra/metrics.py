"""Metrics stubs — Prometheus/StatsD infrastructure removed.

inc_prices_fetched() and inc_steam_429() are no-ops kept for call-site
compatibility. collect_metrics() returns an empty response so the /metrics
route continues to work without a Prometheus server.
"""

from __future__ import annotations


def inc_prices_fetched() -> None:
    """No-op stub (Prometheus infrastructure removed)."""


def inc_steam_429() -> None:
    """No-op stub (Prometheus infrastructure removed)."""


def collect_metrics() -> str:
    """Return empty metrics document (no Prometheus scraper configured)."""
    return ""
