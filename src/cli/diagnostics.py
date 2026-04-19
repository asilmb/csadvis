"""
Diagnostic commands: cmd_status, cmd_validate_prices, cmd_validate_top,
cmd_monitor, cmd_watchdog, and their helpers.
"""
from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

from config import settings

logger = logging.getLogger(__name__)

# --- Validate prices constants -----------------------------------------------

_VP_DIVERGENCE_THRESHOLD = 50.0  # percent — flag if DB vs Steam API diverges more than this
_VP_DELAY_SECONDS = 4.0  # rate-limit delay between Steam API calls
_VP_COL_NAME = 34
_VP_COL_PRICE = 10
_VP_COL_API = 10
_VP_COL_DIV = 11


# --- Validate prices helpers -------------------------------------------------


def _vp_load_db_data() -> tuple[list, dict]:
    """
    Load all containers and their latest price data from the DB.

    Returns:
        (containers, price_data)
        price_data: {container_name: {current_price, mean_price, quantity, lowest_price}}
    """
    from collections import defaultdict
    from datetime import timedelta

    from sqlalchemy import func

    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer, FactContainerPrice

    cutoff_30d = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)

    db = SessionLocal()
    try:
        containers = db.query(DimContainer).all()
        id_to_name = {str(c.container_id): str(c.container_name) for c in containers}

        # Query 1: latest price row per container
        latest_ts_subq = (
            db.query(
                FactContainerPrice.container_id,
                func.max(FactContainerPrice.timestamp).label("max_ts"),
            )
            .filter(FactContainerPrice.price.isnot(None))
            .group_by(FactContainerPrice.container_id)
            .subquery()
        )
        latest_rows = (
            db.query(FactContainerPrice)
            .join(
                latest_ts_subq,
                (FactContainerPrice.container_id == latest_ts_subq.c.container_id)
                & (FactContainerPrice.timestamp == latest_ts_subq.c.max_ts),
            )
            .all()
        )
        latest_map = {str(r.container_id): r for r in latest_rows}

        # Query 2: last 30 days for mean price
        recent_rows = (
            db.query(FactContainerPrice)
            .filter(
                FactContainerPrice.timestamp >= cutoff_30d,
                FactContainerPrice.price.isnot(None),
            )
            .all()
        )
        prices_by_cid: dict[str, list[float]] = defaultdict(list)
        for r in recent_rows:
            prices_by_cid[str(r.container_id)].append(float(r.price))

        price_data: dict[str, dict] = {}
        for cid, name in id_to_name.items():
            latest = latest_map.get(cid)
            if not latest:
                continue
            prices_30d = prices_by_cid.get(cid, [])
            mean_30d = sum(prices_30d) / len(prices_30d) if prices_30d else None
            price_data[name] = {
                "current_price": float(latest.price) if latest.price else None,
                "mean_price": mean_30d,
                "quantity": int(latest.volume_7d or 0),
                "lowest_price": (
                    float(latest.lowest_price) if latest.lowest_price else None
                ),
            }

        return list(containers), price_data
    finally:
        db.close()


def _vp_select_top_n(
    containers: list,
    price_data: dict[str, dict],
    signals: dict[str, dict],
    n: int,
) -> list[tuple]:
    """
    Select top-N containers by invest signal score (descending), then by name as tiebreaker.
    Only containers with a real current_price are eligible.
    Returns list of (container, db_price).
    """
    ranked = []
    for c in containers:
        cid = str(c.container_id)
        name = str(c.container_name)
        sig = signals.get(cid, {})
        if sig.get("verdict") == "NO DATA":
            continue
        db_price = price_data.get(name, {}).get("current_price")
        if not db_price:
            continue
        score = sig.get("score", 0)
        ranked.append((c, db_price, score, name))

    ranked.sort(key=lambda x: (-x[2], x[3]))  # score desc, name asc
    return [(c, db_price) for c, db_price, _score, _name in ranked[:n]]


async def _vp_fetch_prices(client: object, names: list[str]) -> dict[str, dict]:
    """
    Fetch priceoverview for each name sequentially with a rate-limit delay.
    Returns {name: overview_dict}.
    """
    import asyncio

    results: dict[str, dict] = {}
    total = len(names)
    for idx, name in enumerate(names, 1):
        print(f"  [{idx}/{total}] Querying Steam API: {name} ...", flush=True)
        result = await client.fetch_price_overview(name)  # type: ignore[union-attr]
        results[name] = result
        if idx < total:
            await asyncio.sleep(_VP_DELAY_SECONDS)
    return results


def _format_price(value: float) -> str:
    return f"{value:.0f}{settings.currency_symbol}"


def _vp_print_report(top: list[tuple], api_results: dict[str, dict]) -> None:
    separator = "-" * 70

    print()
    print("=== Price Validation Report ===")
    print(
        f"{'Container':<{_VP_COL_NAME}}"
        f"{'DB Price':<{_VP_COL_PRICE}}"
        f"{'Steam API':<{_VP_COL_API}}"
        f"{'Divergence':<{_VP_COL_DIV}}"
        f"Status"
    )
    print(separator)

    total = len(top)
    ok_count = 0
    flagged_count = 0
    skip_count = 0

    for c, db_price in top:
        name = str(c.container_name)
        api_data = api_results.get(name, {})
        api_price = api_data.get("median_price", 0.0) if api_data else 0.0

        display_name = (
            name if len(name) <= _VP_COL_NAME - 1 else name[: _VP_COL_NAME - 4] + "..."
        )
        db_str = _format_price(db_price)

        if not api_price:
            api_str = "N/A"
            div_str = "N/A"
            status = "SKIP"
            skip_count += 1
        else:
            divergence_pct = abs(api_price - db_price) / db_price * 100
            api_str = _format_price(api_price)
            div_str = f"{divergence_pct:.1f}%"
            if divergence_pct > _VP_DIVERGENCE_THRESHOLD:
                status = "DIVERGENCE"
                flagged_count += 1
            else:
                status = "OK"
                ok_count += 1

        print(
            f"{display_name:<{_VP_COL_NAME}}"
            f"{db_str:<{_VP_COL_PRICE}}"
            f"{api_str:<{_VP_COL_API}}"
            f"{div_str:<{_VP_COL_DIV}}"
            f"{status}"
        )

    print(separator)
    print(
        f"Checked: {total}"
        f"  |  OK: {ok_count}"
        f"  |  Flagged: {flagged_count}"
        f"  |  Skipped (no API data): {skip_count}"
    )
    print()


def cmd_validate_prices(args) -> None:
    """Check top-N containers by invest signal score against live Steam Market API prices."""
    import asyncio

    from scrapper.steam.client import SteamMarketClient
    from src.domain.connection import init_db
    from src.domain.investment import compute_all_investment_signals

    top_n: int = args.top

    try:
        client = SteamMarketClient()
    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

    init_db()
    print("Loading container data from DB ...", flush=True)
    containers, price_data = _vp_load_db_data()
    print(f"  Loaded {len(containers)} containers.", flush=True)

    signals = compute_all_investment_signals(containers, price_data)
    top = _vp_select_top_n(containers, price_data, signals, top_n)

    if not top:
        print(
            "[WARN] No containers with price data found in DB. Run cs2 scrape && cs2 backfill first."
        )
        sys.exit(0)

    print(
        f"\nSelected top-{len(top)} containers by signal score. Querying Steam Market API ...\n",
        flush=True,
    )

    names = [str(c.container_name) for c, _ in top]
    api_results = asyncio.run(_vp_fetch_prices(client, names))

    _vp_print_report(top, api_results)


# --- Monitor helpers ---------------------------------------------------------


def _fmt_age(ts: datetime | None) -> str:
    """Return human-readable age string, e.g. '2m ago', '1h 5m ago', 'never'."""
    if ts is None:
        return "never"
    delta = int((datetime.now(UTC).replace(tzinfo=None) - ts).total_seconds())
    if delta < 0:
        return "just now"
    if delta < 60:
        return f"{delta}s ago"
    if delta < 3600:
        m, s = divmod(delta, 60)
        return f"{m}m {s}s ago"
    h, rem = divmod(delta, 3600)
    m = rem // 60
    return f"{h}h {m}m ago"


def _trunc(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def cmd_status(args) -> None:
    """Show database statistics."""
    from sqlalchemy import func

    from src.domain.connection import SessionLocal, init_db
    from src.domain.models import DimContainer, FactContainerPrice

    init_db()
    with SessionLocal() as db:
        n_containers = db.query(DimContainer).count()
        n_prices = db.query(FactContainerPrice).count()

        latest = (
            db.query(FactContainerPrice)
            .order_by(FactContainerPrice.timestamp.desc())
            .first()
        )
        latest_ts = latest.timestamp.strftime("%Y-%m-%d %H:%M UTC") if latest else "never"

        type_counts = (
            db.query(DimContainer.container_type, func.count())
            .group_by(DimContainer.container_type)
            .all()
        )

    print("\n  CS2 Analytics - Database Status")
    print("  " + "-" * 42)
    print(f"  Containers    : {n_containers}")
    for ctype, count in type_counts:
        print(f"    {ctype.value:<24} {count}")
    print(f"  Price records : {n_prices:,}")
    print(f"  Latest price  : {latest_ts}")

    from scrapper.state import get_state

    state = get_state()
    print(f"  Last scraped  : {state.get('last_parsed', 'never')}")

    from infra.steam_credentials import get_login_secure

    cookie_len = len(get_login_secure())
    if cookie_len == 0:
        print("  Cookie        : NOT SET — enter it via the dashboard cookie form")
    elif cookie_len < 50:
        print(f"  Cookie        : SUSPICIOUS (length={cookie_len}, expected >200)")
    else:
        print(f"  Cookie        : set  (length={cookie_len})")

    from src.domain.event_calendar import EVENTS, is_calendar_stale

    if is_calendar_stale():
        most_recent = max(ev["end"] for ev in EVENTS) if EVENTS else "never"
        print(
            f"  Calendar      : STALE — last event ended {most_recent} (update event_calendar.py)"
        )
    else:
        import datetime as _dt

        upcoming = [ev for ev in EVENTS if ev["start"] >= _dt.date.today()]
        upcoming.sort(key=lambda e: e["start"])
        if upcoming:
            nxt = upcoming[0]
            print(f"  Calendar      : ok — next: {nxt['name']} ({nxt['start']})")
        else:
            print("  Calendar      : ok — no upcoming events in EVENTS list")
    print()


def cmd_monitor(args) -> None:
    """Display work queue state via the API /system/queue-status endpoint."""
    import json
    import urllib.request

    from config import settings

    url = f"http://{settings.api_internal_host}:{settings.api_port}/api/v1/system/queue-status"
    print()
    print("  In-Process Work Queue")
    print("  " + "─" * 42)
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            state = json.loads(resp.read())
        print(f"  Busy        : {'yes' if state.get('busy') else 'no'}")
        print(f"  Current job : {state.get('current_type') or '—'}")
        print(f"  Queue size  : {state.get('queue_size', 0)}")
        print(f"  Last job at : {state.get('last_job_at') or 'never'}")
        print(f"  Restarts    : {state.get('restarts', 0)}")
        if state.get('last_error'):
            print(f"  Last error  : {state['last_error']}")
    except Exception as exc:
        print(f"  [ERROR] Could not reach API: {exc}")
        print("  Make sure the API server is running (python src/main.py api)")
    print()


def cmd_validate_top(args) -> None:
    """Validate top-N flip candidates against live Steam Market API prices."""
    import asyncio

    from scrapper.steam.client import SteamMarketClient
    from src.domain.connection import init_db
    from src.domain.investment import compute_all_investment_signals

    top_n: int = args.top

    try:
        client = SteamMarketClient()
    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

    init_db()
    print("Loading container data from DB ...", flush=True)
    containers, price_data = _vp_load_db_data()
    print(f"  Loaded {len(containers)} containers.", flush=True)

    signals = compute_all_investment_signals(containers, price_data)
    top = _vp_select_top_n(containers, price_data, signals, top_n)

    if not top:
        print("[WARN] No containers with price data found. Run cs2 scrape && cs2 backfill first.")
        sys.exit(0)

    print(f"\nSelected top-{len(top)} containers by signal score. Querying Steam Market API ...\n", flush=True)
    names = [str(c.container_name) for c, _ in top]
    api_results = asyncio.run(_vp_fetch_prices(client, names))
    _vp_print_report(top, api_results)


def cmd_watchdog(args) -> None:
    """Watchdog is no longer needed — the in-process worker self-supervises."""
    print("\n  Watchdog: not applicable — in-process worker has a built-in supervisor.")
    print("  Use `cs2 monitor` to check queue state.\n")
