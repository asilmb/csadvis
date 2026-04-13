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

    from database.connection import SessionLocal
    from database.models import DimContainer, FactContainerPrice

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

    from database.connection import init_db
    from engine.investment import compute_all_investment_signals
    from ingestion.steam.client import SteamMarketClient

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

    from database.connection import SessionLocal, init_db
    from database.models import DimContainer, FactContainerPrice

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

    from scraper.state import get_state

    state = get_state()
    print(f"  Last scraped  : {state.get('last_parsed', 'never')}")

    from config import settings

    cookie_len = len(settings.steam_login_secure.strip())
    if cookie_len == 0:
        print("  Cookie        : NOT SET — price history and transactions unavailable")
    elif cookie_len < 50:
        print(f"  Cookie        : SUSPICIOUS (length={cookie_len}, expected >200)")
    else:
        print(f"  Cookie        : set  (length={cookie_len})")

    from engine.event_calendar import EVENTS, is_calendar_stale

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
    """Display Worker Registry + Task Queue statistics."""
    from database.connection import SessionLocal, init_db
    from database.models import TaskQueue, TaskStatus, WorkerRegistry

    init_db()

    with SessionLocal() as db:
        workers = db.query(WorkerRegistry).order_by(WorkerRegistry.name).all()
        queue_counts = (
            db.query(TaskQueue.status, TaskQueue.status)
            .all()  # fetch all statuses for manual grouping
        )
        _ = queue_counts  # unused — we query per-status below

        counts: dict[str, int] = {}
        for st in TaskStatus:
            counts[str(st)] = db.query(TaskQueue).filter(TaskQueue.status == st).count()

        oldest = (
            db.query(TaskQueue)
            .filter(TaskQueue.status.in_([TaskStatus.PENDING, TaskStatus.RETRY]))
            .order_by(TaskQueue.priority.asc(), TaskQueue.created_at.asc())
            .limit(5)
            .all()
        )

    # ── Worker Registry ───────────────────────────────────────────────────────
    W = (16, 12, 18, 36)  # col widths: name, status, heartbeat, task_id
    _SEP = "  " + "─" * (sum(W) + len(W) * 2)
    print()
    print("  Worker Registry")
    print(_SEP)
    print(
        f"  {'Name':<{W[0]}}  {'Status':<{W[1]}}  {'Last Heartbeat':<{W[2]}}  {'Active Task':<{W[3]}}"
    )
    print(_SEP)
    if not workers:
        print("  (no workers registered)")
    for w in workers:
        task_cell = _trunc(w.current_task_id or "—", W[3])
        print(
            f"  {_trunc(w.name, W[0]):<{W[0]}}"
            f"  {_trunc(w.status, W[1]):<{W[1]}}"
            f"  {_fmt_age(w.last_heartbeat):<{W[2]}}"
            f"  {task_cell:<{W[3]}}"
        )
    print(_SEP)

    # ── Task Queue ────────────────────────────────────────────────────────────
    Q = (14, 8)  # status, count
    _QSEP = "  " + "─" * (sum(Q) + 2)
    print()
    print("  Task Queue — Status Summary")
    print(_QSEP)
    print(f"  {'Status':<{Q[0]}}  {'Count':>{Q[1]}}")
    print(_QSEP)
    for st_name, count in counts.items():
        print(f"  {st_name:<{Q[0]}}  {count:>{Q[1]}}")
    print(_QSEP)

    # ── Oldest active tasks ───────────────────────────────────────────────────
    if oldest:
        T = (8, 20, 10, 14, 8)  # id(short), type, priority, age, retries
        _TSEP = "  " + "─" * (sum(T) + len(T) * 2)
        print()
        print("  Oldest PENDING / RETRY tasks (top 5)")
        print(_TSEP)
        print(
            f"  {'ID':>{T[0]}}"
            f"  {'Type':<{T[1]}}"
            f"  {'Priority':>{T[2]}}"
            f"  {'Age':<{T[3]}}"
            f"  {'Retries':>{T[4]}}"
        )
        print(_TSEP)
        for t in oldest:
            short_id = str(t.id)[:8]
            print(
                f"  {short_id:>{T[0]}}"
                f"  {_trunc(str(t.type), T[1]):<{T[1]}}"
                f"  {t.priority:>{T[2]}}"
                f"  {_fmt_age(t.created_at):<{T[3]}}"
                f"  {t.retries:>{T[4]}}"
            )
        print(_TSEP)
    print()


def cmd_validate_top(args) -> None:
    """
    Enqueue an on-demand JIT validation task for the current top-N flip candidates.

    Reads top_flips from the latest FactPortfolioAdvice row, extracts up to
    --top names (default 3), and enqueues a HIGH-priority "market_validation"
    task.  Then polls the TaskQueue for up to --timeout seconds and prints the
    final status.

    Requires workers to be running (cs2 start) to process the task.
    """
    import time

    from database.connection import SessionLocal, init_db
    from database.models import TaskQueue
    from services.portfolio import get_latest_advice
    from services.task_manager import TaskQueueService

    top_n: int = args.top
    timeout: int = args.timeout

    init_db()

    advice = get_latest_advice()
    top_flips: list[dict] = advice.get("top_flips") or [] if advice else []
    if not top_flips:
        print(
            "[WARN] No top flip candidates found in DB. "
            "Run cs2 start (or cs2 scrape + portfolio refresh) first."
        )
        sys.exit(0)

    names = [str(f["name"]) for f in top_flips[:top_n] if f.get("name")]
    if not names:
        print("[WARN] top_flips entries have no 'name' field — cannot validate.")
        sys.exit(1)

    print(f"\n  Validate-Top — enqueueing {len(names)} candidate(s) at HIGH priority:")
    for i, n in enumerate(names, 1):
        print(f"    {i}. {n}")
    print()

    svc = TaskQueueService()
    dto = svc.enqueue("market_validation", priority=1, payload={"names": names})
    if dto is None:
        print(
            "  [INFO] Identical market_validation task already PENDING / PROCESSING — "
            "no duplicate enqueued."
        )
        sys.exit(0)

    task_id = dto.id
    print(f"  Task enqueued: {task_id}")
    print(f"  Waiting up to {timeout}s for completion ...\n")

    deadline = time.monotonic() + timeout
    status = "PENDING"
    while time.monotonic() < deadline:
        with SessionLocal() as db:
            row = db.query(TaskQueue).filter(TaskQueue.id == task_id).first()
        if row is None:
            print("  [ERROR] Task disappeared from queue — unexpected state.")
            sys.exit(1)
        status = str(row.status)
        if status in ("COMPLETED", "FAILED"):
            print(f"  Task {task_id[:8]}… → {status}")
            print()
            sys.exit(0 if status == "COMPLETED" else 1)
        time.sleep(2)

    print(f"  [TIMEOUT] Task {task_id[:8]}… still in status {status!r} after {timeout}s.")
    print("  Workers may not be running. Use `cs2 monitor` to check queue state.")
    print()


def cmd_watchdog(args) -> None:
    """
    Run the stuck-task watchdog once.

    Finds workers with stale heartbeats (> 90s) whose PROCESSING tasks have
    exceeded the per-type TTL.  Reclaimed tasks are reset to PENDING.
    """
    from database.connection import init_db
    from services.task_manager import TASK_TTL, WORKER_STUCK_THRESHOLD_S, TaskQueueService

    init_db()

    print(
        f"\n  Watchdog — stuck threshold: {WORKER_STUCK_THRESHOLD_S}s"
        f" | task TTLs: {TASK_TTL}\n"
    )

    svc = TaskQueueService()
    n = svc.reclaim_stuck_tasks()

    if n == 0:
        print("  No stuck tasks found.\n")
    else:
        print(f"  Reclaimed {n} task(s) → reset to PENDING.\n")
