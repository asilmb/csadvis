"""
Portfolio Advisor — 40 / 40 / 20 balance allocation engine.

Given a Steam wallet balance and market data, returns concrete actions:

  sell    — inventory items with SELL signal → realise first, add proceeds to balance
  flip    — best container to flip (7-day trade ban cycle)
  invest  — best container for long-term investment (CS2 gold, high CAGR)
  reserve — 20 % kept free (never deployed)

Allocation ratios:
  40 % → FLIP   (weekly cycle, buy now, sell in 7 days)
  40 % → INVEST (long-term hold, months–years)
  20 % → RESERVE (liquid buffer)

Flip criteria (all must pass):
  net_per_unit > 0               (profitable after Steam 15 % fee)
  weekly_volume >= planned_qty   (market can absorb the position)
  volatility_30d < 15 %          (price stable enough to exit within 7 days)
  flip_score = unit_margin_pct × volume_factor × (1 - volatility)

Invest criterion (CAGR-based):
  CAGR = (current / oldest) ^ (1/years) − 1
  invest_score = CAGR × (1 − volatility_180d)
  Minimum: CAGR > 7.2 % and ≥ 1 year of history
"""

from __future__ import annotations

import logging
import statistics
from datetime import UTC, datetime, timedelta

from config import settings
from src.domain.analytics.armory_advisor import DEFAULT_REWARD_CATALOG as _ARMORY_POOL
from src.domain.events import SuperDealDetected
from src.domain.lifecycle import classify_lifecycle, is_flip_eligible, is_invest_eligible
from src.domain.services import SuperDealDomainService
from src.domain.specifications import PriceWithinRange, ZScoreBelow
from src.domain.value_objects import ROI, Amount
from src.domain.wall_filter import compute_wall_metrics, get_best_buy_order

logger = logging.getLogger(__name__)

_STEAM_FEE_DIV = settings.steam_fee_divisor  # 1.15 — overridable via .env
_STEAM_MIN_FEE = settings.steam_fee_fixed  # ~5₸ — overridable via .env
_VOLATILITY_MIN_FLIP = 0.05  # 5 % min — below this the asset is dead (no movement)
_VOLATILITY_MAX_FLIP = 0.30  # 30 % max — above this price is too chaotic to predict exit
_MIN_NET_CAGR = 0.01  # 1 % minimum NET annual return after Steam fee
_SPREAD_MAX_FLIP = 0.25  # 25 % max bid-ask spread for flip
_LIQUIDITY_MIN_DAILY = 1000  # minimum avg daily volume for flip (absolute floor)

# ─── Super Deal constants (PV-15) ─────────────────────────────────────────────
_SUPER_DEAL_RATIO = 0.70  # price < baseline * 0.70  (30 % discount trigger)
_SUPER_DEAL_ZSCORE = -3.0  # Z-score threshold for statistical anomaly
_Z_SCORE_WINDOW = 60  # rolling window for Z-score (days)
_SUPER_DEAL_Z_ONSET = -2.0  # Z-score level that marks crash onset (for pre_crash_mean)
_MIN_HISTORY_DAYS = 90  # minimum price history to qualify (mature asset guard)
_VOL_SPIKE_THRESHOLD = 2.0  # max avg_vol_7d / avg_vol_30d ratio (supply shock guard)
_MIN_SUPER_DEAL_MARGIN = 0.20  # minimum expected net margin after Steam fee (20 %)
_MAX_DAYS_AT_LOW = 7  # max consecutive days at low before treating as new baseline


def _parse_ts(h: dict) -> datetime:
    """Parse the 'timestamp' string from a price history dict into a datetime."""
    ts: str = h.get("timestamp", "")
    if len(ts) == 16:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M")
    return datetime.fromisoformat(ts[:19])


def _net(sell_price: float) -> float:
    """Steam seller net after 15 % fee."""
    return sell_price / _STEAM_FEE_DIV - _STEAM_MIN_FEE


def _volatility(prices: list[float]) -> float | None:
    """std / mean of a price list.  Returns None if insufficient data (< 5 prices or mean == 0)."""
    if len(prices) < 5:
        return None
    mean_p = statistics.mean(prices)
    if mean_p == 0:
        return None
    return statistics.stdev(prices) / mean_p


def _prices_in_window(history: list[dict], days: int) -> list[float]:
    """Extract price values from history that fall within the last N days.

    Returns prices sorted chronologically (oldest → newest) so that
    callers can rely on out[-1] being the most recent price.
    """
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)
    out: list[tuple[datetime, float]] = []
    for h in history:
        try:
            dt = _parse_ts(h)
            if dt >= cutoff and h.get("price"):
                out.append((dt, float(h["price"])))
        except (ValueError, TypeError, KeyError):
            logger.debug("_prices_in_window: skipping malformed row %r", h)
            continue
    out.sort(key=lambda x: x[0])
    return [p for _, p in out]


def _compute_cagr(history: list[dict]) -> float | None:
    """
    Compute annualised CAGR from oldest to newest price.
    Returns None if < 1 year of history or insufficient data.
    """
    valid = []
    for h in history:
        try:
            dt = _parse_ts(h)
            if h.get("price") and h["price"] > 0:
                valid.append((dt, float(h["price"])))
        except (ValueError, TypeError, KeyError):
            logger.debug("_compute_cagr: skipping malformed row %r", h)
            continue

    if len(valid) < 30:
        return None

    valid.sort(key=lambda x: x[0])
    oldest_dt, oldest_price = valid[0]
    current_price = valid[-1][1]
    years = (datetime.now(UTC).replace(tzinfo=None) - oldest_dt).days / 365.25

    if years < 1.0:
        return None
    try:
        return float((current_price / oldest_price) ** (1.0 / years) - 1.0)
    except (ZeroDivisionError, ValueError):
        return None


def _compute_cagr_metrics(history: list[dict]) -> tuple[float, float, float]:
    """
    Compute (gross_cagr, net_cagr, years) from price history.

    Both CAGR values share the same oldest valid-price row as anchor,
    eliminating the time-period inconsistency of the prior implementation.

    Returns (0.0, 0.0, 0.0) when history is insufficient (< 30 rows with price).
    Returns (gross_cagr, 0.0, years) when net_proceeds <= 0 (ultra-cheap items).

    net_cagr formula:
        net_proceeds = _net(current_price)   # = current_price / 1.15 − 5₸
        net_cagr = (net_proceeds / oldest_price) ^ (1 / years) − 1
    """
    valid: list[tuple[datetime, float]] = []
    for h in history:
        try:
            dt = _parse_ts(h)
            if h.get("price") and h["price"] > 0:
                valid.append((dt, float(h["price"])))
        except (ValueError, TypeError, KeyError):
            continue

    if len(valid) < 30:
        return (0.0, 0.0, 0.0)

    valid.sort(key=lambda x: x[0])
    oldest_dt, oldest_price = valid[0]
    current_price = valid[-1][1]
    years = (datetime.now(UTC).replace(tzinfo=None) - oldest_dt).days / 365.25

    if years < 0.1:
        return (0.0, 0.0, years)

    try:
        gross_cagr = float((current_price / oldest_price) ** (1.0 / years) - 1.0)
    except (ZeroDivisionError, ValueError):
        return (0.0, 0.0, years)

    net_proceeds = _net(current_price)
    if net_proceeds <= 0:
        return (gross_cagr, 0.0, years)

    try:
        net_cagr = float((net_proceeds / oldest_price) ** (1.0 / years) - 1.0)
    except (ZeroDivisionError, ValueError):
        net_cagr = 0.0

    return (gross_cagr, net_cagr, years)


def _volumes_in_window(history: list[dict], days: int) -> list[float]:
    """Extract volume_7d values from history that fall within the last N days."""
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)
    out: list[float] = []
    for h in history:
        try:
            dt = _parse_ts(h)
            v = h.get("volume_7d")
            if dt >= cutoff and v is not None:
                out.append(float(v))
        except (ValueError, TypeError, KeyError):
            continue
    return out


def _compute_zscore(history: list[dict], window_days: int) -> float | None:
    """
    Z-score of the most recent price vs the rolling N-day window.

    Uses all prices in the window (including the current price) for mean/std.
    Returns None when fewer than 10 prices are available or std is zero.
    """
    prices = _prices_in_window(history, window_days)
    if len(prices) < 10:
        return None
    mean_p = statistics.mean(prices)
    try:
        std_p = statistics.stdev(prices)
    except statistics.StatisticsError:
        return None
    if std_p == 0.0:
        return None
    return (prices[-1] - mean_p) / std_p


def _pre_crash_mean(history: list[dict], z_onset_threshold: float) -> float | None:
    """
    Mean price for the 30 days immediately before the anomaly onset.

    Onset = first point in chronological history where the 60-day rolling
    Z-score fell at or below z_onset_threshold.  Using -2.0 as onset (earlier
    than the -3.0 trigger) captures the pre-panic reference mean before the
    market started moving toward the anomaly.

    Returns None when onset is not detected or insufficient pre-onset data.
    """
    dated: list[tuple[datetime, float]] = []
    for h in history:
        try:
            dt = _parse_ts(h)
            if h.get("price") and h["price"] > 0:
                dated.append((dt, float(h["price"])))
        except (ValueError, TypeError, KeyError):
            continue

    dated.sort(key=lambda x: x[0])
    if len(dated) < 10:
        return None

    onset_dt: datetime | None = None
    for i in range(10, len(dated)):
        current_dt, current_price = dated[i]
        cutoff = current_dt - timedelta(days=_Z_SCORE_WINDOW)
        window_prices = [p for dt, p in dated[:i] if dt >= cutoff]
        if len(window_prices) < 5:
            continue
        mean_w = statistics.mean(window_prices)
        try:
            std_w = statistics.stdev(window_prices)
        except statistics.StatisticsError:
            continue
        if std_w == 0.0:
            continue
        z = (current_price - mean_w) / std_w
        if z <= z_onset_threshold:
            onset_dt = current_dt
            break

    if onset_dt is None:
        return None

    pre_start = onset_dt - timedelta(days=30)
    pre_prices = [p for dt, p in dated if pre_start <= dt < onset_dt]
    if len(pre_prices) < 5:
        return None
    return statistics.mean(pre_prices)


def _consecutive_days_below(history: list[dict], threshold_price: float) -> int:
    """
    Count consecutive calendar days (newest-first) where the daily closing
    price is strictly below threshold_price.  Stops at the first day where
    the price is at or above the threshold.
    """
    by_date: dict[str, float] = {}
    for h in history:
        try:
            dt = _parse_ts(h)
            if h.get("price") and h["price"] > 0:
                by_date[dt.strftime("%Y-%m-%d")] = float(h["price"])
        except (ValueError, TypeError, KeyError):
            continue

    if not by_date:
        return 0

    count = 0
    for date_str in sorted(by_date.keys(), reverse=True):
        if by_date[date_str] < threshold_price:
            count += 1
        else:
            break
    return count


def _detect_super_deal(
    container_id: str,
    container_name: str,
    history: list[dict],
    pd_info: dict,
    baseline: float,
    net_cagr: float,
) -> dict | None:
    """
    Delegate all 7 PV-15 guard filters to SuperDealDomainService.
    Pre-computes intermediate values from raw history, then wraps them in
    domain types (Amount, ROI) and calls evaluate().  Returns a backward-
    compatible dict on full pass, None otherwise.
    """
    current_price = pd_info.get("current_price")
    if not current_price or current_price <= 0 or baseline <= 0:
        return None

    # Pre-compute intermediates that require engine helpers / raw history
    z = _compute_zscore(history, _Z_SCORE_WINDOW)
    if z is None:
        return None

    # Quick pre-screen with specs — fast rejection before expensive stats computation
    _pre_screen = ZScoreBelow(_SUPER_DEAL_ZSCORE) & PriceWithinRange(
        Amount(1), Amount(baseline * _SUPER_DEAL_RATIO)
    )
    if not _pre_screen.is_satisfied_by({"z_score": z, "current_price": current_price}):
        return None

    weekly_vol = float(pd_info.get("quantity") or 0)
    vols_30d = _volumes_in_window(history, 30)
    avg_vol_30d = statistics.mean(vols_30d) if vols_30d else 0.0
    vol_spike = (weekly_vol / avg_vol_30d) if avg_vol_30d > 0 else 0.0

    earliest = _earliest_date(history)
    if earliest is None:
        return None
    history_days = (datetime.now(UTC).replace(tzinfo=None) - earliest).days

    low_threshold = baseline * _SUPER_DEAL_RATIO
    days_at_low = _consecutive_days_below(history, low_threshold)

    pre_crash = _pre_crash_mean(history, _SUPER_DEAL_Z_ONSET)
    if pre_crash is None or pre_crash <= 0:
        return None

    # Delegate 7-filter AND-logic to the pure domain service
    decision = SuperDealDomainService().evaluate(
        current_price=Amount(current_price),
        baseline_price=Amount(baseline),
        z_score=z,
        vol_spike=vol_spike,
        history_days=history_days,
        net_cagr=ROI(net_cagr),
        consecutive_days_at_low=days_at_low,
        pre_crash_mean=Amount(pre_crash),
        steam_fee_multiplier=settings.steam_fee_divisor,
        steam_fee_fixed=Amount(settings.steam_fee_fixed),
    )

    if not decision.is_super_deal:
        return None

    result = {
        "name": container_name,
        "container_id": container_id,
        "verdict": "ULTRA BUY",
        "is_super_deal": True,
        "buy_price": int(current_price),
        "target_exit_price": decision.target_exit_price.amount,
        "stop_loss_price": decision.stop_loss_price.amount,
        "mandatory_exit_days": 60,
        "z_score": round(z, 2),
        "expected_margin_pct": round(decision.expected_margin.value * 100, 1),
        "days_at_low": days_at_low,
    }

    from infra.signal_handler import notify_super_deal
    notify_super_deal(
        SuperDealDetected(
            timestamp=datetime.now(UTC).replace(tzinfo=None),
            item_name=container_name,
            payload=result,
        )
    )
    return result


def _earliest_date(history: list[dict]) -> datetime | None:
    """Return the earliest datetime in a history list."""
    earliest: datetime | None = None
    for h in history:
        try:
            dt = _parse_ts(h)
            if earliest is None or dt < earliest:
                earliest = dt
        except (ValueError, TypeError, KeyError):
            continue
    return earliest


def validate_super_deal_candidate(
    container_name: str,
    container_id: str,
    current_price: float,
    volume: int,
    history: list[dict],
) -> dict | None:
    """
    Public entry point for on-demand super-deal validation (PV-19).

    Derives baseline and net_cagr from history, then delegates to
    _detect_super_deal with the caller-supplied fresh price.  Returns the
    super-deal result dict on success, None when criteria are not met or
    when there is insufficient data to compute intermediates.

    Parameters
    ----------
    container_name: market_hash_name (for logging and result dict).
    container_id:   DimContainer UUID string (for result dict).
    current_price:  Fresh JIT price just fetched from Steam.
    volume:         Current weekly volume from the same API response.
    history:        Price history rows in portfolio_advisor format
                    [{timestamp, price, volume_7d}, ...] ordered ASC.
    """
    prices_30d = _prices_in_window(history, 30)
    if not prices_30d:
        return None

    baseline = statistics.mean(prices_30d)
    _, net_cagr, _ = _compute_cagr_metrics(history)
    pd_info = {"current_price": current_price, "quantity": volume}
    return _detect_super_deal(
        container_id, container_name, history, pd_info, baseline, net_cagr
    )


def allocate_portfolio(
    balance: float,
    inventory_items: list[dict],  # [{market_hash_name, count, ...}]
    containers: list,  # DimContainer ORM objects
    price_data: dict[str, dict],  # {name: {current_price, mean_price, quantity, lowest_price}}
    trade_advice: dict[str, dict],  # {container_id: compute_trade_advice result}
    price_history: dict[str, list[dict]],  # {container_id: [{timestamp, price, ...}]}
    invest_signals: dict[str, dict],  # {container_id: compute_investment_signal result}
    positions_map: dict[str, datetime | None] | None = None,  # {container_name: buy_date} for F-03
    order_book_data: dict[str, dict] | None = None,  # {container_id: order_book dict} for WALL-1
) -> dict:
    """
    Main portfolio allocation.

    Returns a dict with keys:
        sell            list of sell candidates (from inventory)
        flip            best flip candidate dict (or None)
        invest          best invest candidate dict (or None)
        reserve_amount  float
        flip_budget     float (40 %)
        invest_budget   float (40 %)
        total_balance   float
        top_flips       list[dict]   top 5 flip candidates for context
        top_invests     list[dict]   top 5 invest candidates for context
    """
    # Inventory lookup
    inv_map: dict[str, int] = {}
    for item in inventory_items:
        name = item.get("market_hash_name", "")
        inv_map[name] = inv_map.get(name, 0) + item.get("count", 1)

    # Total capital = cash + inventory market value (40/40/20 allocation base)
    inventory_value = sum(
        price_data.get(name, {}).get("current_price", 0) * qty for name, qty in inv_map.items()
    )
    total_capital = balance + inventory_value

    flip_budget = total_capital * 0.40
    invest_budget = total_capital * 0.40
    reserve_amount = total_capital * 0.20

    name_to_id = {str(c.container_name): str(c.container_id) for c in containers}

    now = datetime.now(UTC).replace(tzinfo=None)

    # ── Step 0: SELL candidates ───────────────────────────────────────────────
    sell_candidates: list[dict] = []
    for name, qty in inv_map.items():
        cid = name_to_id.get(name)
        if not cid:
            continue
        # F-03: skip if bought < 7 days ago (Steam trade ban)
        if positions_map is not None:
            buy_date = positions_map.get(name)
            if buy_date is not None and (now - buy_date) < timedelta(hours=168):
                logger.debug(
                    "F-03: %s bought %.1f hours ago — skipping (trade ban)",
                    name,
                    (now - buy_date).total_seconds() / 3600,
                )
                continue
        verdict = invest_signals.get(cid, {}).get("verdict", "")
        if verdict not in ("SELL", "LEAN SELL"):
            continue
        adv = trade_advice.get(cid, {})
        sell_t = adv.get("sell_target", 0)
        if not sell_t:
            continue
        net_each = round(_net(sell_t))
        net_total = round(net_each * qty)
        sell_candidates.append(
            {
                "name": name,
                "container_id": cid,
                "qty": qty,
                "sell_target": int(sell_t),
                "net_each": int(net_each),
                "net_total": int(net_total),
                "verdict": verdict,
            }
        )
    sell_candidates.sort(key=lambda x: x["net_total"], reverse=True)

    # ── Step 1: FLIP candidates ───────────────────────────────────────────────
    flip_candidates: list[dict] = []
    for c in containers:
        if str(c.container_name) in _ARMORY_POOL:
            continue  # managed via Armory Pass — excluded from flip pool
        adv = trade_advice.get(str(c.container_id), {})
        buy_t = adv.get("buy_target", 0)
        sell_t = adv.get("sell_target", 0)
        if not buy_t or not sell_t or buy_t <= 0:
            continue

        net_unit = _net(sell_t) - buy_t
        if net_unit <= 0:
            continue

        planned_qty = max(1, int(flip_budget // buy_t))
        pd_info = price_data.get(str(c.container_name), {})

        # FLIP-R2: recency guard — if current price is below the 20th-percentile buy
        # target, the 90-day history window no longer reflects current market conditions.
        current_price_now = pd_info.get("current_price") or 0
        if current_price_now < buy_t:
            continue  # stale history — current price below historical buy target
        weekly_vol = pd_info.get("quantity", 0) or 0
        _history_for_lc = price_history.get(str(c.container_id), [])
        prices_30d = _prices_in_window(_history_for_lc, 30)
        vol_30d = _volatility(prices_30d)
        if vol_30d is None:
            continue  # insufficient price history for flip assessment

        # Lifecycle gate — skip NEW and LEGACY stages for flip
        _lc_all_prices = [float(h["price"]) for h in _history_for_lc if h.get("price")]
        _lc_first_date = _earliest_date(_history_for_lc)
        if _lc_first_date is not None and _lc_all_prices:
            _lc_vol_30d_avg = float(weekly_vol) / 30 if weekly_vol else 0.0
            _lc_stage = classify_lifecycle(
                first_seen_date=_lc_first_date.date(),
                current_date=datetime.now(UTC).replace(tzinfo=None).date(),
                prices_30d=prices_30d,
                vol_7d=float(weekly_vol) / 7 if weekly_vol else 0.0,
                vol_30d_avg=_lc_vol_30d_avg,
                all_time_prices=_lc_all_prices,
            )
            if not is_flip_eligible(_lc_stage):
                continue  # NEW (speculative), LEGACY (invest-only), or DEAD

        # Float: avg daily volume (proxy for liquidity)
        avg_daily_vol = weekly_vol / 7 if weekly_vol else 0.0

        # Bid-ask spread (if lowest_price available from DB)
        median_p = pd_info.get("current_price") or buy_t
        lowest_p = pd_info.get("lowest_price")
        spread_pct = 0.0
        if lowest_p and median_p > 0 and lowest_p < median_p:
            spread_pct = (median_p - lowest_p) / median_p

        if vol_30d < _VOLATILITY_MIN_FLIP or vol_30d > _VOLATILITY_MAX_FLIP:
            continue  # outside flip range: dead asset (<5%) or too chaotic (>30%)
        if avg_daily_vol < _LIQUIDITY_MIN_DAILY:
            continue  # absolute liquidity floor — less than 1000 sales/day → skip
        if avg_daily_vol * 7 < planned_qty * 2:
            continue  # market cannot absorb position in 7 days with 2x safety factor
        if spread_pct > _SPREAD_MAX_FLIP:
            continue  # bid-ask spread too wide

        # WALL-1: order book filter — exclude if sell wall is too deep to exit in time
        cid_str = str(c.container_id)
        _ob = (order_book_data or {}).get(cid_str, {})
        _wall_metrics: dict = {}
        _best_buy_order: float = 0.0
        if _ob:
            _wall_metrics = compute_wall_metrics(
                sell_order_graph=_ob.get("sell_order_graph", []),
                current_price=float(current_price_now),
                target_price=float(sell_t),
                avg_daily_vol=avg_daily_vol,
                vol_30d=vol_30d,
            )
            _best_buy_order = get_best_buy_order(_ob.get("buy_order_graph", []))
            if not _wall_metrics.get("passes_wall_filter", True):
                logger.debug(
                    "WALL-1: %s excluded — estimated_days=%.1f > wall_max_days=%d",
                    c.container_name,
                    _wall_metrics.get("estimated_days", 0),
                    settings.wall_max_days,
                )
                continue

        unit_margin_pct = net_unit / buy_t
        if unit_margin_pct < settings.flip_min_net_margin:
            continue  # net margin below minimum threshold after Steam fee

        actual_qty = min(planned_qty, max(1, weekly_vol))
        budget_used = actual_qty * buy_t

        # Time-adjusted weekly ROI — Steam 7-day trade ban is the minimum hold period.
        # queue_days: expected time to sell `actual_qty` units at avg daily market volume.
        # effective_hold_days = 7 (trade ban) + queue_days (sell queue).
        # weekly_roi normalises unit_margin_pct to a 7-day base, enabling fair comparison
        # across containers with different liquidity (avoids vol_factor distortion).
        queue_days = actual_qty / avg_daily_vol if avg_daily_vol > 0 else 999.0
        effective_hold_days = 7.0 + queue_days
        # Spread factor: tighter spread = better (1.0 = no spread penalty)
        spread_factor = 1.0 - spread_pct
        weekly_roi = unit_margin_pct / (effective_hold_days / 7.0)
        flip_score = weekly_roi * (1.0 - vol_30d) * spread_factor

        if flip_score <= 0:
            continue

        flip_candidates.append(
            {
                "name": str(c.container_name),
                "container_id": str(c.container_id),
                "qty": actual_qty,
                "buy_price": int(buy_t),
                "sell_price": int(sell_t),
                "net_per_unit": round(net_unit),
                "expected_net_total": round(net_unit * actual_qty),
                "flip_score": round(flip_score, 4),
                "weekly_volume": weekly_vol,
                "avg_daily_vol": round(avg_daily_vol, 1),
                "volatility_pct": round(vol_30d * 100, 1),
                "spread_pct": round(spread_pct * 100, 1),
                "budget_used": round(budget_used),
                "net_margin_pct": adv.get("net_margin_pct", 0),
                "effective_hold_days": round(effective_hold_days, 1),
                # WALL-1 order book metrics (0/0.0 when order_book_data not provided)
                "volume_to_target": _wall_metrics.get("volume_to_target", 0),
                "estimated_days": _wall_metrics.get("estimated_days", 0.0),
                "best_buy_order": int(_best_buy_order),
            }
        )

    flip_candidates.sort(key=lambda x: x["flip_score"], reverse=True)
    best_flip = flip_candidates[0] if flip_candidates else None

    # ── Step 2: INVEST candidates (Net CAGR) ─────────────────────────────────
    invest_candidates: list[dict] = []
    for c in containers:
        if str(c.container_name) in _ARMORY_POOL:
            continue  # managed via Armory Pass — excluded from invest pool
        history = price_history.get(str(c.container_id), [])

        # Lifecycle gate — only AGING and LEGACY for invest
        _lc_all_prices_inv = [float(h["price"]) for h in history if h.get("price")]
        _lc_first_date_inv = _earliest_date(history)
        if _lc_first_date_inv is not None and _lc_all_prices_inv:
            _lc_prices_30d_inv = _prices_in_window(history, 30)
            _lc_pd_info_inv = price_data.get(str(c.container_name), {})
            _lc_weekly_vol_inv = float(_lc_pd_info_inv.get("quantity") or 0)
            _lc_stage_inv = classify_lifecycle(
                first_seen_date=_lc_first_date_inv.date(),
                current_date=datetime.now(UTC).replace(tzinfo=None).date(),
                prices_30d=_lc_prices_30d_inv,
                vol_7d=_lc_weekly_vol_inv / 7 if _lc_weekly_vol_inv else 0.0,
                vol_30d_avg=_lc_weekly_vol_inv / 30 if _lc_weekly_vol_inv else 0.0,
                all_time_prices=_lc_all_prices_inv,
            )
            if not is_invest_eligible(_lc_stage_inv):
                continue  # NEW (too young), ACTIVE (flip candidate), or DEAD

        gross_cagr, net_cagr, years = _compute_cagr_metrics(history)

        # Require ≥ 1 year of history and positive net annual return after Steam fee
        if years < 1.0 or net_cagr < _MIN_NET_CAGR:
            continue

        pd_info = price_data.get(str(c.container_name), {})
        current_price = pd_info.get("current_price")
        if not current_price:
            continue

        prices_180d = _prices_in_window(history, 180)
        vol_180d = _volatility(prices_180d)
        if vol_180d is None:
            continue  # insufficient recent price data for invest volatility

        # invest_score ranked on NET CAGR — reflects actual after-fee return
        invest_score = net_cagr * (1.0 - vol_180d)

        adv = trade_advice.get(str(c.container_id), {})
        buy_t = adv.get("buy_target") or current_price
        if not buy_t or buy_t <= 0:
            continue

        planned_qty = max(1, int(invest_budget // buy_t))
        history_years = round(years, 1)

        invest_candidates.append(
            {
                "name": str(c.container_name),
                "container_id": str(c.container_id),
                "qty": planned_qty,
                "buy_price": int(buy_t),
                "cagr_pct": round(gross_cagr * 100, 1),
                "cagr_net_pct": round(net_cagr * 100, 1),
                "volatility_pct": round(vol_180d * 100, 1),
                "invest_score": round(invest_score, 4),
                "budget_used": round(planned_qty * buy_t),
                "history_years": history_years,
            }
        )

    invest_candidates.sort(key=lambda x: x["invest_score"], reverse=True)
    best_invest = invest_candidates[0] if invest_candidates else None

    # ── Step 3: SUPER DEAL scan (PV-15) ──────────────────────────────────────
    # Scans all containers for anomalous price dips that justify deploying 50 %
    # of the reserve (= 10 % of total capital).  Does NOT touch the 40/40 split.
    super_deal_candidates: list[dict] = []
    for c in containers:
        cid = str(c.container_id)
        sd_history = price_history.get(cid, [])
        sd_pd_info = price_data.get(str(c.container_name), {})
        sd_prices_30d = _prices_in_window(sd_history, 30)
        sd_baseline = statistics.mean(sd_prices_30d) if sd_prices_30d else 0.0
        _, sd_net_cagr, _ = _compute_cagr_metrics(sd_history)

        candidate = _detect_super_deal(
            container_id=cid,
            container_name=str(c.container_name),
            history=sd_history,
            pd_info=sd_pd_info,
            baseline=sd_baseline,
            net_cagr=sd_net_cagr,
        )
        if candidate is not None:
            super_deal_candidates.append(candidate)

    # Best = most extreme anomaly (lowest Z-score); allocate 50 % of reserve
    best_super_deal: dict | None = None
    super_deal_budget: float = 0.0
    if super_deal_candidates:
        best_super_deal = min(super_deal_candidates, key=lambda x: x["z_score"])
        super_deal_budget = round(reserve_amount * 0.50)
        best_super_deal["budget"] = super_deal_budget
        logger.info(
            "PV-15 Super Deal detected: %s  Z=%.2f  margin=%.1f%%  budget=%.0f",
            best_super_deal["name"],
            best_super_deal["z_score"],
            best_super_deal["expected_margin_pct"],
            super_deal_budget,
        )

    return {
        "sell": sell_candidates,
        "flip": best_flip,
        "invest": best_invest,
        "reserve_amount": round(reserve_amount),
        "flip_budget": round(flip_budget),
        "invest_budget": round(invest_budget),
        "total_balance": round(balance),
        "inventory_value": round(inventory_value),
        "total_capital": round(total_capital),
        "top_flips": flip_candidates[:5],
        "top_invests": invest_candidates[:5],
        "super_deal": best_super_deal,
    }
