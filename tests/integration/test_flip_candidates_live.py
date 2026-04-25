"""
Integration test — flip candidate pipeline against the LIVE database.

Run with:
    docker exec cs2-api-1 python -m pytest tests/integration/test_flip_candidates_live.py -v -s

Prints a full rejection breakdown and the top candidates so you can see
exactly why the flip section is empty or what made it through.
"""
from __future__ import annotations

import statistics
from datetime import UTC, datetime, timedelta

import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def db():
    import sys
    sys.path.insert(0, "/app/src")
    from src.domain.connection import SessionLocal
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture(scope="module")
def live_data(db):
    """Pull everything the flip pipeline needs in one shot."""
    from src.domain.models import DimContainer, FactContainerPrice
    from src.domain.portfolio import get_portfolio_data
    from sqlalchemy import func

    cutoff_90 = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=90)
    cutoff_30 = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)

    containers = (
        db.query(DimContainer)
        .filter(DimContainer.is_blacklisted == 0)
        .all()
    )

    # latest price per container
    latest_sub = (
        db.query(
            FactContainerPrice.container_id,
            func.max(FactContainerPrice.timestamp).label("ts"),
        )
        .group_by(FactContainerPrice.container_id)
        .subquery()
    )
    latest_rows = (
        db.query(FactContainerPrice)
        .join(
            latest_sub,
            (FactContainerPrice.container_id == latest_sub.c.container_id)
            & (FactContainerPrice.timestamp == latest_sub.c.ts),
        )
        .all()
    )
    price_map: dict[str, float] = {str(r.container_id): r.price for r in latest_rows}

    # 90-day history counts
    hist_counts = dict(
        db.query(
            FactContainerPrice.container_id,
            func.count(FactContainerPrice.id),
        )
        .filter(FactContainerPrice.timestamp >= cutoff_90)
        .group_by(FactContainerPrice.container_id)
        .all()
    )

    # 30-day prices for volatility
    hist_30d_rows = (
        db.query(FactContainerPrice.container_id, FactContainerPrice.price)
        .filter(FactContainerPrice.timestamp >= cutoff_30)
        .order_by(FactContainerPrice.container_id, FactContainerPrice.timestamp)
        .all()
    )
    hist_30d: dict[str, list[float]] = {}
    for cid, price in hist_30d_rows:
        hist_30d.setdefault(str(cid), []).append(price)

    portfolio_data = get_portfolio_data()

    return {
        "containers": containers,
        "price_map": price_map,
        "hist_counts": hist_counts,
        "hist_30d": hist_30d,
        "portfolio_data": portfolio_data,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────


def _volatility(prices: list[float]) -> float | None:
    if len(prices) < 5:
        return None
    mean_p = statistics.mean(prices)
    if mean_p == 0:
        return None
    return statistics.stdev(prices) / mean_p


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_flip_pipeline_breakdown(live_data):
    """
    Walk every non-blacklisted container through the flip gates and print
    a detailed rejection breakdown + top candidates.
    """
    from src.domain.analytics.armory_advisor import DEFAULT_REWARD_CATALOG as _ARMORY_POOL
    from src.domain.lifecycle import classify_lifecycle, is_flip_eligible
    from src.domain.trade_advisor import compute_trade_advice
    from config import settings

    _FEE_DIV   = settings.steam_fee_divisor
    _FEE_FIXED = settings.steam_fee_fixed
    _VOL_MIN   = 0.05   # _VOLATILITY_MIN_FLIP
    _VOL_MAX   = 0.30   # _VOLATILITY_MAX_FLIP
    _LIQ_MIN   = 1000   # _LIQUIDITY_MIN_DAILY

    def net(p):
        return p / _FEE_DIV - _FEE_FIXED

    containers    = live_data["containers"]
    price_map     = live_data["price_map"]
    hist_counts   = live_data["hist_counts"]
    hist_30d      = live_data["hist_30d"]
    portfolio_data = live_data["portfolio_data"]

    rejected: dict[str, int] = {}
    candidates: list[dict]   = []
    now = datetime.now(UTC).replace(tzinfo=None)

    for c in containers:
        cid  = str(c.container_id)
        name = str(c.container_name)

        # ── gate 1: armory pool ──────────────────────────────────────────────
        if name in _ARMORY_POOL:
            rejected["armory_pool"] = rejected.get("armory_pool", 0) + 1
            continue

        # ── gate 2: trade advice ─────────────────────────────────────────────
        h90 = hist_counts.get(c.container_id, 0)
        if h90 < 5:
            rejected["no_trade_advice_history"] = rejected.get("no_trade_advice_history", 0) + 1
            continue

        # build minimal history rows for trade advisor
        from src.domain.models import FactContainerPrice
        from src.domain.connection import SessionLocal
        from datetime import timedelta
        cutoff_90 = now - timedelta(days=90)
        with SessionLocal() as _db:
            rows_90 = (
                _db.query(FactContainerPrice)
                .filter(
                    FactContainerPrice.container_id == c.container_id,
                    FactContainerPrice.timestamp >= cutoff_90,
                )
                .order_by(FactContainerPrice.timestamp)
                .all()
            )
        hist_rows = [{"timestamp": str(r.timestamp), "price": r.price} for r in rows_90]

        adv    = compute_trade_advice(name, c.base_cost, str(c.container_type.value), hist_rows)
        buy_t  = adv["buy_target"]
        sell_t = adv["sell_target"]

        if not buy_t or not sell_t or buy_t <= 0:
            rejected["no_trade_advice"] = rejected.get("no_trade_advice", 0) + 1
            continue

        # ── gate 3: profitability ────────────────────────────────────────────
        curr         = price_map.get(cid, 0) or 0
        effective_buy = curr if curr > 0 else buy_t
        net_unit      = net(sell_t) - effective_buy
        margin_pct    = net_unit / effective_buy * 100 if effective_buy > 0 else 0

        if net_unit <= 0:
            rejected["no_profit"] = rejected.get("no_profit", 0) + 1
            continue

        # ── gate 4: volatility data ──────────────────────────────────────────
        prices_30 = hist_30d.get(cid, [])
        vol_30d   = _volatility(prices_30)
        if vol_30d is None:
            rejected["no_volatility_data"] = rejected.get("no_volatility_data", 0) + 1
            continue

        # ── gate 5: lifecycle (LC-1 behavioral) ──────────────────────────────
        if rows_90:
            all_prices_90 = [r.price for r in rows_90]
            all_vols_90   = [getattr(r, "volume_7d", 0) or 0 for r in rows_90]
            lc_phase, _   = classify_lifecycle(all_prices_90, all_vols_90, None)
            if lc_phase is None:
                rejected["lifecycle_no_data"] = rejected.get("lifecycle_no_data", 0) + 1
                continue
            if not is_flip_eligible(lc_phase):
                key = f"lifecycle_{lc_phase.value}"
                rejected[key] = rejected.get(key, 0) + 1
                continue

        # ── gate 6: volatility range ─────────────────────────────────────────
        if vol_30d < _VOL_MIN or vol_30d > _VOL_MAX:
            rejected["volatility_out_of_range"] = rejected.get("volatility_out_of_range", 0) + 1
            continue

        # ── gate 7: liquidity ────────────────────────────────────────────────
        pd_info       = portfolio_data.get(name, {}) or {}
        weekly_vol    = pd_info.get("quantity", 0) or 0
        avg_daily_vol = weekly_vol / 7 if weekly_vol else 0

        if avg_daily_vol < _LIQ_MIN:
            rejected["low_liquidity"] = rejected.get("low_liquidity", 0) + 1
            continue

        # ── passed all gates ─────────────────────────────────────────────────
        score = margin_pct * min(avg_daily_vol / 10000, 1.0) * (1 - vol_30d)
        candidates.append({
            "name":        name,
            "eff_buy":     effective_buy,
            "sell_t":      sell_t,
            "margin_pct":  round(margin_pct, 1),
            "vol_30d":     round(vol_30d, 3),
            "daily_vol":   round(avg_daily_vol),
            "score":       round(score, 2),
            "data_source": adv["data_source"],
        })

    # ── Print results ─────────────────────────────────────────────────────────
    total = len(containers)
    print(f"\n{'='*60}")
    print(f"FLIP PIPELINE — {total} containers checked")
    print(f"{'='*60}")
    print("\nREJECTION BREAKDOWN:")
    for reason, count in sorted(rejected.items(), key=lambda x: -x[1]):
        pct = count / total * 100
        print(f"  {reason:<30} {count:4d}  ({pct:.0f}%)")

    print(f"\nCANDIDATES PASSED: {len(candidates)}")
    if candidates:
        candidates.sort(key=lambda x: -x["score"])
        print("\nTOP CANDIDATES:")
        print(f"  {'Name':<45} {'eff_buy':>8} {'sell_t':>8} {'margin':>8} {'vol30d':>7} {'daily_vol':>10} {'score':>7}")
        print(f"  {'-'*100}")
        for c in candidates[:10]:
            print(
                f"  {c['name'][:45]:<45} {c['eff_buy']:>8.0f} {c['sell_t']:>8.0f}"
                f" {c['margin_pct']:>+7.1f}% {c['vol_30d']:>7.3f} {c['daily_vol']:>10} {c['score']:>7.2f}"
            )
    else:
        print("\n  *** NO CANDIDATES — checking near-misses ***")
        # re-run without liquidity gate to see what would pass
        near = []
        for c in containers:
            cid  = str(c.container_id)
            name = str(c.container_name)
            if name in _ARMORY_POOL:
                continue
            curr = price_map.get(cid, 0) or 0
            if not curr:
                continue
            prices_30 = hist_30d.get(cid, [])
            if len(prices_30) < 5:
                continue
            # rough buy/sell from percentiles
            s_prices = sorted(prices_30)
            k20 = int(len(s_prices) * 0.20)
            k90 = int(len(s_prices) * 0.90)
            buy_est  = s_prices[k20]
            sell_est = s_prices[k90]
            net_est  = net(sell_est) - curr
            if net_est > 0:
                vol = _volatility(s_prices)
                pd_info = portfolio_data.get(name, {}) or {}
                wvol = pd_info.get("quantity", 0) or 0
                near.append((name, curr, sell_est, round(net_est/curr*100, 1),
                              round(vol or 0, 3), round(wvol/7)))

        near.sort(key=lambda x: -x[3])
        print(f"\n  {'Name':<45} {'curr':>8} {'sell':>8} {'net%':>7} {'vol':>7} {'d_vol':>8}")
        for row in near[:15]:
            print(f"  {row[0][:45]:<45} {row[1]:>8.0f} {row[2]:>8.0f} {row[3]:>+6.1f}% {row[4]:>7.3f} {row[5]:>8}")

    # test always passes — it's diagnostic
    assert True
