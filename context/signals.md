# Math & Signals (2026-04-04)

## Investment Signals (engine/investment.py)

Score Range: [-2, +2]. 2=BUY, 1=LEAN BUY, 0=HOLD, -1=LEAN SELL, -2=SELL.
- PriceRatio Buy: current / baseline < 0.85 (+1)
- PriceRatio Sell: current / baseline > 1.20 (-1) [Skip if current < ratio_floor]
- Momentum Buy: (current - mean_30d) / mean_30d < -0.05 (+1)
- Momentum Sell: (current - mean_30d) / mean_30d > +0.08 (or +0.15 if event-matched) (-1)
- SellAtLoss: net proceeds (sell / steam_fee_divisor - steam_fee_fixed) < user buy_price

**Filtering via Specification Pattern (DDD-3.3):**
- Before scoring, BUY signals pass through `VolumeAbove(settings.liquidity_min_volume)` spec
- Spec candidate: `{"volume": qty}` — missing key → False (conservative)
- Low-liquidity BUY/LEAN BUY → suppressed to HOLD + `liquidity_warning` key added to result

**Side-effects via EventBus (DDD-3.2):**
- Liquidity suppression → publishes `LiquidityWarning` event on `default_bus`
- SuperDeal detection → publishes `SuperDealDetected` event on `default_bus`
- `services/signal_handler.py` subscribes at startup → logs warnings/info
- Engine stays pure: no direct logging of signal decisions inside engine modules

## Signal Labels (services/cache_writer.py)

ratio_signal derived from price_ratio_pct:
- pct <= -10.0 → "CHEAP" | pct >= 10.0 → "EXPENSIVE" | else → "NEUTRAL"

momentum_signal derived from momentum_pct:
- pct >= 5.0 → "RISING" | pct <= -5.0 → "FALLING" | else → "STABLE"

trend_signal, event_signal, unrealized_pnl: always NULL (no pipeline support yet)

## Trade Advisor (engine/trade_advisor.py)

Baseline (Cases): max(base_cost - key_price, 25).
Baseline (Capsules): max(base_cost, 25).
Target Buy: 20th percentile 90d.
Target Sell: 70th percentile 90d.
Net Margin: (sell / steam_fee_divisor - steam_fee_fixed - buy) / buy.

## Smart Buy Price (domain/services.py — PV-11)

Formula: `net / (1 + min_margin)` where `net = sell / fee_divisor − fee_fixed`
- sell_price = 30-day mean (historical norm from get_portfolio_data)
- fee_divisor = settings.steam_fee_divisor (1.15)
- fee_fixed = Amount(settings.steam_fee_fixed) (Amount(5))
- min_margin = settings.flip_min_net_margin (0.05)
- Returns Amount(0) when net_proceeds ≤ 0 (fixed fee dominates low-price items)
- Exposed per container in get_portfolio_data() output

## Portfolio Constraints (engine/portfolio_advisor.py)

Flip: 40%. Filter: spread <= 25%, vol > 0, net_margin >= flip_min_net_margin (5%).
  Score: weekly_roi * (1 - vol_30d) * spread_factor
    where effective_hold_days = 7.0 + queue_days (7-day trade ban penalty)
          queue_days = actual_qty / avg_daily_vol (999.0 if vol = 0)
          weekly_roi = unit_margin_pct / (effective_hold_days / 7.0)

Invest: 40%. Filter: net_cagr >= _MIN_NET_CAGR (1%), history >= 1y.
  Score: net_cagr * (1 - vol_180d)  ← Net CAGR after Steam fee, NOT gross
    _compute_cagr_metrics(history) → (gross_cagr, net_cagr, years) — single source of truth

Reserve: 20% (static, not deployed automatically).

**Super Deal pre-screen (DDD-3.3 Specification Pattern):**
- `ZScoreBelow(-3.0) & PriceWithinRange(Amount(1), Amount(baseline * 0.70))`
- Applied before expensive SuperDealDomainService.evaluate() call
- Fast rejection without constructing full domain objects

Correlation Warning: |Pearson r| > 0.70 between selected flip/invest pair.

## Correlation (engine/correlation.py)

Method: Pearson r on log-returns. Min shared samples: 30.
Resampling: _resample_pair() builds dense daily grid over overlap range using ffill.
  - Days where BOTH series are pure ffill (no real DB entry) are excluded.
  - Guarantees every log-return represents exactly 1 calendar day.
Cache: in-process, 4h TTL, thread-safe lock.

## Cookie Expiry Detection (frontend/callbacks.py — PV-07/13)

sync_all callback checks: `wallet.error_code in {"NO_COOKIE", "STALE_COOKIE"}`
                    and: `transactions.error_code in {"NO_COOKIE", "STALE_COOKIE"}`
On auth error → `cookie-status-badge` in header shows red:
  "Steam Session Expired. Update auth cookie."
On clean sync → badge hidden (`display: none`).
