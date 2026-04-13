from __future__ import annotations

from dataclasses import dataclass

from domain.value_objects import Amount, ROI

# ─── Smart Buy Price ───────────────────────────────────────────────────────────


def compute_smart_buy_price(
    sell_price: Amount,
    *,
    fee_divisor: float,
    fee_fixed: Amount,
    min_margin: float,
) -> Amount:
    """
    Maximum price you can pay and still achieve `min_margin` net profit
    when selling at `sell_price` after Steam fees.

    Formula:
        net_proceeds    = sell_price / fee_divisor − fee_fixed
        smart_buy_price = net_proceeds / (1 + min_margin)

    Returns Amount(0) when net_proceeds <= 0 (fixed fee dominates the sale value).

    Parameters
    ----------
    sell_price:
        Expected exit price (typically the 30-day mean / pre-crash mean).
    fee_divisor:
        Steam fee divisor, e.g. 1.15 for 15 % fee.
    fee_fixed:
        Steam fixed per-transaction fee, e.g. Amount(5).
    min_margin:
        Minimum acceptable net margin as a ratio, e.g. 0.05 for 5 %.
    """
    net = sell_price.amount / fee_divisor - fee_fixed.amount
    if net <= 0:
        return Amount(0)
    return Amount(net / (1 + min_margin))

# ─── Investment Domain ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LiquidityDecision:
    """Result of InvestmentDomainService.evaluate_investment()."""

    is_liquid: bool
    reason: str | None  # populated only when is_liquid=False


class InvestmentDomainService:
    """
    Pure domain logic for investment signal validation.

    Liquidity Guard: a BUY signal is invalid when the market cannot absorb
    the intended purchase within a reasonable time frame.

    Guard condition: volume_24h < min_liquidity_ratio
    where min_liquidity_ratio is the minimum required daily volume (units).

    The caller is responsible for computing min_liquidity_ratio from the
    intended position size and a safety multiplier:
        min_liquidity_ratio = (position_budget / price.amount) * safety_factor
    """

    def evaluate_investment(
        self,
        *,
        price: Amount,
        volume_24h: int,
        avg_volume_7d: float,
        min_liquidity_ratio: float,
    ) -> LiquidityDecision:
        """
        Returns LiquidityDecision(is_liquid=False) when the market is too thin
        to execute the intended position without significant price impact.

        Parameters
        ----------
        price:
            Current market price per unit (used for position-value context).
        volume_24h:
            Number of units traded in the last 24 hours.
        avg_volume_7d:
            7-day rolling average of daily traded units (for context / future guards).
        min_liquidity_ratio:
            Minimum required daily volume (in units). Guard fires when
            volume_24h < min_liquidity_ratio.
        """
        if volume_24h < min_liquidity_ratio:
            return LiquidityDecision(
                is_liquid=False,
                reason=(
                    f"Insufficient liquidity: volume_24h={volume_24h} < "
                    f"required={min_liquidity_ratio:.1f} units/day "
                    f"(price={price}, avg_7d={avg_volume_7d:.1f})"
                ),
            )
        return LiquidityDecision(is_liquid=True, reason=None)


@dataclass(frozen=True)
class SuperDealDecision:
    is_super_deal: bool
    reason: str | None
    target_exit_price: Amount | None
    stop_loss_price: Amount | None
    expected_margin: ROI | None


class SuperDealDomainService:
    """Pure domain logic for Super Deal detection. No DB, no config, no I/O."""

    _Z_SCORE_THRESHOLD = -3.0
    _VOL_SPIKE_MAX = 2.0
    _MIN_HISTORY_DAYS = 90
    _MIN_NET_CAGR = 0.01
    _MAX_CONSECUTIVE_DAYS = 7
    _MIN_MARGIN = 0.20
    _PRICE_DISCOUNT = 0.70
    _STOP_LOSS_FACTOR = 0.85

    def evaluate(
        self,
        *,
        current_price: Amount,
        baseline_price: Amount,
        z_score: float,
        vol_spike: float,
        history_days: int,
        net_cagr: ROI,
        consecutive_days_at_low: int,
        pre_crash_mean: Amount,
        steam_fee_multiplier: float,
        steam_fee_fixed: Amount,
    ) -> SuperDealDecision:
        # Filter 1: deep discount vs baseline
        if not (current_price < baseline_price * self._PRICE_DISCOUNT):
            return SuperDealDecision(
                is_super_deal=False,
                reason=f"price not deep enough: {current_price} >= {baseline_price * self._PRICE_DISCOUNT}",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=None,
            )

        # Filter 2: statistically oversold
        if not (z_score <= self._Z_SCORE_THRESHOLD):
            return SuperDealDecision(
                is_super_deal=False,
                reason=f"z_score not low enough: {z_score} > {self._Z_SCORE_THRESHOLD}",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=None,
            )

        # Filter 3: no volume panic spike
        if not (vol_spike <= self._VOL_SPIKE_MAX):
            return SuperDealDecision(
                is_super_deal=False,
                reason=f"vol_spike too high: {vol_spike} > {self._VOL_SPIKE_MAX}",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=None,
            )

        # Filter 4: enough price history
        if not (history_days >= self._MIN_HISTORY_DAYS):
            return SuperDealDecision(
                is_super_deal=False,
                reason=f"insufficient history: {history_days} < {self._MIN_HISTORY_DAYS} days",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=None,
            )

        # Filter 5: positive net CAGR
        if not (net_cagr.value >= self._MIN_NET_CAGR):
            return SuperDealDecision(
                is_super_deal=False,
                reason=f"net_cagr too low: {net_cagr.to_percent_str()} < {self._MIN_NET_CAGR * 100:.1f}%",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=None,
            )

        # Filter 6: not in prolonged freefall
        if not (consecutive_days_at_low < self._MAX_CONSECUTIVE_DAYS):
            return SuperDealDecision(
                is_super_deal=False,
                reason=f"consecutive_days_at_low too high: {consecutive_days_at_low} >= {self._MAX_CONSECUTIVE_DAYS}",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=None,
            )

        # Filter 7: Net Proceeds Guard — reject if selling at pre_crash mean yields no profit after fees
        net_exit = pre_crash_mean.amount / steam_fee_multiplier - steam_fee_fixed.amount
        if net_exit <= current_price.amount:
            return SuperDealDecision(
                is_super_deal=False,
                reason="Negative or zero net margin after fees",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=None,
            )

        # Filter 8: expected net margin meets minimum threshold
        margin = (net_exit / current_price.amount) - 1.0
        expected_margin = ROI(margin)

        if not (margin >= self._MIN_MARGIN):
            return SuperDealDecision(
                is_super_deal=False,
                reason=f"expected margin too low: {expected_margin.to_percent_str()} < {self._MIN_MARGIN * 100:.0f}%",
                target_exit_price=None,
                stop_loss_price=None,
                expected_margin=expected_margin,
            )

        return SuperDealDecision(
            is_super_deal=True,
            reason=None,
            target_exit_price=pre_crash_mean * (1.0 / steam_fee_multiplier),
            stop_loss_price=current_price * self._STOP_LOSS_FACTOR,
            expected_margin=expected_margin,
        )
