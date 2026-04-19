"""Steam Market request pacing — human-like delays with heavy tail."""

from __future__ import annotations

import random


def human_delay(
    short: tuple[float, float] = (8.0, 18.0),
    medium: tuple[float, float] = (25.0, 55.0),
    long: tuple[float, float] = (70.0, 130.0),
    medium_prob: float = 0.12,
    long_prob: float = 0.04,
) -> float:
    """
    Human-like inter-request delay with a heavy tail.

      ~84% — short  (8–18 s)   typical market browsing pace
      ~12% — medium (25–55 s)  user reads listing / switches tabs
       ~4% — long   (70–130 s) user briefly walks away

    Uniform distributions are a bot fingerprint — real humans have variance.
    """
    r = random.random()
    if r < long_prob:
        return random.uniform(*long)
    if r < long_prob + medium_prob:
        return random.uniform(*medium)
    return random.uniform(*short)


def request_delay() -> float:
    """Delay for fetch_nameid / fetch_order_book (slightly tighter range)."""
    return human_delay(
        short=(6.5, 14.0),
        medium=(18.0, 40.0),
        long=(55.0, 110.0),
        medium_prob=0.10,
        long_prob=0.03,
    )
