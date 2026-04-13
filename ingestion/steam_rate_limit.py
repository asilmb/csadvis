"""Steam Market request pacing — mandatory jitter for human-like delays."""

from __future__ import annotations

import random


def request_delay() -> float:
    """Return a random delay in [6.5, 14.8] seconds — jitter for human-like pacing."""
    return random.uniform(6.5, 14.8)
