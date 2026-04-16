from __future__ import annotations

import functools
from dataclasses import dataclass


@functools.total_ordering
@dataclass(frozen=True)
class Amount:
    """Monetary amount in whole integer units (e.g. kopecks, cents)."""

    amount: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "amount", round(self.amount))

    def __add__(self, other: Amount) -> Amount:
        if not isinstance(other, Amount):
            return NotImplemented
        return Amount(self.amount + other.amount)

    def __sub__(self, other: Amount) -> Amount:
        if not isinstance(other, Amount):
            return NotImplemented
        return Amount(self.amount - other.amount)

    def __mul__(self, factor: int | float) -> Amount:
        if not isinstance(factor, (int, float)):
            return NotImplemented
        return Amount(round(self.amount * factor))

    def __rmul__(self, factor: int | float) -> Amount:
        return self.__mul__(factor)

    def __truediv__(self, divisor: int | float) -> Amount:
        if not isinstance(divisor, (int, float)):
            return NotImplemented
        return Amount(round(self.amount / divisor))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Amount):
            return NotImplemented
        return self.amount == other.amount

    def __lt__(self, other: Amount) -> bool:
        if not isinstance(other, Amount):
            return NotImplemented
        return self.amount < other.amount

    def __hash__(self) -> int:
        return hash(self.amount)

    def __str__(self) -> str:
        return f"{self.amount:,}"


@functools.total_ordering
@dataclass(frozen=True)
class ROI:
    value: float

    def to_percent_str(self) -> str:
        return f"{self.value * 100:.2f}%"

    def __mul__(self, factor: int | float) -> ROI:
        if not isinstance(factor, (int, float)):
            return NotImplemented
        return ROI(self.value * factor)

    def __rmul__(self, factor: int | float) -> ROI:
        return self.__mul__(factor)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ROI):
            return NotImplemented
        return self.value == other.value

    def __lt__(self, other: ROI) -> bool:
        if not isinstance(other, ROI):
            return NotImplemented
        return self.value < other.value

    def __hash__(self) -> int:
        return hash(self.value)


@functools.total_ordering
@dataclass(frozen=True)
class Percentage:
    value: float

    def __post_init__(self) -> None:
        if not (0.0 <= self.value <= 1.0):
            raise ValueError(f"Percentage.value must be in [0.0, 1.0], got {self.value}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Percentage):
            return NotImplemented
        return self.value == other.value

    def __lt__(self, other: Percentage) -> bool:
        if not isinstance(other, Percentage):
            return NotImplemented
        return self.value < other.value

    def __hash__(self) -> int:
        return hash(self.value)
