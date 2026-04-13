"""
Tests for ingestion/steam/ — _parse_steam_price() and normalize_market_hash_name().

fetch_history / fetch_price_overview require live HTTP + Steam cookie,
so we test only the pure parsing logic here.
"""

from __future__ import annotations

import pytest

from ingestion.steam.formatter import normalize_market_hash_name
from ingestion.steam.parser import _parse_steam_price


class TestParseSteamPriceEmpty:
    def test_empty_string_returns_zero(self) -> None:
        assert _parse_steam_price("") == 0.0

    def test_none_like_empty_returns_zero(self) -> None:
        assert _parse_steam_price("   ") == 0.0

    def test_pure_currency_symbol_returns_zero(self) -> None:
        assert _parse_steam_price("$") == 0.0


class TestParseSteamPriceUSD:
    def test_dollar_prefix(self) -> None:
        assert _parse_steam_price("$1.25") == pytest.approx(1.25)

    def test_no_prefix(self) -> None:
        assert _parse_steam_price("1.25") == pytest.approx(1.25)

    def test_large_us_format(self) -> None:
        # 1,234.56  → comma = thousands, dot = decimal
        assert _parse_steam_price("1,234.56") == pytest.approx(1234.56)

    def test_dollar_large(self) -> None:
        assert _parse_steam_price("$10.00") == pytest.approx(10.00)

    def test_integer_value(self) -> None:
        assert _parse_steam_price("5") == pytest.approx(5.0)


class TestParseSteamPriceEuropean:
    def test_euro_decimal_comma(self) -> None:
        # European: 1.234,56 → dot = thousands, comma = decimal
        assert _parse_steam_price("1.234,56") == pytest.approx(1234.56)

    def test_simple_decimal_comma(self) -> None:
        # "1,25" — one comma, 2 digits after → decimal comma
        assert _parse_steam_price("1,25") == pytest.approx(1.25)

    def test_euro_suffix_stripped(self) -> None:
        assert _parse_steam_price("1,25 €") == pytest.approx(1.25)


class TestParseSteamPriceKZT:
    def test_kzt_space_thousands(self) -> None:
        # "1 234,56 ₸" — spaces are stripped (non-numeric), comma = decimal
        assert _parse_steam_price("1 234,56 ₸") == pytest.approx(1234.56)

    def test_kzt_simple(self) -> None:
        assert _parse_steam_price("500 ₸") == pytest.approx(500.0)

    def test_kzt_no_decimals(self) -> None:
        assert _parse_steam_price("1000 ₸") == pytest.approx(1000.0)


class TestParseSteamPriceEdgeCases:
    def test_only_letters_returns_zero(self) -> None:
        assert _parse_steam_price("N/A") == 0.0

    def test_multiple_commas_treated_as_thousands(self) -> None:
        # "1,234,567" — multiple commas → all stripped (thousands)
        assert _parse_steam_price("1,234,567") == pytest.approx(1234567.0)

    def test_zero_value(self) -> None:
        assert _parse_steam_price("0.00") == pytest.approx(0.0)

    def test_very_small_value(self) -> None:
        assert _parse_steam_price("$0.03") == pytest.approx(0.03)


class TestNormalizeMarketHashName:
    def test_genesis_terminal_gets_sealed_prefix(self) -> None:
        assert normalize_market_hash_name("Genesis Terminal") == "Sealed Genesis Terminal"

    def test_dead_hand_terminal_gets_sealed_prefix(self) -> None:
        assert normalize_market_hash_name("Dead Hand Terminal") == "Sealed Dead Hand Terminal"

    def test_already_sealed_terminal_is_idempotent(self) -> None:
        assert normalize_market_hash_name("Sealed Genesis Terminal") == "Sealed Genesis Terminal"

    def test_holo_foil_slash_replaced_with_dash(self) -> None:
        # (Holo/Foil) → (Holo-Foil): slash replaced for API URL; DB stores original
        assert normalize_market_hash_name("Cologne 2014 (Holo/Foil)") == "Cologne 2014 (Holo-Foil)"

    def test_holo_foil_dash_input_is_idempotent(self) -> None:
        # Already normalized — safe to call again
        assert normalize_market_hash_name("Cologne 2014 (Holo-Foil)") == "Cologne 2014 (Holo-Foil)"

    def test_holo_foil_mid_name_replaced(self) -> None:
        # Replacement works regardless of position in the string
        assert normalize_market_hash_name("Bravo (Holo/Foil) Pack") == "Bravo (Holo-Foil) Pack"

    def test_regular_case_unchanged(self) -> None:
        assert normalize_market_hash_name("Kilowatt Case") == "Kilowatt Case"

    def test_autograph_capsule_unchanged(self) -> None:
        name = "Paris 2023 Challengers Autograph Capsule"
        assert normalize_market_hash_name(name) == name

    def test_strips_whitespace(self) -> None:
        assert normalize_market_hash_name("  Kilowatt Case  ") == "Kilowatt Case"

    def test_terminal_whitespace_strip_then_sealed(self) -> None:
        assert normalize_market_hash_name("  Genesis Terminal  ") == "Sealed Genesis Terminal"
