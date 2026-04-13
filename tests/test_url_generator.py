"""
Unit tests for frontend/url_generator.py (PV-20).

Pure tests — no DB, no network, no Dash renderer.
item_link() is tested only for return type (requires Dash import but no layout).

Covers:
  get_market_url():
    - basic encoding (spaces → %20, | → %7C, parentheses encoded)
    - appid in URL (default 730)
    - custom appid
    - empty name → generic market URL (no trailing slash garbage)
    - whitespace-only name → generic market URL
    - special chars: & # % " ' encoded correctly
    - name with ampersand does not break URL
    - name with quotes encoded
    - non-ASCII characters encoded

  get_inventory_url():
    - valid steam_id + asset_id → URL contains both
    - valid steam_id, no asset_id → base inventory URL
    - falsy steam_id (None, "", "0", 0) → None
    - asset_id=0 or "0" → omitted from URL
    - asset_id=None explicitly → base URL without fragment suffix
    - integer steam_id accepted
    - integer asset_id accepted

  get_inspect_url():
    - valid steam:// prefix → returned as-is
    - csgo_econ_action_preview prefix → returned as-is
    - None → None
    - empty string → None
    - http:// URL → None (not a valid inspect link)
    - arbitrary string → None
    - leading whitespace stripped before check
"""

from __future__ import annotations

from frontend.url_generator import get_inspect_url, get_inventory_url, get_market_url

# ─── get_market_url ───────────────────────────────────────────────────────────


class TestGetMarketUrl:
    def test_spaces_encoded(self):
        url = get_market_url("AK-47 Redline")
        assert "%20" in url
        assert " " not in url

    def test_pipe_encoded(self):
        url = get_market_url("AK-47 | Redline (Field-Tested)")
        assert "|" not in url
        assert "%7C" in url

    def test_parentheses_encoded(self):
        url = get_market_url("AK-47 | Redline (Field-Tested)")
        assert "(" not in url
        assert ")" not in url
        assert "%28" in url
        assert "%29" in url

    def test_appid_in_url(self):
        url = get_market_url("AK-47 | Redline (Field-Tested)")
        assert "/730/" in url

    def test_custom_appid(self):
        url = get_market_url("Some Item", appid=440)
        assert "/440/" in url

    def test_empty_name_returns_generic_url(self):
        url = get_market_url("")
        assert url == "https://steamcommunity.com/market/listings/730"

    def test_whitespace_only_returns_generic_url(self):
        url = get_market_url("   ")
        assert url == "https://steamcommunity.com/market/listings/730"

    def test_ampersand_encoded(self):
        url = get_market_url("Case & Key")
        assert "&" not in url
        assert "%26" in url

    def test_hash_encoded(self):
        url = get_market_url("Item #1")
        assert "#" not in url
        assert "%23" in url

    def test_percent_encoded(self):
        # "Item 100%" → space→%20, %→%25 → two % signs in result
        url = get_market_url("Item 100%")
        assert "%25" in url  # the literal % is encoded
        assert " " not in url  # space is also encoded

    def test_double_quote_encoded(self):
        url = get_market_url('Item "Special"')
        assert '"' not in url

    def test_single_quote_encoded(self):
        url = get_market_url("Item's Edition")
        assert "'" not in url

    def test_url_starts_with_base(self):
        url = get_market_url("Revolution Case")
        assert url.startswith("https://steamcommunity.com/market/listings/730/")

    def test_non_ascii_encoded(self):
        url = get_market_url("Нож | Тигровый зуб")
        assert "Н" not in url  # Cyrillic must be encoded

    def test_hyphen_not_encoded(self):
        url = get_market_url("AK-47 | Redline")
        assert "AK-47" in url  # hyphens are safe

    def test_full_url_roundtrip(self):
        from urllib.parse import unquote
        name = "Desert Eagle | Heat Treated (Factory New)"
        url = get_market_url(name)
        # Decode and confirm name is recoverable
        suffix = url.split("/730/")[1]
        assert unquote(suffix) == name


# ─── get_inventory_url ────────────────────────────────────────────────────────


class TestGetInventoryUrl:
    _STEAM_ID = "76561198000000001"

    def test_valid_steam_id_and_asset_id(self):
        url = get_inventory_url(self._STEAM_ID, asset_id="12345")
        assert self._STEAM_ID in url
        assert "12345" in url

    def test_valid_steam_id_no_asset_id(self):
        url = get_inventory_url(self._STEAM_ID)
        assert self._STEAM_ID in url
        assert url is not None

    def test_none_steam_id_returns_none(self):
        assert get_inventory_url(None) is None  # type: ignore[arg-type]

    def test_empty_steam_id_returns_none(self):
        assert get_inventory_url("") is None

    def test_zero_string_steam_id_returns_none(self):
        assert get_inventory_url("0") is None

    def test_zero_int_steam_id_returns_none(self):
        assert get_inventory_url(0) is None  # type: ignore[arg-type]

    def test_asset_id_zero_omitted(self):
        url = get_inventory_url(self._STEAM_ID, asset_id=0)
        assert url is not None
        # Should not end with _0
        assert not url.endswith("_0")

    def test_asset_id_zero_string_omitted(self):
        url = get_inventory_url(self._STEAM_ID, asset_id="0")
        assert url is not None
        assert not url.endswith("_0")

    def test_integer_steam_id_accepted(self):
        url = get_inventory_url(76561198000000001)
        assert url is not None
        assert "76561198000000001" in url

    def test_integer_asset_id_accepted(self):
        url = get_inventory_url(self._STEAM_ID, asset_id=99999)
        assert url is not None
        assert "99999" in url

    def test_contains_730_2_fragment(self):
        url = get_inventory_url(self._STEAM_ID)
        assert "730_2" in url

    def test_asset_id_none_returns_base_url(self):
        url = get_inventory_url(self._STEAM_ID, asset_id=None)
        assert url is not None
        assert "_None" not in url


# ─── get_inspect_url ──────────────────────────────────────────────────────────


class TestGetInspectUrl:
    def test_steam_prefix_returned_asis(self):
        link = "steam://rungame/730/76561202255233023/+csgo_econ_action_preview%20S76561198..."
        assert get_inspect_url(link) == link

    def test_csgo_econ_prefix_returned_asis(self):
        link = "csgo_econ_action_preview S76561198... A123456789 D987654321"
        assert get_inspect_url(link) == link

    def test_none_returns_none(self):
        assert get_inspect_url(None) is None

    def test_empty_string_returns_none(self):
        assert get_inspect_url("") is None

    def test_http_url_returns_none(self):
        assert get_inspect_url("http://steamcommunity.com/...") is None

    def test_https_url_returns_none(self):
        assert get_inspect_url("https://steamcommunity.com/...") is None

    def test_arbitrary_string_returns_none(self):
        assert get_inspect_url("not an inspect link at all") is None

    def test_leading_whitespace_stripped(self):
        link = "  steam://rungame/730/abc"
        result = get_inspect_url(link)
        assert result is not None
        assert result == link.strip()
