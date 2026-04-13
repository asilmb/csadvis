"""
ingestion.steam — Steam Market client package (PV-48).

Public API re-exported for convenient imports:
    from scrapper.steam import SteamMarketClient, InvalidHashNameError
    from scrapper.steam import normalize_market_hash_name, to_api_name
    from scrapper.steam import fetch_all, fetch_all_current_prices
"""

from scrapper.steam.client import SteamMarketClient, _publish_auth_error
from scrapper.steam.formatter import InvalidHashNameError, normalize_market_hash_name, to_api_name
from scrapper.steam.logic import fetch_all, fetch_all_current_prices
from scrapper.steam.parser import _parse_steam_price

__all__ = [
    "SteamMarketClient",
    "InvalidHashNameError",
    "normalize_market_hash_name",
    "to_api_name",
    "fetch_all",
    "fetch_all_current_prices",
    "_parse_steam_price",
    "_publish_auth_error",
]
