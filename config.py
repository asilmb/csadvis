from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parent
_ENV_FILE = _PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",  # silently drop unknown .env keys (e.g. old Skinport fields)
    )

    steam_id: str = ""  # Steam ID for inventory auto-load (e.g. 76561198...)
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_internal_host: str = "api"  # Docker service name; override to "localhost" for local dev
    dashboard_port: int = 8050

    # Display currency
    currency_symbol: str = "₸"  # symbol shown in UI labels and axis ticks

    # Market constants — override in .env if Valve changes pricing
    key_price: float = 1200.0  # standard CS2 case key price (~$2.49 at 481₸/$)
    steam_fee_divisor: float = 1.15  # Steam Market 15 % fee: net = price / 1.15 − fixed
    steam_fee_fixed: float = 5.0  # Steam fixed minimum fee per transaction (~$0.01)

    # Signal thresholds — override in .env if market conditions change
    ratio_floor: float = (
        120.0  # skip ratio signal when current_price < this value (~$0.25 at 481₸/$)
    )
    momentum_event_threshold: float = (
        15.0  # momentum sell threshold (%) for event-matched containers
    )

    # Flip quality gates — override in .env to tune
    flip_min_net_margin: float = 0.05  # minimum net margin for flip candidates (5 %)
    flip_sell_target_cap: float = 1.20  # max sell target as multiple of current price (120 %)

    # Liquidity guard — override in .env to tune
    liquidity_min_volume: float = 10.0  # minimum daily units traded for BUY signals to be valid

    # Order book wall filter — override in .env to tune
    wall_max_days: int = 7  # max estimated days to absorb sell wall before target price

    # Event calendar YAML (F-07) — override in .env to use a custom path or remote URL
    events_yaml_path: str = "data/events.yaml"  # path to local YAML event calendar file
    events_remote_url: str = ""  # remote URL for cs2 events refresh (empty = disabled)

    # Webhook notifications (PV-17) — leave empty to disable
    webhook_url: str = ""  # POST endpoint for SuperDealDetected / AuthError alerts


settings = Settings()
