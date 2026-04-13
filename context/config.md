# Configuration (`config.py` → `.env` file)

| Key | Default | Description |
|-----|---------|-------------|
| `DATABASE_PATH` | `./cs2_analytics.db` | SQLite file path |
| `STEAM_LOGIN_SECURE` | `""` | steamLoginSecure cookie |
| `STEAM_ID` | `""` | Steam ID for inventory |
| `LOG_LEVEL` | `INFO` | Logging level |
| `API_HOST` | `0.0.0.0` | FastAPI host |
| `API_PORT` | `8000` | FastAPI port |
| `DASHBOARD_PORT` | `8050` | Dash port |
| `key_price` | `1200.0` | CS2 key price in account currency |
| `steam_fee_divisor` | `1.15` | Steam 15% fee divisor |
| `steam_fee_fixed` | `5.0` | Steam fixed fee per transaction |
| `ratio_floor` | `120.0` | Min price for ratio signal |
| `momentum_event_threshold` | `15.0` | Momentum sell threshold for event containers (%) |
| `flip_min_net_margin` | `0.05` | Minimum net margin for flip candidates (5%) |
| `flip_sell_target_cap` | `1.20` | Max sell target as multiple of current price |
| `wall_max_days` | `7` | Max days to absorb sell wall (WALL-1) |
| `events_yaml_path` | `data/events.yaml` | Local YAML calendar path |
| `events_remote_url` | `""` | Remote URL for `cs2 events refresh` |

## Engine Constants (not in .env — hardcoded in engine modules)

| Constant | Value | Location | Description |
|----------|-------|----------|-------------|
| `_MIN_NET_CAGR` | `0.01` | `engine/portfolio_advisor.py` | Minimum Net CAGR (after fees) for invest candidates (PV-08) |
| `_MIN_SAMPLE_PEARSON` | `30` | `engine/correlation.py` | Minimum shared data points for Pearson r computation |
| `_HIGH_CORR_THRESHOLD` | `0.70` | `engine/correlation.py` | |r| above this triggers correlation warning |
| `_BUY_RATIO_THRESHOLD` | `0.85` | `engine/investment.py` | price/baseline < 0.85 → buy point |
| `_SELL_RATIO_THRESHOLD` | `1.20` | `engine/investment.py` | price/baseline > 1.20 → sell point |
| `_BUY_MOMENTUM_THRESHOLD` | `-5.0` | `engine/investment.py` | momentum < -5% → buy point |
| `_SELL_MOMENTUM_THRESHOLD` | `8.0` | `engine/investment.py` | momentum > 8% → sell point (non-event) |
