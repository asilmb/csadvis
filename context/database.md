# Database Schema (PostgreSQL 16 / TimescaleDB)

## Tables

### dim_containers
```
container_id: UUID (PK)
container_name: str
container_type: Enum (Weapon Case, Souvenir Package, Sealed Terminal,
                       Sticker Capsule, Autograph Capsule, Event Capsule)
base_cost: float
is_blacklisted: bool (default False)
```

### fact_container_prices
```
id: BigInteger (PK)
container_id: UUID (FK → dim_containers)
timestamp: datetime (UTC, no tz)
price: float
mean: float | null
lowest_price: float | null
volume_7d: int | null
source: str (steam_market | steam_live | daily_aggregate)
```

### dim_user_positions
```
id: UUID (PK)
container_name: str
buy_price: float
quantity: int
buy_date: datetime
```

### positions (new — replaces dim_user_positions in reconciler)
```
id: UUID (PK)
container_id: UUID (FK)
asset_id: str | null  (Steam assetid for reconciliation)
quantity: int
buy_price: float
buy_date: datetime
trade_unlock_at: datetime | null
listing_id: str | null
```

### fact_portfolio_snapshots
```
id: UUID (PK)
timestamp: datetime
wallet: float
inventory: float
```

### fact_transactions
```
id: UUID (PK)
timestamp: datetime
type: str (BUY | SELL | FLIP)
container_name: str
price: float
total: float
pnl: float
listing_id: str | null
```

### dim_annual_summary
```
id: int (PK)
year: int (unique)
pnl: float
notes: str | null
```

### fact_portfolio_advice
```
id: UUID (PK)
computed_at: datetime
advice_json: JSON   — always 1 row, replaced on refresh
```

### fact_investment_signals
```
id: UUID (PK)
container_id: UUID (FK)
computed_at: datetime
verdict: str (BUY | LEAN BUY | HOLD | LEAN SELL | SELL | NO DATA)
score: int [-2, +2]
price_ratio_pct: float | null
momentum_pct: float | null
ratio_signal: str (CHEAP | EXPENSIVE | NEUTRAL)
momentum_signal: str (RISING | FALLING | STABLE)
sell_at_loss: bool
unrealized_pnl: float | null
```

### task_queue
```
id: UUID (PK)
type: str
status: Enum (PENDING | PROCESSING | COMPLETED | FAILED | RETRY)
priority: int (1=HIGH, 2=MEDIUM, 3=LOW)
payload: JSON | null
created_at: datetime
updated_at: datetime
retries: int (default 0)
```

### worker_registry
```
id: UUID (PK)
name: str (hostname)
status: str
last_heartbeat: datetime
current_task_id: str | null
```

### event_log
```
id: UUID (PK)
event_type: str
payload: JSON
created_at: datetime
```

### system_settings
```
key: str (PK)
value: str
updated_at: datetime
```

## Constraints
- Currency: single account currency — no USD/EUR/RUB conversions stored
- Timestamps: UTC, naive (no tzinfo) — `datetime.now(UTC).replace(tzinfo=None)`
- Prices: `float` (not Decimal) — financial rounding via `Amount` domain value object

## Redis Keys (runtime state, not persisted to DB)
```
cs2:backfill:last_run      → ISO datetime of last backfill run
cs2:scraper:last_parsed    → date string of last scraper run
cs2:wallet:balance         → float as string
cs2:nameid:cache           → Hash {market_hash_name: item_nameid}
STEALTH_BLOCK_EXPIRES      → reason string, TTL 6h (set on HTTP 429)
```
