# Dashboard Structure

| Tab label | value | Renderer |
|-----------|-------|---------|
| Анализ | `market` | `frontend/renderers/market.py` |
| Inventory | `inventory` | `frontend/inventory.py` |
| Portfolio | `portfolio` | `frontend/renderers/portfolio.py` |
| Balance | `balance` | `frontend/balance.py` |
| Analytics | `analytics` | `frontend/renderers/analytics.py` |

**Layout:** Sidebar (width=3, container list + search) + Main content (width=9, tabs + tab content)

**Navbar:** Logo + "Last updated" badge + scheduler badge + version + "Синхронизировать" button

**Key callbacks (frontend/callbacks.py):**
- `sync_all` — triggers wallet + inventory + transactions sync, then refresh_cache
- `update_container_list` — sidebar filter by search string + signal verdict
- `render_tab` — routes to renderer based on `main-tabs` value
- `update_last_updated_badge` — shows staleness, orange when cache > 1h
- `update_armory_pass` — Portfolio tab Step 4 widget (F-09)

**Intervals:**
- `auto-refresh`: 5 min — triggers full re-render
- `startup-interval`: 1 shot at 1s — triggers initial load
- `header-interval`: 60s — updates last-updated badge
- `sync-reset-interval`: 2s — resets sync button state after completion

**Design tokens (frontend/theme.py — COLORS dict):**
```python
"bg":     "#0f1923"  # dark navy background
"bg2":    "#1a2433"  # card background
"border": "#2a3a4a"  # border color
"text":   "#c7d5e0"  # primary text
"muted":  "#8f98a0"  # secondary text
"gold":   "#ffd700"  # highlights
"green":  "#00c853"  # positive/BUY
"yellow": "#ffd600"  # HOLD
"red":    "#eb4b4b"  # negative/SELL
"orange": "#ff9800"  # LEAN signals
"blue":   "#66c0f4"  # info/links
```
