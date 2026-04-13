# CLAUDE_DESIGN_SPEC.md — UI/UX Layout

## Dashboard Structure
- **System Tab:** Liveness, Steam Cookie Valid/Expired status, Token Bucket capacity, Active Workers, Watchdog Registry, Task Control (Pending, Processing, Failed), Recent Task History.
- **Inventory Tab:** Raw assets, visibility toggles ("Show All"), quantity and pricing state.
- **Portfolio / Analytics:** Baseline comparisons, price momentum, profit margins.

## UX Rules
- **Polling:** Auto-refresh system states via Dash `dcc.Interval`.
- **Modals:** Steam Auth (`sessionid` and `steamLoginSecure`) updates via popups to unpause tasks.
- **Format:** Account currency, symbol from `settings.currency_symbol`.
- **Status Indicators:** P1 (High), P2 (Low) priority coloring. PROCESSING status always visible at the top.