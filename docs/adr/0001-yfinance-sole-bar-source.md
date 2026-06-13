# yfinance is the sole daily-bar source; on failure, serve stale cache — never another source

The US swing scanner needs one canonical, consistently-adjusted price series for its weekly-MA and stop-level logic. The server runs in the US with unrestricted network access, so yfinance is the natural choice: best adjustment quality and earnings dates from the same source. Every successful pull is cached as a per-symbol CSV; when Yahoo is unreachable, the scan runs on the last-good cache with a loud staleness warning.

## Considered Options

- **akshare primary** (what `t_us_tech_swing.py` originally did) — Sina's qfq adjustment basis differs from Yahoo's and data quality is less trusted; its only advantage (reachable from CN networks) is irrelevant on a US server.
- **Futu OpenD for everything** — one ecosystem, but couples the scan to a running daemon and a history quota.
- **Source-switching fallback** (fetch missing days from akshare/Futu when Yahoo fails) — rejected because splicing differently-adjusted series silently shifts moving averages and stop levels; a one-day-stale but internally consistent series is strictly safer for a mid-to-long-term system.

## Consequences

- akshare/Futu prices may be used to *cross-check* yfinance, never to *substitute* into the series.
- Switching bar sources later means re-baselining all cached history, not just changing a fetch call.
- Cache staleness is the designed degradation, not an error.
