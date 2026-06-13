# The scanner owns exit policy; automated enforcement is deferred to the automation phase

The swing scanner (Layer 4: breakeven at +15%, trim at +25%, exit on 20-week MA breach, stops under key levels) and the Futu trailing-stop daemon (autonomous 5% giveback from high-water mark) were two competing exit policies aimed at the same holdings — and the tighter 5% trail would always fire first, vetoing the mid-to-long-term thesis on volatile names like NVDA. We decided the scanner is the single exit authority.

Because the system's current purpose is opportunity identification (the user executes all trades manually, holding weeks to months), exits are evaluated on **daily closes** by the morning scan and surfaced in the report ("stop breached → exit today"). No intraday enforcement runs in this phase; the daemon is parked.

## Target architecture (automation phase, future)

When automated trading begins, the division of labor is: scanner computes each position's current stop level daily and writes per-position stop prices into the daemon's config (`trailing_stop_config.json`); the daemon enforces those levels intraday through Futu. Its autonomous trail-percent logic stays retired — do not re-enable it. Promotion to the real account requires ≥2 clean weeks in simulation including at least one correctly-executed stop trigger.

## Consequences

- `t_futu_trade.py` is not part of the phase-1 daily loop; its surviving value is the high-water-mark persistence, sim/real modes, and order machinery for the automation phase.
- Daily-close stop evaluation means an intraday crash is absorbed until the next morning's report — an accepted property of the mid-to-long-term horizon, not a bug.
- If the scan fails to run, positions keep yesterday's stop levels (stale but defined).
