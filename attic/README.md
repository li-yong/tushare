# attic — retired CN-era scripts

Code from the original CN A-share / HK screener pipeline, retired when the repo
migrated to the US Nasdaq swing system (2026-06). Kept for reference and as a
source of ideas to port (see the migration plan), **not** run in the live system.

Nothing here is on the daily path. The live system is at the repo root:
`t_us_tech_swing.py`, `ndx_predictor.py`, `us_daily_run.sh`, the parked
`t_futu_trade.py`, and the core library (`finlib.py`, `finlib_indicator.py`,
`constant.py`) which stays at root because `t_futu_trade.py` still imports it.

History is preserved — these were moved with `git mv`, so `git log --follow
attic/<file>` shows their full past.

## What's in here

- **CN daily/weekly/monthly/yearly analyzers** — `t_daily_*`, `t_weekly_*`,
  `t_monthly_*`, `t_yearly_*`, `t_secondly_*` plus their shell runners
  (`t_daily_run_*.sh`, etc.). The old fetch→exam pipeline.
- **Pattern / strategy scripts** — `t_double_bottom.py`, `t_fibonacci.py`,
  `t_cycle.py`, `quekou.py`, `t_daily_turtle.py`, `t_daily_get_key_points.py`,
  `t_daily_pattern_Hit_Price_Volume*.py`, etc.
- **Fundamentals / quality** — `t_yearly_beneish.py`, `t_daily_pe_pb_roe_history.py`,
  `t_daily_fundamentals*.py`.
- **Efficacy DB** — `report_3.py`, `t_summary*.py`,
  `t_monthly_strategy_perf_gathering.py`, `t_daily_backtest.py` (the per-signal
  forward-return database — the repo's most reusable idea, candidate for a phase-2
  rebuild on US data).
- **CN data plumbing** — `ak_share.py`, `t_daily_update_csv_from_*.py`,
  `t_fetch_*.py`, `t_daily_hsgt.py` (northbound flows), `t_daily_get_ag_index*`.
- **Dev junk / unrelated** — `athena.py` (card game), `gptbot_tmp.py` (stub),
  `del.py`, `test_file_output.py`, `pretty_to_csv.py`, `get_forbes.r`.

See `../CONTEXT.md` and `../docs/adr/` for the live system's design.
