# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A personal quant-trading toolkit, originally for CN A-shares (unmaintained since ~2023), now migrating to a narrow US Nasdaq swing-trading system. It is a flat collection of Python scripts, not a package — there is no build step, no test suite, and no linter. Python 3.12 from miniconda (`/home/ryan/miniconda3/bin/python`). Comments and output are mixed Chinese/English; market prefixes in filenames are `ag_` (A-share), `hk_`, `us_`.

**Migration status (2026-06):** the live system is the US core — `t_us_tech_swing.py` (4-layer swing scanner) and `ndx_predictor.py` (quarterly NDX rebalance predictor), run daily by `us_daily_run.sh` (cron, 14:00 America/Los_Angeles). The system identifies opportunities for mid-to-long-term positions; the user trades manually, and automated execution (via `t_futu_trade.py`, currently parked) is a future phase. The CN-era scripts are retired in `attic/` (see `attic/README.md`); the root now holds only the live US core plus the shared library (`finlib.py`, `finlib_indicator.py`, `constant.py`, still imported by `t_futu_trade.py`). Read `CONTEXT.md` for the domain language and `docs/adr/` before changing the US core; key decisions: yfinance is the sole bar source with stale-cache fallback (ADR-0001), the scanner owns exit policy with stops evaluated at daily close (ADR-0002).

## Architecture

- **`finlib.py`** — the core library (~6700 lines). A single `Finlib` god-class providing: data fetch via Tushare/Tushare-Pro and AKShare, stock-code format conversion (`SH600519` ↔ `600519.SH` ↔ `600519`; see `get_code_format`, `add_market_to_code`, `ts_code_to_code`), trading-day calendars (`get_last_trading_day`, `is_a_trading_day_ag`), fundamental "garbage stock" filtering (`remove_garbage*`, using reason strings from `constant.py`), file-freshness caching (`is_cached`, `file_verify`), and MySQL persistence. Note: `logging` is configured before `import tushare` on purpose (workaround); logs append to `/home/ryan/del.log`.
- **`finlib_indicator.py`** — `Finlib_indicator` class: technical indicators (SMA/EMA/MACD/KDJ/ATR, bar styles, 均线/金叉/死叉 trend logic) built on TA-Lib.
- **`constant.py`** — shared string constants: garbage reasons, bar-style names, BUY/SELL operation labels. Filter results and signals are matched by these strings, so reuse them rather than inventing new literals.
- **`select.yml`** — watchlists and current holdings (`US_HOLD`, `HK_HOLD`, `CN_HOLD`, `US`, ...) consumed by analysis scripts.
- **`doc/script_brief_intro.txt`** — maps each `t_*.py` script to the CSV files it produces.

### Live orchestration (US core)

Three cron runners, all pinned to `CRON_TZ=America/Los_Angeles` 14:00 (~1h after the US close). Cadence matches how often each stage's underlying data moves; pick the runner accordingly when adding a script. Each runner's primary stage drives its exit code; supplementary stages use the `run_step` helper (or a tee'd block) so a single failure logs its own rc but never flips the run, and OpenD-down stages degrade gracefully.

- **`us_daily_run.sh`** — weekdays (`0 14 * * 1-5`). Primary: `t_us_tech_swing.py` → Morning Report `result/us_tech_swing_<date>.txt`. Supplementary: three-layer chain `t_us_premium.py → t_us_delivery.py → t_us_resonance.py` (贵气×兑现×共振); `t_us_breakout_screen.py` (consolidation breakout); `t_us_steady_climb.py` (小步慢涨·跌一点快补回, NDX-100); `t_us_searchlight.py` (池外 贵气×兑现 侦察); `t_us_key_kline.py --scan --universe both` (关键K线 择时扫描, SP500∪NDX → `us_key_kline_scan_<date>.txt`). Logs to `us_daily_run.log`.
- **`us_weekly_run.sh`** — Sundays (`0 14 * * 0`). Slow-moving screens: `t_us_undervalue.py` (超跌优质, SP500∪NDX; primary) + `t_us_watchlist_suggest.py` (print-only — never `--apply` from cron) + `t_us_news_top_detector.py` (新闻见顶稻草 + 启动火种: Grok top_scan × §2.1 exhaustion bar, 加 §6 镜像——SP500∪NDX 关键K线价格漏斗 → Grok launch_scan; ~$5/run, degrades to price-only without xAI; see `docs/news_driven_top_detection.md`) + `t_us_signal_attrib.py` (信号归因: forward outcomes for every episode in `result/us_signal_log/us_signal_ledger.csv`, which `t_us_tech_swing.py` and `t_us_news_top_detector.py` append to on live runs via `signal_ledger.py`; runs after news-top so fresh episodes are included). Logs to `us_weekly_run.log`.
- **`us_monthly_run.sh`** — 1st of month (`0 14 1 * *`). `t_us_cycle_monitor.py` (DRAM/AI 周期, SEC EDGAR quarterly XBRL — monthly catches every new filing). Logs to `us_monthly_run.log`.
- Run by hand: `ndx_predictor.py` (quarterly, 3/6/9/12, NDX rebalance); `t_us_key_kline.py` — `--ticker SYM` annotates one name (chart + 现状 timing/stop block + legend), or `--scan` sweeps a pre-filtered universe (S&P 500 ∪ Nasdaq-100; `--universe sp500|ndx|both`, `--plot-top N`) for names at a fresh entry now, ranked. Not a free-market screener (方法论 §2.1: only quality pools have a real 主语).
- **`huice.py`** — 指示级回测 (run by hand): point-in-time replay of the scanners' directives (tech_swing / key_kline / gap_scan / undervalue via `--source`), forward-simulated under the system's own exit discipline. `-asof <date> --ticker SYM` replays one day; `--start/--end [--universe sp500|ndx|both] [--jobs N]` sweeps a range (episodes deduped like the signal ledger). It reuses the live scanners' code through `tsw._ASOF` truncation — when adding a per-date memo, always re-anchor `_ASOF` on cache hits or later tickers leak the future. Findings: `docs/huice_backtest_findings.md` — headline: edge lives in the pool, not the patterns (all sources lose their α on SP500∪NDX); positive expectancy is regime-conditional (2022: all meanR negative, right tail gone, but the layered stops kept avg loss per episode under ~3.5%); MIXED-state entries are the one robust negative-EV pocket. Synthetic replay has no execution friction — the live ledger (`t_us_signal_attrib.py`) stays the source of truth.

### CN-era orchestration (retired, in `attic/`)

The historical pipeline below is no longer run; it lives in `attic/`. Scripts there are named by run cadence (`t_daily_*`, `t_weekly_*`, `t_monthly_*`, `t_yearly_*`, `t_secondly_*`) and orchestrated by shell runners taking a `FULL`/`DAILY` argument (`t_daily_run_0.sh` → `t_daily_run_1_update_data.sh` → `t_daily_run_2_exam.sh`, running `report_3.py` and the `t_daily_*` analyzers). They use argparse flags like `--fetch_basic_daily`, `--refresh_qfq`, `--force_run`, `--no_question`.

### Data layout (outside this repo)

All data lives under `/home/ryan/DATA/`, which the scripts hard-code:

- `DAY_Global/{AG,AG_qfq,AG_INDEX,US,HK,...}/*.csv` — per-symbol daily bars.
- `pickle/` — cached instrument lists, fundamentals, index membership.
- `result/` — analysis output. Each US script writes its dated reports into its **own per-script subfolder** (`result/us_tech_swing/us_tech_swing_<date>.txt`, `result/us_gap_scan/`, `result/daily_report/us_daily_report_<date>.md`, …); `us_tech_signal` lives under `us_tech_swing/` and `us_gap_activity` under `us_gap_scan/`. Cross-script readers (`us_daily_report.py`, `t_us_resonance.py`) resolve files with a **recursive glob** over `result/`, so they find a report whether it's flat or sub-foldered — keep that contract when adding readers. (Legacy CN-era `result/today/` and `result/Selected/` subsets may still exist.) `result/` is its own git repo (the runners `git pull` it).

A MySQL database `ryan_stock_db` (localhost) stores pattern-performance and order-tracking tables (`create_or_update_ptn_perf_db_record`, `t_daily_update_order_tracking_stock.py`).

### Trading integrations

- **Futu**: `t_futu_trade.py` requires a running Futu OpenD (default `127.0.0.1:11111`). `run_trailing_stop.sh` wraps its trailing-stop mode (simulation by default; `-r` for real account — see `trailing_stop_readme.md`).
- **`ndx_predictor.py`** — standalone NASDAQ-100 rebalance predictor (Futu snapshots + nasdaqtrader.com + Nasdaq screener API); `--demo` runs on synthetic data without network/OpenD, caches in `ndx_cache/`, writes `ndx_report.md`.
- **`Oanda_v2/`** — forex trading (separate sub-project with its own `lib/` and `logic/`).
- **`SF/`** — unrelated Salesforce/Selenium side project; ignore for trading work.

## Conventions

- Most scripts assume CWD is `/home/ryan/tushare_ryan` and use absolute paths into `/home/ryan/DATA`.
- Expensive fetch/compute steps are guarded by file-mtime caching (`Finlib.is_cached`) keyed to the last trading day; `--force_run` bypasses it.
- When adding a new selection/filter reason, define the string in `constant.py`.

## Agent skills

### Issue tracker

Issues live as local markdown files under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
