#!/usr/bin/env bash
# us_daily_run.sh — daily US swing scan (phase 1: opportunity identification).
#
# Runs the 4-layer scanner once, after the US close, and writes the Morning
# Report to /home/ryan/DATA/result/us_tech_swing_<date>.txt. The scanner tries
# Futu for live holdings stop-status; if OpenD is down it degrades gracefully
# (see ADR-0002). Bars come from yfinance with stale-cache fallback (ADR-0001).
#
# Then runs three supplementary, non-fatal stages (a failure in any does NOT
# fail the run — the exit code stays tied to the primary swing scan):
#   • Regime monitor (t_us_regime_monitor.py): 牛转熊体制 backdrop — layered
#     verdict (200dma break+slope all-weather defense + leading-confluence
#     slow-top early-reduce), validated on 2018/2020/2022 → us_regime_monitor_<date>.txt
#   • Chanlun holdings checkup (t_us_chanlun.py --hold): 三类买卖点 structure
#     over current positions (select.yml US_SWING_STOPS), sell-side focused —
#     走势/最近信号+确认日/失效位/背驰 per name → us_chanlun/us_chanlun_hold_
#     <date>.txt. Exit REFERENCE only (二卖/三卖 vs the layered stops); exit
#     policy stays with the swing scanner (ADR-0002). yfinance-only.
#   • Three-layer chain: t_us_premium → t_us_delivery → t_us_resonance
#     (贵气 × 兑现 × 技术觉醒 共振) → /home/ryan/DATA/result/us_{premium,delivery}_
#     <date>.csv + us_resonance_<date>.txt
#   • Broad consolidation-breakout screener (t_us_breakout_screen.py) across the
#     US large-cap universe → us_breakout_screen_<date>.{txt,csv}
#   • Steady-climber screener (t_us_steady_climb.py) across NASDAQ-100: smooth
#     low-vol uptrends that reclaim dips fast → us_steady_climb_<date>.{txt,csv}
#   • Out-of-pool searchlight (t_us_searchlight.py): scores NDX-100 names outside
#     the watchlist on 贵气×兑现 → us_searchlight_<date>.{txt,csv}
#   • Key-K-line entry scan (t_us_key_kline.py --scan): across S&P 500 ∪ Nasdaq-100,
#     ranks names sitting at a FRESH entry now (突破/初吻/PP) with live stop / 1R →
#     us_key_kline_scan_<date>.txt. The WHEN+WHERE timing pass over a quality pool
#     (NOT a free-market screener — 方法论 §2.1). yfinance-only, no Futu/OpenD.
#   • Gap scan (t_us_gap_scan.py) across S&P 500 ∪ Nasdaq-100, two passes: --scan
#     lists UP-gap entry candidates in two tiers with a reason annotation — Tier A
#     (fresh high-volume strong-close, tight stop) + Tier B (gap survived ≥5d
#     unfilled = market-confirmed, even if vol/close were weak); gap floor = stop
#     → us_gap_scan_<date>.{txt,csv}; --activity ranks names by valid-gap count
#     over 30 days (最活跃/最常跳空) → us_gap_activity_<date>.{txt,csv}.
#     yfinance-only, no Futu/OpenD.
#   • Pullback-shock screen (t_us_pullback_shock.py) across S&P 500 ∪ Nasdaq-100:
#     names in a strong uptrend (>200dma & 半年≥+20% & 距252日高≤15%) that just took
#     a big single-day drop — the empirically positive "buy the dip in a strong
#     name" TIMING (15y event study, 对 QQQ 超额为正、短期即翻正; 只做 A 侧, 不追
#     弱势股大涨). Tiered by drop severity (A ≤-7% / B -5%~-7%); entry=last close,
#     stop=急跌日低−0.5ATR, target=252日高 → us_pullback_shock_<date>.{txt,csv}.
#     yfinance-only, no Futu/OpenD.
#   • Bottom-entry screen (t_us_bottom_entry.py) across S&P 500 ∪ Nasdaq-100:
#     names sitting at a "超跌(距252日高≤−30%)+ 跌破20周线" entry state NOW,
#     tiered by SEC-EDGAR point-in-time ROE (PASS/FAIL/UNKNOWN — quality is a
#     blow-up/left-tail filter, NOT a return ranker; FAIL = junk-bounce, check for
#     structural impairment). The live sibling of t_us_bottom_entry_backtest.py;
#     methodology + empirics in docs/twenty_week_trend_system.md §7 →
#     us_bottom_entry_<date>.{md,csv}. yfinance + SEC EDGAR, no Futu/OpenD.
#   • Breadth diffusion (t_us_breadth_diffusion.py): 板块广度扩散状态机 (点火/
#     确立/成熟/衰竭预警) — semis watchlist vs SOXX + NDX-100 vs QQQ, RS percentile
#     ranked in the SP500∪NDX pool → us_breadth_diffusion/us_breadth_diffusion_
#     <sector>_<date>.txt + series csv. Environment read, not a signal source.
#   • Market network structure (t_us_network_report.py --refresh): runs the 3-stage
#     correlation-network pipeline over Nasdaq-100 — static MST/Louvain
#     (t_us_network_structure.py), dynamic crowding-temperature
#     (t_us_network_dynamics.py), unwind event-study (t_us_network_event_study.py),
#     each writing its own dated txt/png/json — then synthesizes ONE ZH-CN report
#     → us_network_report/us_network_report_<date>.md. Risk/timing ENVIRONMENT
#     judgment (which themes are crowding vs unwinding), NOT buy/sell signals
#     (spec docs/stock_market_network_structure.md §3). yfinance-only.
# Fundamentals come from Futu F10 with yfinance fallback (us_fundamentals.py);
# OpenD down → those stages degrade to yfinance / empty, never crash.
#
# Schedule via cron at 14:00 America/Los_Angeles, weekdays — see the CRON block
# at the bottom of this file.

set -u
set -o pipefail

REPO=/home/ryan/tushare_ryan
PY=/home/ryan/miniconda3/bin/python
LOG=$REPO/us_daily_run.log

# cron runs with a minimal environment; make conda's python and tools reachable.
export PATH=/home/ryan/miniconda3/bin:/usr/local/bin:/usr/bin:/bin

cd "$REPO" || { echo "cannot cd to $REPO" >&2; exit 1; }

ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

# run_step NAME CMD... — run a supplementary stage, log its rc, never fail the run.
run_step() {
    local name="$1"; shift
    echo "----- $name start $(ts) -----" >> "$LOG"
    "$@" >> "$LOG" 2>&1
    local r=$?
    echo "----- $name done (rc=$r) $(ts) -----" >> "$LOG"
}

echo "===== us_daily_run start $(ts) =====" >> "$LOG"

# Full-universe scan. No --no-futu: let it use OpenD when available.
# Also writes us_tech_signal_<date>.csv, which the resonance stage joins.
"$PY" t_us_tech_swing.py >> "$LOG" 2>&1
rc=$?

# Supplementary: market-regime monitor (牛转熊体制) — the macro backdrop for the
# whole report: "should we even be adding risk today?". Layered verdict — 200dma
# break+slope (all-weather defense) + leading-confluence (slow-top early-reduce).
# Validated across 2018/2020/2022 (see docs/bull_to_bear_2021_2022.md). yfinance-
# only; reuses the universe cache the swing scan just refreshed. Writes its own
# dated report us_regime_monitor_<date>.txt. Non-fatal — its rc never flips the run.
run_step "regime-monitor" "$PY" t_us_regime_monitor.py

# Supplementary: signal canary (信号金丝雀) — rolling 21-day losing rate of the
# system's OWN ledger episodes (last 40 completed). NOT an early warning — the
# 2021-22 validation falsified that (melt-up tops read LOWEST right before the
# turn); it is a COINCIDENT pool-health confirmation: high = the recent signal
# environment is already deteriorating. Runs right after tech_swing so today's
# ledger appends are included; honestly reports 样本不足 while the ledger is
# young. Verdict & calibration: t_us_signal_canary.py header + --validate.
run_step "signal-canary" "$PY" t_us_signal_canary.py

# Supplementary: chanlun holdings checkup (缠论持仓体检, 卖点侧) — 三类买卖点
# structure read over the hand-maintained live positions (select.yml
# US_SWING_STOPS; no Futu/OpenD). Division runs on FULL history (append-only,
# zero-repaint validated on MU/NVDA) and every signal carries its 实际确认日.
# Reference layer for exits (二卖=最后逃命点 / 三卖=趋势展开) alongside the
# layered stops — NOT a stop engine; exit policy stays with the swing scanner
# (ADR-0002). Table tee'd to a dated report. Non-fatal — its rc never flips
# the run's exit code.
echo "----- chanlun hold start $(ts) -----" >> "$LOG"
CH_TXT=/home/ryan/DATA/result/us_chanlun/us_chanlun_hold_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$CH_TXT")"
"$PY" t_us_chanlun.py --hold 2>> "$LOG" | tee "$CH_TXT" >> "$LOG"
ch_rc=${PIPESTATUS[0]}
echo "----- chanlun hold done (rc=$ch_rc) $(ts) -----" >> "$LOG"

# Supplementary: three-layer chain (贵气 → 兑现 → 共振). premium fills the Futu
# fundamentals cache; delivery reuses it; resonance joins the three CSVs. All
# non-fatal (run_step), so they never flip the run's exit code.
run_step "premium"   "$PY" t_us_premium.py
run_step "delivery"  "$PY" t_us_delivery.py
run_step "resonance" "$PY" t_us_resonance.py

# Supplementary: broad consolidation-breakout screen across US large-caps.
# Non-fatal — log its own rc, but the run's exit code stays tied to the primary
# swing scan above. Its printed table is teed to a dated report; CSV is written
# by the script itself.
echo "----- breakout screen start $(ts) -----" >> "$LOG"
BO_TXT=/home/ryan/DATA/result/us_breakout_screen/us_breakout_screen_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$BO_TXT")"
"$PY" t_us_breakout_screen.py 2>> "$LOG" | tee "$BO_TXT" >> "$LOG"
bo_rc=${PIPESTATUS[0]}
echo "----- breakout screen done (rc=$bo_rc) $(ts) -----" >> "$LOG"

# Supplementary: steady-climber screen (小步慢涨·跌一点快补回) across NASDAQ-100.
# Smooth low-vol uptrends that reclaim dips fast — writes its own dated txt+csv.
# Non-fatal: logs its own rc, never flips the run's exit code.
run_step "steady-climb" "$PY" t_us_steady_climb.py --universe ndx

# Supplementary: out-of-pool searchlight (池外侦察) — scores every NDX-100 name
# outside the current watchlist on 贵气×兑现, surfacing stronger names we don't
# yet track. Writes its own CSV; we tee the human table to a dated report.
# Non-fatal — its rc never flips the run's exit code.
echo "----- searchlight start $(ts) -----" >> "$LOG"
SL_TXT=/home/ryan/DATA/result/us_searchlight/us_searchlight_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$SL_TXT")"
"$PY" t_us_searchlight.py 2>> "$LOG" | tee "$SL_TXT" >> "$LOG"
sl_rc=${PIPESTATUS[0]}
echo "----- searchlight done (rc=$sl_rc) $(ts) -----" >> "$LOG"

# Supplementary: key-K-line entry scan (择时 + 止损) over S&P 500 ∪ Nasdaq-100.
# Ranks names at a fresh entry right now (突破/初吻/PP) with live stop / 1R; the
# table is teed to a dated report. No --plot-top in cron (avoid a daily flood of
# PNGs in result/ — plot survivors by hand). yfinance-only; no Futu dependency.
# Non-fatal — its rc never flips the run's exit code.
echo "----- key-kline scan start $(ts) -----" >> "$LOG"
KK_TXT=/home/ryan/DATA/result/us_key_kline/us_key_kline_scan_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$KK_TXT")"
"$PY" t_us_key_kline.py --scan --universe both 2>> "$LOG" | tee "$KK_TXT" >> "$LOG"
kk_rc=${PIPESTATUS[0]}
echo "----- key-kline scan done (rc=$kk_rc) $(ts) -----" >> "$LOG"

# Supplementary: gap scan (跳空缺口) over S&P 500 ∪ Nasdaq-100. Two passes, each
# tee'd to a dated report; both write their own CSV. --scan = 近期放量、收强、仍
# 未回补的向上缺口(可操作, 缺口下沿做天然止损); --activity = 缺口活跃度排行(近
# 30 日有效缺口次数, 找最常跳空的高波动名)。yfinance-only; no Futu dependency.
# Non-fatal — their rc never flips the run's exit code.
echo "----- gap scan start $(ts) -----" >> "$LOG"
GAP_TXT=/home/ryan/DATA/result/us_gap_scan/us_gap_scan_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$GAP_TXT")"
"$PY" t_us_gap_scan.py --scan --universe both 2>> "$LOG" | tee "$GAP_TXT" >> "$LOG"
gap_rc=${PIPESTATUS[0]}
echo "----- gap scan done (rc=$gap_rc) $(ts) -----" >> "$LOG"

echo "----- gap activity start $(ts) -----" >> "$LOG"
GAPA_TXT=/home/ryan/DATA/result/us_gap_scan/us_gap_activity_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$GAPA_TXT")"
"$PY" t_us_gap_scan.py --activity --universe both --days 30 2>> "$LOG" | tee "$GAPA_TXT" >> "$LOG"
gapa_rc=${PIPESTATUS[0]}
echo "----- gap activity done (rc=$gapa_rc) $(ts) -----" >> "$LOG"

# Supplementary: pullback-shock screen (强势股·单日急跌 回调买点) over S&P 500 ∪
# Nasdaq-100. Surfaces names in a strong uptrend (>200dma & 半年≥+20% & 距252日高
# ≤15%) that just had a big single-day drop — an empirically positive dip-buy/add
# TIMING (15y event study: 单日≤-7% → 21日 alpha +2.5% / 63日 +6.1% vs QQQ, 短期即
# 翻正). The A-side counterpart to NOT chasing downtrend pops (B side loses week 1).
# Entry=last close, stop=急跌日低−0.5ATR, target=252日高. Tee'd to a dated report;
# CSV written by the script. yfinance-only, no Futu/OpenD. Non-fatal.
echo "----- pullback-shock start $(ts) -----" >> "$LOG"
PS_TXT=/home/ryan/DATA/result/us_pullback_shock/us_pullback_shock_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$PS_TXT")"
"$PY" t_us_pullback_shock.py --scan --universe both 2>> "$LOG" | tee "$PS_TXT" >> "$LOG"
ps_rc=${PIPESTATUS[0]}
echo "----- pullback-shock done (rc=$ps_rc) $(ts) -----" >> "$LOG"

# Supplementary: bottom-entry screen (超跌底部入场) over S&P 500 ∪ Nasdaq-100.
# Lists names at a "超跌 + 跌破20周线(线下)" entry state now, tiered by SEC-EDGAR
# point-in-time ROE (PASS 防归零优先 / FAIL 高弹性需查结构性受损 / UNKNOWN). Writes
# its own dated md+csv to result/us_bottom_entry/. Bound to the 20-week system §7;
# quality is a left-tail filter not a ranker. yfinance + SEC EDGAR; no Futu/OpenD.
# Note: PIT-ROE cache (backtest_cache/pit_quality.pkl) is built once and reused —
# refresh it periodically (run with --force) to pick up new 10-K filings.
# Non-fatal (run_step) — its rc never flips the run's exit code.
run_step "bottom-entry" "$PY" t_us_bottom_entry.py --universe both

# Supplementary: breadth diffusion (板块广度扩散 — 风口启动/衰竭状态机, spec
# docs/breadth_diffusion_framework.md). Four indicators (NH-NL accel / %>50MA
# Zweig thrust / AD-line divergence / rs_breadth accel) → one state per day:
# IGNITION→ESTABLISHED→MATURE→EXHAUSTION_WARN. Two passes: the semis watchlist
# vs SOXX (the semis barometer tech_swing already refreshes daily, so its
# cache is always warm) and NDX-100 vs QQQ (pool-level read).
# RS percentile ranks inside the SP500∪NDX reference pool (--rs-ref both default;
# the ref pool MUST be larger than the sector or rs_breadth degenerates to a
# constant). Runs late so the both-pool bar cache is already warm from the scans
# above. Writes its own dated txt+csv to result/us_breadth_diffusion/. State/
# environment read, NOT a buy/sell signal source — not fed to the daily-report
# merger. yfinance-only, no Futu/OpenD. Non-fatal (run_step).
run_step "breadth-semis" "$PY" t_us_breadth_diffusion.py --watchlist US_SWING_SEMIS --benchmark SOXX
run_step "breadth-ndx"   "$PY" t_us_breadth_diffusion.py --pool ndx --benchmark QQQ

# Supplementary: market network-structure analysis (相关性网络 抱团/瓦解 环境研判).
# One command (--refresh) runs the 3-stage pipeline — static MST/Louvain → dynamic
# crowding-temperature → unwind event-study, each writing its own dated txt/png/json
# — then synthesizes a single ZH-CN report → us_network_report/us_network_report_
# <date>.md. Environment/risk read (which themes are crowding vs unwinding), NOT a
# buy/sell signal source, so it is NOT fed to the us_daily_report merger below.
# yfinance-only; reuses the bar cache the swing scan just refreshed; no Futu/OpenD.
# Non-fatal (run_step) — its rc never flips the run's exit code.
run_step "network-structure" "$PY" t_us_network_report.py --refresh --no-plot --universe ndx --groups baskets

# Final: merge every sub-report above into one combined daily report
# (/home/ryan/DATA/result/us_daily_report_<date>.md). Pulls the actionable
# BUY/SELL/manage signals out of each sub-report — each annotated with its
# reason and source — plus a multi-screen confluence tally, then appends every
# sub-report verbatim. Runs last so all inputs exist. Non-fatal (run_step).
run_step "daily-report" "$PY" us_daily_report.py

if [ $rc -eq 0 ]; then
    echo "===== us_daily_run OK    $(ts) =====" >> "$LOG"
else
    echo "===== us_daily_run FAIL (rc=$rc) $(ts) =====" >> "$LOG"
fi

exit $rc

# ── CRON ──────────────────────────────────────────────────────────────────────
# Install with `crontab -e` and add the two lines below. CRON_TZ pins the
# schedule to US Pacific regardless of the server's own timezone (this box was
# Asia/Shanghai at setup time), so it fires at 14:00 PT — ~1h after the close —
# on weekdays. DST is handled automatically by the IANA zone.
#
#   CRON_TZ=America/Los_Angeles
#   0 14 * * 1-5  /home/ryan/tushare_ryan/us_daily_run.sh
