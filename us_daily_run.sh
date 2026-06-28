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
BO_TXT=/home/ryan/DATA/result/us_breakout_screen_$(date +%Y%m%d).txt
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
SL_TXT=/home/ryan/DATA/result/us_searchlight_$(date +%Y%m%d).txt
"$PY" t_us_searchlight.py 2>> "$LOG" | tee "$SL_TXT" >> "$LOG"
sl_rc=${PIPESTATUS[0]}
echo "----- searchlight done (rc=$sl_rc) $(ts) -----" >> "$LOG"

# Supplementary: key-K-line entry scan (择时 + 止损) over S&P 500 ∪ Nasdaq-100.
# Ranks names at a fresh entry right now (突破/初吻/PP) with live stop / 1R; the
# table is teed to a dated report. No --plot-top in cron (avoid a daily flood of
# PNGs in result/ — plot survivors by hand). yfinance-only; no Futu dependency.
# Non-fatal — its rc never flips the run's exit code.
echo "----- key-kline scan start $(ts) -----" >> "$LOG"
KK_TXT=/home/ryan/DATA/result/us_key_kline_scan_$(date +%Y%m%d).txt
"$PY" t_us_key_kline.py --scan --universe both 2>> "$LOG" | tee "$KK_TXT" >> "$LOG"
kk_rc=${PIPESTATUS[0]}
echo "----- key-kline scan done (rc=$kk_rc) $(ts) -----" >> "$LOG"

# Supplementary: gap scan (跳空缺口) over S&P 500 ∪ Nasdaq-100. Two passes, each
# tee'd to a dated report; both write their own CSV. --scan = 近期放量、收强、仍
# 未回补的向上缺口(可操作, 缺口下沿做天然止损); --activity = 缺口活跃度排行(近
# 30 日有效缺口次数, 找最常跳空的高波动名)。yfinance-only; no Futu dependency.
# Non-fatal — their rc never flips the run's exit code.
echo "----- gap scan start $(ts) -----" >> "$LOG"
GAP_TXT=/home/ryan/DATA/result/us_gap_scan_$(date +%Y%m%d).txt
"$PY" t_us_gap_scan.py --scan --universe both 2>> "$LOG" | tee "$GAP_TXT" >> "$LOG"
gap_rc=${PIPESTATUS[0]}
echo "----- gap scan done (rc=$gap_rc) $(ts) -----" >> "$LOG"

echo "----- gap activity start $(ts) -----" >> "$LOG"
GAPA_TXT=/home/ryan/DATA/result/us_gap_activity_$(date +%Y%m%d).txt
"$PY" t_us_gap_scan.py --activity --universe both --days 30 2>> "$LOG" | tee "$GAPA_TXT" >> "$LOG"
gapa_rc=${PIPESTATUS[0]}
echo "----- gap activity done (rc=$gapa_rc) $(ts) -----" >> "$LOG"

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
