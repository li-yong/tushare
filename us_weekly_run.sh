#!/usr/bin/env bash
# us_weekly_run.sh — weekly US slow-moving screens (cadence matched to the data).
#
# These two look at things that change on the scale of weeks, not days, so we
# run them weekly rather than burning a daily slot on near-identical output:
#   • Undervalued-quality screener (t_us_undervalue.py) across S&P 500 ∪ NDX-100:
#     market cap > $1B, 1y drop > 30%, 3y-avg ROE > 12% → a "超跌但底子好"
#     candidate list for manual review → us_undervalue_<date>.csv
#   • Watchlist suggester (t_us_watchlist_suggest.py): pulls the current Futu
#     semiconductor sector, flags delisted/renamed dead tickers, and scores
#     candidates on 贵气+兑现 → us_watchlist_suggest_<date>.csv. PRINT-ONLY
#     (no --apply): it never edits select.yml here; promotion stays manual.
#   • News-driven top + launch detector (t_us_news_top_detector.py): 见顶方向
#     四类稻草 × 主语迁移 (Grok top_scan) × §2.1 exhaustion bar; 启动方向 (§6
#     镜像) 价格先行漏斗 (SP500∪NDX 关键K线进场筛选, 免费) → 前 N 名 Grok
#     launch_scan 四类火种 × 主语回归 → us_news_top_<date>.md. Weekly because
#     straw/spark news develops on the scale of weeks; the 14-day window with
#     the 7-day cadence overlaps by design (a persisting straw refreshes its
#     ledger episode instead of opening a new one). Costs ~$4/run (top) +
#     ~$1.2/run (launch topn=12) in Grok calls; degrades to price-only when
#     xAI is unreachable.
#   • Signal attribution (t_us_signal_attrib.py): forward outcomes for every
#     signal episode in us_signal_ledger.csv → us_signal_attrib_<date>.txt.
#     The weekly answer to "which signal type actually pays?"
#   • Tide gauge (t_us_tide_gauge.py): 潮位仪·真水量 — Fed H.4.1 净流动性
#     (WALCL−TGA−RRP, 免key官方zip) + 全市场总市值 (screener) + ETF 创赎流向
#     (前向积累快照) → us_tide_gauge_<date>.txt. 价值在水量潮向与价格潮位的
#     背离 (2022H1 型); H.4.1 每周四发布, 周频正好. 提示不门控 (registry #22).
#   • Weekly report merger (us_weekly_report.py): 汇总以上全部子报告为一份
#     us_weekly_report_<date>.md — 水文环境(潮/风)、稻草/火种、慢筛候选共振
#     计票、信号账本审计、周日复盘清单, 末尾附原文. Runs last so all inputs
#     exist. 日报管今天怎么动手, 周报管这周想什么.
#
# Both are non-fatal and degrade gracefully when OpenD is down (yfinance / dead-
# ticker validation only). The exit code tracks undervalue, the primary stage.
#
# Schedule via cron — Sundays 14:00 America/Los_Angeles (after Friday's close,
# before Monday). See the CRON block at the bottom.

set -u
set -o pipefail

REPO=/home/ryan/tushare_ryan
PY=/home/ryan/miniconda3/bin/python
LOG=$REPO/us_weekly_run.log

export PATH=/home/ryan/miniconda3/bin:/usr/local/bin:/usr/bin:/bin

cd "$REPO" || { echo "cannot cd to $REPO" >&2; exit 1; }

ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

run_step() {
    local name="$1"; shift
    echo "----- $name start $(ts) -----" >> "$LOG"
    "$@" >> "$LOG" 2>&1
    local r=$?
    echo "----- $name done (rc=$r) $(ts) -----" >> "$LOG"
    return $r
}

echo "===== us_weekly_run start $(ts) =====" >> "$LOG"

# Primary: undervalued-quality scan. Its rc drives the run's exit code.
run_step "undervalue" "$PY" t_us_undervalue.py
rc=$?

# Supplementary: watchlist suggestions (print-only, never --apply from cron).
run_step "watchlist-suggest" "$PY" t_us_watchlist_suggest.py

# Supplementary: news-driven top + launch detection (新闻见顶稻草 + 启动火种
# 扫描) — runs BEFORE signal-attrib so this week's straw/spark episodes are
# already in the ledger when attribution recomputes.
run_step "news-top" "$PY" t_us_news_top_detector.py

# Supplementary: signal attribution (信号归因) — recompute forward outcomes for
# every signal episode the daily scan has logged (us_signal_ledger.csv). Weekly
# because it's a slow feedback loop: the answer to "which signal type pays?"
# changes with sample size, not with days. Feeds the Sunday review.
run_step "signal-attrib" "$PY" t_us_signal_attrib.py

# Supplementary: tide gauge (潮位仪·真水量) — 净流动性/总市值/ETF创赎流向.
# 周频匹配 H.4.1 的周四发布节奏; 背离读数进周日复盘.
run_step "tide-gauge" "$PY" t_us_tide_gauge.py

# Supplementary: wind class (阵风/季风分类) — 相对线持续性×新鲜度 (价格端)
# + Grok 主导叙事前向日志 (~$0.1, 无 key 自动降级价格端 only). 季风以周为
# 最小分辨率; 叙事日志无法回填, 每周必跑.
run_step "wind-class" "$PY" t_us_wind_class.py

# Supplementary: swell (无风有涌) — 碾磨式跑赢 + Grok 查无催化剂 = SWELL 标注
# (~$0.8/run, 前 8 名; 无 key 降级纯价格候选). 标注不门控, 样本前向积累.
run_step "swell" "$PY" t_us_swell.py

# Final: merge every sub-report above into one combined weekly report
# (/home/ryan/DATA/result/weekly_report/us_weekly_report_<date>.md) — same
# pattern as us_daily_report.py in the daily runner. Extracts the actionable
# 风险/候选/复盘 items from each sub-report — annotated with source — plus a
# cross-screen confluence tally and the Sunday review checklist, then appends
# every sub-report verbatim. Runs last so all inputs exist. Non-fatal.
run_step "weekly-report" "$PY" us_weekly_report.py

if [ $rc -eq 0 ]; then
    echo "===== us_weekly_run OK    $(ts) =====" >> "$LOG"
else
    echo "===== us_weekly_run FAIL (rc=$rc) $(ts) =====" >> "$LOG"
fi

exit $rc

# ── CRON ──────────────────────────────────────────────────────────────────────
#   CRON_TZ=America/Los_Angeles
#   0 14 * * 0  /home/ryan/tushare_ryan/us_weekly_run.sh
