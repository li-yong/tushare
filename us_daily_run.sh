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
#   • Intraday internals checkup (t_us_intraday_internals.py): 持仓·分钟线派发
#     体检 (收盘判) — Futu OpenD 1m 增量缓存 (US_yf_1m/; yfinance 1m 额度不够,
#     2026-07-09 切换; OpenD down → 旧缓存降级), 每票算 SVR资金流/VWAP
#     下方时间/早盘30min + EXHAUSTION(放量跳空衰竭, 双向波动事件·复核触发器,
#     n=5 证伪其单独方向性) / INTERNALS_WEAK(资金流恶化) 两 flag → us_intraday_
#     internals/us_intraday_internals_<date>.{txt,csv}。CSV 序列 = 内部指标假说
#     (n=1, MU 6/25) 的前向样本库。不做盘中止损 (实证反对); 退出政策归 scanner
#     (ADR-0002)。见 docs/mu_1m_decline_study.md。
#   • Three-layer chain: t_us_premium → t_us_delivery → t_us_resonance
#     (贵气 × 兑现 × 技术觉醒 共振) → /home/ryan/DATA/result/us_{premium,delivery}_
#     <date>.csv + us_resonance_<date>.txt
#   • Broad consolidation-breakout screener (t_us_breakout_screen.py) across the
#     US large-cap universe → us_breakout_screen_<date>.{txt,csv}
#   • Steady-climber screener (t_us_steady_climb.py) across NASDAQ-100: smooth
#     low-vol uptrends that reclaim dips fast → us_steady_climb_<date>.{txt,csv}
#   • Out-of-pool searchlight (t_us_searchlight.py): scores NDX-100 names outside
#     the watchlist on 贵气×兑现 → us_searchlight_<date>.{txt,csv}
#   NOTE 2026-07: the entry-candidate scans below (key_kline / gap_scan /
#   pullback_shock / tr_surge / trend_confirm / bottom_entry) run on --universe
#   all = SP500 ∪ NDX-100 ∪ Russell-2000 Health Care+Technology ∪ mid-cap
#   $2B–$30B Health Care+Technology (~1290 names; R2000 slice = VTWO holdings ×
#   Nasdaq screener sectors; midht slice = screener cap band, plugs the
#   FROG-type gap between R2000's top and SP500's admission rules — see
#   t_us_undervalue load_universe). Ranking/environment reads (market_leaders, breadth rs-ref,
#   sector_rotation, network) stay on 'both' — small caps would distort their
#   fixed-rank/percentile logic, and the huice α findings are on the both pool.
#   • Key-K-line entry scan (t_us_key_kline.py --scan): across the all-pool,
#     ranks names sitting at a FRESH entry now (突破/初吻/PP) with live stop / 1R →
#     us_key_kline_scan_<date>.txt. The WHEN+WHERE timing pass over a quality pool
#     (NOT a free-market screener — 方法论 §2.1). yfinance-only, no Futu/OpenD.
#   • Gap scan (t_us_gap_scan.py) across the all-pool (SP500∪NDX∪R2000 HC+Tech), two passes: --scan
#     lists UP-gap entry candidates in two tiers with a reason annotation — Tier A
#     (fresh high-volume strong-close, tight stop) + Tier B (gap survived ≥5d
#     unfilled = market-confirmed, even if vol/close were weak); gap floor = stop
#     → us_gap_scan_<date>.{txt,csv}; --activity ranks names by valid-gap count
#     over 30 days (最活跃/最常跳空) → us_gap_activity_<date>.{txt,csv}.
#     yfinance-only, no Futu/OpenD.
#   • Pullback-shock screen (t_us_pullback_shock.py) across the all-pool (SP500∪NDX∪R2000 HC+Tech):
#     names in a strong uptrend (>200dma & 半年≥+20% & 距252日高≤15%) that just took
#     a big single-day drop — the empirically positive "buy the dip in a strong
#     name" TIMING (15y event study, 对 QQQ 超额为正、短期即翻正; 只做 A 侧, 不追
#     弱势股大涨). Tiered by drop severity (A ≤-7% / B -5%~-7%); entry=last close,
#     stop=急跌日低−0.5ATR, target=252日高 → us_pullback_shock_<date>.{txt,csv}.
#     yfinance-only, no Futu/OpenD.
#   • Low-bounce scan (t_us_low_bounce.py --scan): 250日新低·首日反弹 over the
#     all-pool — 新低≤2日前 + 单日收盘≥+7%, 三道流动性门(价≥$3/额≥$5M/量≥1.2×)
#     + 市值≥$300M(Nasdaq screener 当日缓存, 挂了则跳过该门)。pullback_shock 的
#     光谱对端(弱势里的强一天); 候选发现层非验证信号源, 参考止损=刚砸出的250日低
#     → us_low_bounce/us_low_bounce_<date>.{txt,csv}. yfinance-only, no Futu/OpenD.
#   • Earnings-react scan (t_us_earnings_react.py --scan): 财报强反应·守住 over
#     the all-pool — 财报bar≤5交易日前 + 财报日(E)或次日(E+1)收盘涨幅≥7% + 最新
#     收盘仍≥close(财报日-1)。tech_swing 静默期规则里"财报后跳空确认是更好的入场"
#     的那半句, 独立成扫描; 参考止损 = close(财报日-1), 收盘跌破=强反应被完全回吐。
#     价格漏斗先行(只有近窗口≥7%大阳的名才打财报日历 API)。候选发现层, 无 huice
#     检验(财报日历非点位数据) → us_earnings_react/us_earnings_react_<date>.
#     {txt,csv}. yfinance-only, no Futu/OpenD.
#   • TR-surge scan (t_us_tr_surge.py --scan): 近3日真实波幅(TR%)持续放大·高位区 —
#     每日TR%≥3 且合计>15 且收盘距45日高≤15% (高位过滤排除深跌途中的下坡波动;
#     TR度量 close-based 抓不到, 如 MU 2026-04-02 单日-0.44%但TR 7.5)。
#     Volatility STATE read, not a
#     buy/sell signal: report splits hits 上行/下行/拉锯 by net_chg% and points
#     to gap_scan / pullback_shock / key_kline for the actual entry decision →
#     us_tr_surge/us_tr_surge_<date>.{txt,csv}. yfinance-only, no Futu/OpenD.
#   • Surge-stopline monitor (t_us_surge_stopline.py): 持仓·surge日收盘失守警报 —
#     TR≥7% 阳线关键日的收盘为 stopline (新线永远替换旧线), 收盘跌破 = ALERT
#     (最后一根响亮上攻的成果被全部回吐 = 假突破/顶部第一声裂响)。CSCO 2000 验证:
#     顶后2日(-5%)即报警 (L2 20周线是-28.8%), 但牛市假警报密 (NVDA ~37次/3.5年)
#     → 定位 = 复核/减仓评估触发器, NOT a stop engine (退出纪律归 ADR-0002 分层
#     止损); 响亮的顶归它, 温水煮青蛙的安静顶归20周线。持仓池 = US_SWING_STOPS ∪
#     US_HOLD_EXTRA → us_surge_stopline/us_surge_stopline_<date>.{txt,csv}.
#     yfinance-only, no Futu/OpenD.
#   • Trend confirm (t_us_trend_confirm.py --scan): in_trend 三条件翻转扫描
#     (>200d & 半年+20% & 距252高≤15%, 复用 pullback_shock.annotate)。CONFIRM=
#     回调结束确认(进汇总计票) / LOST=跌出强趋势态(持仓★进持仓管理提示, 非卖出)
#     → us_trend_confirm/us_trend_confirm_<date>.{txt,csv}。yfinance-only.
#   • Bottom-entry screen (t_us_bottom_entry.py) across the all-pool (SP500∪NDX∪R2000 HC+Tech):
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
#   • Market leaders (t_us_market_leaders.py): 个股层面持续领跑确认 — sector_rotation
#     的个股配对。trailing 5日窗口, 在 SP500∪NDX 池按当日涨幅固定名次(top/bottom 20)
#     排名, 触发 = top≥2 且 bottom≤1(允许1天噪声); 比单日涨幅榜抗噪(2025-11 MU/SNDK
#     两边打脸期已知假阳性, 未加量能门槛)→ us_market_leaders/us_market_leaders_
#     <universe>_<date>.{txt,csv}. Environment/confirmation read, NOT a buy/sell
#     signal source. yfinance-only, no Futu/OpenD.
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

# Supplementary: intraday internals checkup (持仓·分钟线派发体检, 收盘判) —
# 1m internals (SVR/VWAP-time/f30) + EXHAUSTION/INTERNALS_WEAK flags over the
# full holdings pool (US_SWING_STOPS ∪ US_HOLD_EXTRA). Review trigger, NOT a
# sell signal (n=5 falsified the daily pattern's direction); the dated CSVs
# accumulate the forward sample for the n=1 internals hypothesis. Runs at
# 17:00 ET so today's 1m session is complete. 1m bars from Futu OpenD
# (yfinance quota exhausted); OpenD down → stale 1m cache, never hangs
# (socket pre-check). Daily context stays on the yfinance cache. Non-fatal.
echo "----- intraday internals start $(ts) -----" >> "$LOG"
II_TXT=/home/ryan/DATA/result/us_intraday_internals/us_intraday_internals_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$II_TXT")"
"$PY" t_us_intraday_internals.py 2>> "$LOG" | tee "$II_TXT" >> "$LOG"
ii_rc=${PIPESTATUS[0]}
echo "----- intraday internals done (rc=$ii_rc) $(ts) -----" >> "$LOG"

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

# Supplementary: key-K-line entry scan (择时 + 止损) over the all-pool (SP500∪NDX∪R2000 HC+Tech).
# Ranks names at a fresh entry right now (突破/初吻/PP) with live stop / 1R; the
# table is teed to a dated report. No --plot-top in cron (avoid a daily flood of
# PNGs in result/ — plot survivors by hand). yfinance-only; no Futu dependency.
# Non-fatal — its rc never flips the run's exit code.
echo "----- key-kline scan start $(ts) -----" >> "$LOG"
KK_TXT=/home/ryan/DATA/result/us_key_kline/us_key_kline_scan_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$KK_TXT")"
"$PY" t_us_key_kline.py --scan --universe all 2>> "$LOG" | tee "$KK_TXT" >> "$LOG"
kk_rc=${PIPESTATUS[0]}
echo "----- key-kline scan done (rc=$kk_rc) $(ts) -----" >> "$LOG"

# Supplementary: gap scan (跳空缺口) over the all-pool (SP500∪NDX∪R2000 HC+Tech). Two passes, each
# tee'd to a dated report; both write their own CSV. --scan = 近期放量、收强、仍
# 未回补的向上缺口(可操作, 缺口下沿做天然止损); --activity = 缺口活跃度排行(近
# 30 日有效缺口次数, 找最常跳空的高波动名)。yfinance-only; no Futu dependency.
# Non-fatal — their rc never flips the run's exit code.
echo "----- gap scan start $(ts) -----" >> "$LOG"
GAP_TXT=/home/ryan/DATA/result/us_gap_scan/us_gap_scan_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$GAP_TXT")"
"$PY" t_us_gap_scan.py --scan --universe all 2>> "$LOG" | tee "$GAP_TXT" >> "$LOG"
gap_rc=${PIPESTATUS[0]}
echo "----- gap scan done (rc=$gap_rc) $(ts) -----" >> "$LOG"

echo "----- gap activity start $(ts) -----" >> "$LOG"
GAPA_TXT=/home/ryan/DATA/result/us_gap_scan/us_gap_activity_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$GAPA_TXT")"
"$PY" t_us_gap_scan.py --activity --universe all --days 30 2>> "$LOG" | tee "$GAPA_TXT" >> "$LOG"
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
"$PY" t_us_pullback_shock.py --scan --universe all 2>> "$LOG" | tee "$PS_TXT" >> "$LOG"
ps_rc=${PIPESTATUS[0]}
echo "----- pullback-shock done (rc=$ps_rc) $(ts) -----" >> "$LOG"

# Supplementary: low-bounce scan (250日新低·首日反弹) over the all-pool. The
# spectrum-opposite of pullback_shock: a fresh 250d low (≤2d ago) followed by a
# ≥7% up close — the first fighting-back bar after the last leg down. Gated by
# price ≥$3 / 20d dollar-vol ≥$5M / vol ≥1.2×20d / mktcap ≥$300M (Nasdaq
# screener, day-cached, degrades to no-cap-gate). Candidate DISCOVERY layer,
# NOT a proven signal source (extreme win/loss dispersion; quality gates are
# left-tail filters per docs §7.5-7.6); ref stop = the just-made 250d low.
# Reports a trailing 5-day window (the pattern is sparse; today's hits are ★).
# --grok: each new hit gets ONE Grok catalyst check (~$0.1, x_search+web_search;
# deduped by ticker×signal-date in grok_catalyst.csv) — annotation-only forward
# sample accumulation for the n=1 hypothesis "catalyst present = real reversal
# (VSTM) vs no chatter = dead-cat (SSTK)"; NOT a gate. Degrades to no-annotation
# without xAI key. yfinance + xAI, no Futu/OpenD. Non-fatal.
echo "----- low-bounce start $(ts) -----" >> "$LOG"
LB_TXT=/home/ryan/DATA/result/us_low_bounce/us_low_bounce_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$LB_TXT")"
"$PY" t_us_low_bounce.py --scan --universe all --grok 2>> "$LOG" | tee "$LB_TXT" >> "$LOG"
lb_rc=${PIPESTATUS[0]}
echo "----- low-bounce done (rc=$lb_rc) $(ts) -----" >> "$LOG"

# Supplementary: earnings-react scan (财报强反应·守住, PEAD confirmation
# candidates) over the all-pool. Three gates: earnings bar ≤5 trading days ago;
# E or E+1 close-to-close gain ≥7% (covers pre-open AND after-close reporters);
# latest close still ≥ close(ER-1) — the "财报后跳空确认" entry the tech_swing
# blackout rule points at but nothing scanned for. Reference stop = close(ER-1),
# daily-close judged (ADR-0002). Price funnel first (only ≥7% movers hit the
# yfinance earnings-calendar API, so the all-pool sweep stays cheap). Candidate
# DISCOVERY layer, not a proven source; replayable via huice --source
# earnings_react (past announcement dates are facts — the caveat is delisted
# names lacking calendars, i.e. survivorship); dated CSVs accumulate the live
# forward sample.
# Table tee'd to a dated report. yfinance-only, no Futu/OpenD. Non-fatal.
echo "----- earnings-react start $(ts) -----" >> "$LOG"
ER_TXT=/home/ryan/DATA/result/us_earnings_react/us_earnings_react_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$ER_TXT")"
"$PY" t_us_earnings_react.py --scan --universe all 2>> "$LOG" | tee "$ER_TXT" >> "$LOG"
er_rc=${PIPESTATUS[0]}
echo "----- earnings-react done (rc=$er_rc) $(ts) -----" >> "$LOG"

# Supplementary: TR-surge scan (近3日真实波幅持续放大·高位区) over S&P 500 ∪
# Nasdaq-100. Flags names whose True Range %(vs prev close) ran ≥3% EVERY day
# for 3 days, summed >15%, AND closed within 15% of the 45-day high (filters
# out downhill volatility deep in a decline) — continuous repricing/
# distribution/tug-of-war, invisible to close-to-close measures.
# Volatility STATE read, NOT a signal source; the report's 解读 section splits
# hits by direction (net_chg%) and routes to gap_scan/pullback_shock/key_kline.
# Table tee'd to a dated report; CSV written by the script. yfinance-only,
# no Futu/OpenD. Non-fatal — its rc never flips the run's exit code.
echo "----- tr-surge start $(ts) -----" >> "$LOG"
TRS_TXT=/home/ryan/DATA/result/us_tr_surge/us_tr_surge_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$TRS_TXT")"
"$PY" t_us_tr_surge.py --scan --universe all 2>> "$LOG" | tee "$TRS_TXT" >> "$LOG"
trs_rc=${PIPESTATUS[0]}
echo "----- tr-surge done (rc=$trs_rc) $(ts) -----" >> "$LOG"

# Supplementary: surge-stopline monitor (持仓·surge日收盘失守警报). The sell-side
# key-bar mirror: the close of the latest TR≥7% up surge bar is the stopline
# (always replaced by a newer surge bar); a daily close below it = ALERT — the
# last loud advance fully given back (failed breakout / first crack of a loud
# top). CSCO-2000 validated (alert at -5% from top vs -28.8% for the 20w line);
# whipsaw-prone in bulls, so it is a REVIEW/TRIM trigger for holdings, NOT a
# stop engine (exit discipline stays with the layered stops, ADR-0002). Pool =
# US_SWING_STOPS ∪ US_HOLD_EXTRA. Table tee'd to a dated report; CSV written by
# the script. yfinance-only, no Futu/OpenD. Non-fatal.
echo "----- surge-stopline start $(ts) -----" >> "$LOG"
SS_TXT=/home/ryan/DATA/result/us_surge_stopline/us_surge_stopline_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$SS_TXT")"
"$PY" t_us_surge_stopline.py 2>> "$LOG" | tee "$SS_TXT" >> "$LOG"
ss_rc=${PIPESTATUS[0]}
echo "----- surge-stopline done (rc=$ss_rc) $(ts) -----" >> "$LOG"

# Supplementary: trend confirm (in_trend 三条件翻转) over the all-pool (SP500∪NDX∪R2000 HC+Tech)
# ∪ holdings. Reuses pullback_shock's annotate (>200d & 半年+20% & 距252高≤15%,
# single source of truth). CONFIRM = 回调结束确认 (MU 2026-04-08 型, 确认日多为
# 大阳=内生确认成本, 进 daily-report 候选计票); LOST = 跌出强趋势态 (持仓★进
# 持仓管理提示; 质地降级非卖出, 脊梁归20周线/分层止损, ADR-0002). 状态读数
# 非指令。yfinance-only, no Futu/OpenD. Non-fatal.
echo "----- trend-confirm start $(ts) -----" >> "$LOG"
TC_TXT=/home/ryan/DATA/result/us_trend_confirm/us_trend_confirm_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$TC_TXT")"
"$PY" t_us_trend_confirm.py --scan --universe all 2>> "$LOG" | tee "$TC_TXT" >> "$LOG"
tc_rc=${PIPESTATUS[0]}
echo "----- trend-confirm done (rc=$tc_rc) $(ts) -----" >> "$LOG"

# Supplementary: bottom-entry screen (超跌底部入场) over the all-pool (SP500∪NDX∪R2000 HC+Tech).
# Lists names at a "超跌 + 跌破20周线(线下)" entry state now, tiered by SEC-EDGAR
# point-in-time ROE (PASS 防归零优先 / FAIL 高弹性需查结构性受损 / UNKNOWN). Writes
# its own dated md+csv to result/us_bottom_entry/. Bound to the 20-week system §7;
# quality is a left-tail filter not a ranker. yfinance + SEC EDGAR; no Futu/OpenD.
# Note: PIT-ROE cache (backtest_cache/pit_quality.pkl) is built once and reused —
# refresh it periodically (run with --force) to pick up new 10-K filings.
# Non-fatal (run_step) — its rc never flips the run's exit code.
run_step "bottom-entry" "$PY" t_us_bottom_entry.py --universe all

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

# Supplementary: sector rotation (板块/主题轮动 发现层 — breadth diffusion 的配对
# 发现器). Layer 1: ~20 sector/theme ETFs ranked by weighted relative-strength
# vs SPY (skyte weights on the RS line) + state tag (领跑/转强/转弱/落后). Layer 2:
# top-3 get a breadth-diffusion confirmation (members = SP500∪NDX pool filtered
# by cached yfinance GICS metadata, 30-day TTL at pickle/us_sector_industry.json;
# benchmark = the ETF itself) to separate real diffusion from single-megacap
# fake leadership. Runs after breadth so the both-pool bar cache is warm.
# Environment read, NOT a buy/sell signal source. Non-fatal (run_step).
run_step "sector-rotation" "$PY" t_us_sector_rotation.py

# Supplementary: market leaders (个股层面持续领跑确认 — sector_rotation 的个股配对).
# Trailing 5日窗口, 在 SP500∪NDX 池按当日涨幅固定名次(top/bottom 20)排名, 触发 =
# top≥2 且 bottom≤1(允许1天噪声) — 比单日涨幅榜抗噪(2025-11 MU/SNDK 两边打脸期已知
# 假阳性, 尚未加量能门槛)。Runs after sector-rotation so the both-pool bar cache is
# warm. Environment/confirmation read, NOT a buy/sell signal source. yfinance-only,
# no Futu/OpenD. Table tee'd to a dated report; CSV written by the script itself.
# Non-fatal — its rc never flips the run's exit code.
echo "----- market leaders start $(ts) -----" >> "$LOG"
ML_TXT=/home/ryan/DATA/result/us_market_leaders/us_market_leaders_$(date +%Y%m%d).txt
mkdir -p "$(dirname "$ML_TXT")"
"$PY" t_us_market_leaders.py --scan --universe both 2>> "$LOG" | tee "$ML_TXT" >> "$LOG"
ml_rc=${PIPESTATUS[0]}
echo "----- market leaders done (rc=$ml_rc) $(ts) -----" >> "$LOG"

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

# 时间预算回填 (registry #26 复盘闭环): 对预算期满 (信号日体制 STRONG 20 /
# MIXED 10 / WEAK 5 个交易日) 的旧 us_daily_report 追加 ⏳复核段 — 该日 ledger
# 里每个触发 ticker 的 没动/温吞/已启动/破止损 判定。幂等 (标记注释), 未熟
# 自动跳过下次补; 放 daily-report 之后保证当日报告先落盘。
run_step "time-budget-annotate" "$PY" t_us_time_budget_annotate.py

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
