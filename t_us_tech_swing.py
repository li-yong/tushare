# coding: utf-8
"""
US Tech Swing Trade Scanner  (session 81 framework)

4-layer medium-term trading system for Mag7 + Semiconductors:
  Layer 1 — Market state  : QQQ + SOXX vs 20-week MA → STRONG / MIXED / WEAK
                            then demote one notch if ≥half the generals (MAG7)
                            are below their own 20wMA (leadership-breadth gate)
  Layer 2 — Entry signal  : breakout (STRONG) or pullback to MA (WEAK);
                            MIXED = no new entry (过渡期不开新仓, huice 回测唯一
                            稳定负期望口袋 — setups shown as watch-only; --no-mixed-gate 可关)
  Layer 3 — Pre-trade setup: entry / stop (technical) / target / R:R ≥ 2:1
  Layer 4 — Position mgmt : fast risk control on DAILY bars first — Layer 0
                            hard-stop (crash) and Layer 1 event-driven drop take
                            over and pre-empt the slow weekly rules; otherwise
                            breakeven stop at +30%, trim only on weakness
                            (≥+25% AND weekly close < 10-week MA), exit on 20wMA breach

20-week trend system (docs/twenty_week_trend_system.md) — layered stop discipline,
priority high→low; a higher layer, once it fires, takes over and skips the rest:
  Layer 0  熔断/硬止损   — collapse (peak→latest ≤ -30% in ≤5 sessions). Act now,
                          never wait for the weekly close.
  Layer 1  事件驱动      — abnormal drop (≤ -20% or > Nσ single day). Investigate now.
  Layer 2  20-week MA    — normal trend reversal, evaluated only at the weekly close.
  慢趋势用均线管, 快暴跌用硬止损管 (20周线管"温水", 硬止损管"开水").

AI capex transmission chain (leading indicator for semis):
  Hyperscaler Capex ↑ → NVDA → AVGO → AMAT/LRCX/KLAC → TSM

Usage:
  python t_us_tech_swing.py                  # full scan + Futu positions
  python t_us_tech_swing.py --no-futu        # skip Futu (market closed / data only)
  python t_us_tech_swing.py --ticker NVDA    # single ticker
  python t_us_tech_swing.py --ticker MU --asof 2025-05-01   # 回测: 当时的 report
                                             # --asof 只用 ≤该日的 bar, 锚"今天"到该日,
                                             # 自动 --no-futu, 不查财报; 不传则行为不变。
"""

import sys
import os
import json
import logging
import datetime
import traceback

import pandas as pd
import numpy as np
import tabulate as tab_mod
import yfinance as yf
from optparse import OptionParser

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
# suppress yfinance noise
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# ── Universe (hand-curated watchlist, loaded from select.yml) ──────────────────
# Holdings are NOT here — they come live from Futu (see CONTEXT.md).
WATCHLIST_FILE = '/home/ryan/tushare_ryan/select.yml'

# Defaults double as the fallback if select.yml is missing/unreadable.
MAG7 = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'AMZN', 'TSLA']
SEMIS = ['AMD', 'AVGO', 'AMAT', 'ASML', 'LRCX', 'KLAC', 'TSM']
AI_CHAIN = ['LITE', 'SNDK', 'STX', 'WDC', 'CSCO']  # AI datacenter buildout beyond chips
HYPERSCALERS = ['MSFT', 'GOOGL', 'META', 'AMZN']
BAROMETERS = ['QQQ', 'SOXX']                  # QQQ = Nasdaq, SOXX = semis ETF
ACCOUNT_EQUITY = 0.0                          # hand-maintained, loaded from select.yml
INIT_STOPS: dict = {}                         # ticker → initial technical stop, from
                                              # select.yml US_SWING_STOPS (Layer 1.5)
PRINCIPAL: dict = {}                          # 主要矛盾一句话 {date, text}, from
                                              # select.yml US_PRINCIPAL_CONTRADICTION
FLIP_NOTES: dict = {}                         # ticker → 易位判据一句话, from
                                              # select.yml US_SWING_FLIP


def _load_watchlist():
    """Load the swing watchlist + account equity from select.yml; fall back to defaults.

    YAML entries may be bare tickers or `TICKER: label` maps — we take the
    ticker either way, so the same loader works across select.yml's styles.
    """
    global MAG7, SEMIS, AI_CHAIN, HYPERSCALERS, BAROMETERS, ACCOUNT_EQUITY, INIT_STOPS
    global PRINCIPAL, FLIP_NOTES
    try:
        import yaml
        with open(WATCHLIST_FILE) as fh:
            cfg = yaml.safe_load(fh) or {}

        def _tickers(key, default):
            rows = cfg.get(key)
            if not rows:
                return default
            out = []
            for row in rows:
                out.append(next(iter(row)) if isinstance(row, dict) else row)
            return [str(t).upper() for t in out]

        MAG7 = _tickers('US_SWING_MAG7', MAG7)
        SEMIS = _tickers('US_SWING_SEMIS', SEMIS)
        AI_CHAIN = _tickers('US_SWING_AI_CHAIN', AI_CHAIN)
        HYPERSCALERS = _tickers('US_SWING_HYPERSCALERS', HYPERSCALERS)
        BAROMETERS = _tickers('US_SWING_BAROMETERS', BAROMETERS)
        ACCOUNT_EQUITY = float(cfg.get('US_SWING_EQUITY', 0) or 0)
        # Layer 1.5: the setup's technical stop, recorded by hand when the trade
        # is taken (the broker can't tell us; a stop is a trader decision, not
        # position state). It stays live until breakeven / the 20wMA overtake it.
        raw_stops = cfg.get('US_SWING_STOPS') or {}
        INIT_STOPS = {str(t).upper(): float(v) for t, v in raw_stops.items() if v}
        PRINCIPAL = cfg.get('US_PRINCIPAL_CONTRADICTION') or {}
        raw_flip = cfg.get('US_SWING_FLIP') or {}
        FLIP_NOTES = {str(t).upper(): str(v).strip() for t, v in raw_flip.items() if v}
    except Exception as e:
        logging.warning(f'watchlist load failed ({e}) — using built-in defaults')


def _position_size(entry, stop, equity):
    """Shares to buy so a stop-out loses ~1 R (1% of equity), capped at 25%/name.

    Returns (shares, risk_dollars, cap_bound). shares=0 when equity is unset
    or the entry/stop is unusable.
    """
    if not equity or equity <= 0 or entry is None or stop is None:
        return 0, 0.0, False
    risk_per_share = entry - stop
    if risk_per_share <= 0:
        return 0, 0.0, False
    r_dollars   = equity * RISK_PCT
    risk_shares = int(r_dollars // risk_per_share)
    cap_shares  = int((equity * MAX_POSITION_PCT) // entry)
    shares      = min(risk_shares, cap_shares)
    return max(shares, 0), round(r_dollars, 0), shares == cap_shares < risk_shares


_load_watchlist()
UNIVERSE = list(dict.fromkeys(MAG7 + SEMIS + AI_CHAIN))  # deduplicated, order preserved

# ── Parameters ────────────────────────────────────────────────────────────────
MA_WEEKLY         = 20      # 20-week MA for market-state and exit rule
LEADERSHIP_BREACH_FRAC = 0.5  # ≥this fraction of leaders (MAG7) below their 20wMA
                              # → demote scan aggressiveness regardless of QQQ/SOXX
STATE_BAND_PCT    = 0.03    # market-state hysteresis: a barometer/general within
                            # ±3% of its 20wMA keeps its previous side — only a
                            # close beyond the band edge flips it. QQQ and SOXX
                            # chop across the raw line for weeks in a range market;
                            # without the band the 3-state machine (and the entry
                            # mode with it) whipsaws. Same idea as the exit rule's
                            # buffer_pct (twenty_week_trend_system §2.2), applied
                            # to Layer 1. Holdings' exit rule is NOT banded here.
# ── 体制联动 (两台状态机打通, 2026-07-04) ──────────────────────────────────────
# t_us_regime_monitor (SPY 200日线 DEFEND + 领先共振 WATCH + 熊反免疫期) 每日 cron
# 写机读快照; 本扫描器开扫前读它做入场侧门控:
#   DEFEND        → 所有新仓 Setup 降级为观察行 (⛔体制DEFEND)
#   WATCH / 免疫期 → 扫描模式降一档 (STRONG→MIXED→WEAK; 配合 MIXED 门控 = 不追高)
# 依据: huice 2022 回测 — 熊反里 STRONG 标签胜率最低 (10-17%), 2022-08 熊反 SPY 从未
# 连续站回 200 日线 (免疫期全程生效可挡)。快照缺失/过期 → 优雅降级不挡扫描;
# --asof 回测不读 (live 快照对历史日期是未来函数); --no-regime-gate 可关。
# 只动入场侧 — 退出信号/持仓管理与体制无关 (纪律层不受门控影响)。
REGIME_STATE_FILE = '/home/ryan/DATA/result/us_regime_monitor/us_regime_state.json'
REGIME_MAX_AGE_D  = 5       # 快照超过 N 个自然日视为过期 (容周末+假日)
REGIME_GATE       = True    # --no-regime-gate 置 False
_REGIME_SUPPRESS  = False   # 本次运行 DEFEND 抑制生效 (main 设置, scan_stock 读)
REGIME_IMMUNITY_DEMOTE = False
# ↑ 熊反免疫期是否硬降档。验证 (episode × 前日体制标签 join, 2022+2025-26 大池):
#   DEFEND 抑制 ✓ (2022 挡掉均 -0.41R×6915 单, 2025 放弃 +2.60R×622 单, 净省 ~+1200R)
#   WATCH 降档  ✓ (净省 ~+835R)
#   免疫期硬降档 ✗ 净 -891R: 2022 熊反免疫窗的单均 -0.86R (挡对), 但 2025 V 复苏
#   免疫窗的单均 +2.08R 且以追高型领涨 (挡错, 恰是全期最好的一批) — 没有任何类型
#   切分能实时区分"熊反 vs 真复苏"。故免疫期默认只作报告 ⚠ 注记不动扫描, 想硬门控
#   置 True。全文 docs/huice_backtest_findings.md §2.8。

PRINCIPAL_STALE_D = 10      # 主要矛盾判断的保质期(天)。周日复盘更新, 周频+缓冲;
                            # 超期 = 认识落后于实际的右倾风险, 晨报高亮但不门控
                            # (与 HMM 第二意见同定位: 提示层, 手工判断不可程序化裁决)。
MIXED_NO_NEW_ENTRY = True   # MIXED (过渡期) 不开新仓。huice 指示级回测
                            # (docs/huice_backtest_findings.md §2.4): MIXED 是两个池、
                            # 两个体制下唯一稳定负期望的口袋 (SP500∪NDX meanR -0.70,
                            # PULLBACK/FIRST_KISS MIXED 两池皆负)。只抑制新入场 Setup
                            # (降级为观察行, ⛔ 注记); 退出信号/持仓管理不受影响。
                            # --no-mixed-gate 恢复旧行为 (回测对照用)。
CONSOLIDATION_W   = 10      # look-back window for breakout range
VOL_BREAKOUT_MULT = 1.5     # breakout volume must be ≥ 1.5× 5-week avg
PULLBACK_TOL      = 0.015   # price within 1.5% of MA counts as "touching"
PULLBACK_STOP_PCT = 0.03    # stop placed 3% below the support MA
MIN_RR            = 2.0     # minimum acceptable risk:reward ratio
MA_WEEKLY_FAST    = 10      # 10-week MA: momentum line gating the partial trim
BREAKEVEN_PCT     = 30.0    # lock stop to breakeven only after a real cushion.
                            # +30% (was 15): a pullback to cost is then a ~23%
                            # reversal, not normal chop — a normal pullback can't
                            # shake a working winner out flat before it gaps.
TRIM_PCT          = 25.0    # min gain before a WEAKNESS-triggered partial trim
                            # (trim on momentum cracking, never at a price target —
                            #  most return lives in rare explosive continuation; see
                            #  return-structure finding / us_return_concentration)
LAYOUT_DAYS_MIN   = 14      # earnings layout window: 2 weeks out
LAYOUT_DAYS_MAX   = 28      # earnings layout window: 4 weeks out

# Key-level stop (breakout): place the stop under the nearest real support
# below entry instead of at the deep consolidation low — tightens R:R honestly.
KEY_PIVOT_WIN     = 3       # a swing low = local min over ±3 daily bars
KEY_LOOKBACK_D    = 120     # search swing lows within ~6 months
KEY_CLUSTER_PCT   = 0.015   # swing lows within 1.5% collapse into one level
KEY_STOP_MIN_PCT  = 0.02    # stop must sit ≥2% below entry (avoid whipsaw)
KEY_STOP_MAX_PCT  = 0.15    # ignore support more than 15% below entry (too wide)

# Gap-confirmation 信噪比 filter (see memory: overnight-vs-intraday-information).
# QQQ 27y: overnight 段(昨收→今开) carries ~all the drift, the intraday 段
# (今开→今收) carries 73% of the variance but negative drift = noise. So a fresh
# gap only counts as信息 when the close holds the open (高开站稳 / 低开压住);
# 高开低走 is gap-fill 噪声. Latest-bar flag only — annotates, never gates.
GAP_MIN_PCT       = 0.3     # |overnight gap| below this (%) is noise, not a real gap
# Gap-strength tiers, calibrated on SP500∪NDX 5y (us_return_concentration):
# P(day is a top-1% move | gap in bucket) — 弱 +2% lift 7x · 确认 +3% lift 17x ·
# 强 +5% lift 48x. <2% is the de-noise floor (≈base rate), not an action signal.
# Tier ranks magnitude; it does NOT override the close-holds-open confirmation.
GAP_WEAK_PCT      = 2.0
GAP_CONFIRM_PCT   = 3.0
GAP_STRONG_PCT    = 5.0

# Position sizing (CONTEXT.md: R = 1% of equity; cap 25% of equity per name)
RISK_PCT          = 0.01    # 1 R = 1% of account equity
MAX_POSITION_PCT  = 0.25    # a single position's notional ≤ 25% of equity
STOP_NEAR_PCT     = 0.03    # holding "approaching stop" if within 3% above it

# Portfolio open heat: the loss if every holding's effective stop is hit the
# same day. The pool is one highly-correlated theme, so that is the realistic
# unit of risk — 5 nominal positions ≈ 1 theme position; budget at theme level.
HEAT_CAP_PCT      = 0.06    # total open risk ≤ 6% of equity

# Earnings blackout: a weeks-to-months holding in semis eats ~4 reports a year,
# and most Layer-1 (-20% day) events ARE earnings. No new entry into the event;
# let the report confirm first (post-ER gap-confirmation is the better entry).
ER_BLACKOUT_D     = 5       # no new entry when earnings are ≤ N days away

# ── Layered fast risk control (20-week trend system · Layers 0 & 1) ────────────
# The 20wMA exit (Layer 2) is evaluated at the weekly close and is deliberately
# slow — it manages 温水. A collapse or a fundamental break is 开水: it is its own
# conclusion and must NOT wait for Friday. These two layers run on DAILY bars for
# live holdings and take priority over the weekly rule — once they fire they take
# over and skip the slower checks. See docs/twenty_week_trend_system.md.
HARD_STOP_PCT      = 30.0   # Layer 0 熔断: peak→latest decline (≤window) this deep
                            # = collapse; act now, do not wait for the weekly close.
EVENT_DROP_PCT     = 20.0   # Layer 1 事件: abnormal decline → go investigate the cause
EVENT_VOL_MULT     = 3.0    # …or a single-day move worse than N× trailing daily σ
CRASH_WINDOW_D     = 5      # "单日或数日内": measure the drop over the last N sessions
EVENT_VOL_LOOKBACK = 60     # trailing window for the daily-return σ baseline

# ── Data layer (ADR-0001: yfinance is the sole bar source) ─────────────────────
BAR_CACHE_DIR     = '/home/ryan/DATA/DAY_Global/US_yf'  # one split/div-adjusted CSV per ticker
BAR_FETCH_PERIOD  = '3y'    # generous window cached once; callers slice from it
_DAILY_MEMO: dict = {}      # per-run memo: ticker -> daily df (avoids re-fetching)

# Point-in-time backtest anchor. None = live (今天). When set (via --asof), the
# data layer truncates every series to ≤ _ASOF and all "today"-relative windows
# anchor here, so the scanner reproduces what it would have reported on that day.
# None → behaviour is byte-for-byte the live path.
_ASOF: 'pd.Timestamp | None' = None


def _now() -> 'pd.Timestamp':
    """The run's reference 'today' — _ASOF when backtesting, else the real today."""
    return _ASOF if _ASOF is not None else pd.Timestamp.today().normalize()


def _cache_path(ticker: str) -> str:
    return os.path.join(BAR_CACHE_DIR, f'{ticker}.csv')


def _cache_is_fresh(path: str) -> bool:
    """Fresh = file written today. Daily scan runs once after the close."""
    if not os.path.exists(path):
        return False
    mtime = datetime.date.fromtimestamp(os.path.getmtime(path))
    return mtime == datetime.date.today()


def _read_cache(ticker: str) -> pd.DataFrame:
    path = _cache_path(ticker)
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=['date'])
    return df.set_index('date')


def _fetch_daily(ticker: str) -> pd.DataFrame:
    """
    Daily OHLCV (yfinance, cached). In live mode this is the full cached frame;
    under --asof it is truncated to bars ≤ _ASOF so no future leaks into a
    point-in-time run. The disk cache itself always holds the full history.
    """
    df = _fetch_daily_full(ticker)
    if _ASOF is not None and not df.empty:
        return df[df.index <= _ASOF]
    return df


def _fetch_daily_full(ticker: str) -> pd.DataFrame:
    """
    Daily OHLCV from yfinance, split/dividend-adjusted, cached per ticker.

    ADR-0001: yfinance is the only source. On a successful pull the cache is
    rewritten; when Yahoo is unreachable we serve the last-good cache with a
    loud staleness warning rather than splicing in another (differently
    adjusted) source.
    """
    if ticker in _DAILY_MEMO:
        return _DAILY_MEMO[ticker]

    path = _cache_path(ticker)

    # Already pulled today — trust the cache, skip the network.
    if _cache_is_fresh(path):
        df = _read_cache(ticker)
        _DAILY_MEMO[ticker] = df
        return df

    try:
        raw = yf.Ticker(ticker).history(period=BAR_FETCH_PERIOD, auto_adjust=True)
        if raw.empty:
            raise ValueError('empty frame from yfinance')
        df = raw.rename(columns={
            'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume',
        })[['open', 'high', 'low', 'close', 'volume']].copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = 'date'
        df = df.dropna(subset=['close'])
        os.makedirs(BAR_CACHE_DIR, exist_ok=True)
        df.reset_index().to_csv(path, index=False)
        _DAILY_MEMO[ticker] = df
        return df
    except Exception as e:
        stale = _read_cache(ticker)
        if stale.empty:
            logging.error(f'{ticker}: yfinance failed ({e}) and NO cache exists — skipping')
            _DAILY_MEMO[ticker] = stale
            return stale
        last = stale.index[-1].date()
        age = (datetime.date.today() - last).days
        logging.warning(
            f'{ticker}: yfinance failed ({e}) — SERVING STALE CACHE, '
            f'last bar {last} ({age}d old)'
        )
        _DAILY_MEMO[ticker] = stale
        return stale


# ── Earnings calendar (yfinance get_earnings_dates, cached per ticker) ────────
EARN_CACHE_DIR   = '/home/ryan/DATA/DAY_Global/US_earnings'  # one JSON per ticker
EARN_CACHE_TTL_D = 3    # past dates never change; upcoming ones occasionally get
                        # rescheduled — a short TTL bounds that drift while still
                        # deduping calls across scripts/reruns
_EARN_MEMO: dict = {}   # per-run memo: ticker -> rows (avoids re-reading disk)


def _earn_cache_path(ticker: str) -> str:
    return os.path.join(EARN_CACHE_DIR, f'{ticker}.json')


def _read_earn_cache(ticker: str) -> 'list | None':
    path = _earn_cache_path(ticker)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as fh:
            payload = json.load(fh)
        return [{'date': pd.Timestamp(r['date']), 'surprise': r['surprise']}
                for r in payload['rows']]
    except Exception as e:
        logging.warning(f'{ticker}: earnings cache unreadable ({e})')
        return None


def fetch_earnings_calendar(ticker: str, limit: int = 12) -> 'list | None':
    """
    Earnings calendar rows [{'date': Timestamp, 'surprise': float|None}, ...]
    ascending (past + scheduled upcoming), via yfinance get_earnings_dates,
    cached per ticker for EARN_CACHE_TTL_D days. On a fetch failure the stale
    cache is served with a warning (same contract as the bar cache, ADR-0001);
    None when nothing is available.

    The cache is "the calendar as fetched recently", NOT point-in-time — --asof
    replays should treat it as an approximation. Cache files don't record
    `limit`; every caller uses the default 12, so a hit always satisfies it.
    """
    if ticker in _EARN_MEMO:
        return _EARN_MEMO[ticker]

    path = _earn_cache_path(ticker)
    if os.path.exists(path):
        age_d = (datetime.date.today()
                 - datetime.date.fromtimestamp(os.path.getmtime(path))).days
        if age_d <= EARN_CACHE_TTL_D:
            rows = _read_earn_cache(ticker)
            if rows is not None:
                _EARN_MEMO[ticker] = rows
                return rows

    try:
        ed = yf.Ticker(ticker).get_earnings_dates(limit=limit)
        if ed is None or ed.empty:
            raise ValueError('empty earnings calendar from yfinance')
        dates = pd.to_datetime(ed.index).tz_localize(None).normalize()
        sp = (ed['Surprise(%)'].values if 'Surprise(%)' in ed.columns
              else [np.nan] * len(ed))
        by_date: dict = {}
        for d, s in zip(dates, sp):                # dedupe, keep a non-null surprise
            s = None if pd.isna(s) else float(s)
            if d not in by_date or by_date[d] is None:
                by_date[d] = s
        rows = [{'date': d, 'surprise': by_date[d]} for d in sorted(by_date)]
        os.makedirs(EARN_CACHE_DIR, exist_ok=True)
        # tmp+rename 原子写: huice --jobs 多进程并发抓日历时不会写出半个 JSON
        tmp = f'{path}.tmp.{os.getpid()}'
        with open(tmp, 'w') as fh:
            json.dump({'fetched': str(datetime.date.today()),
                       'rows': [{'date': str(r['date'].date()),
                                 'surprise': r['surprise']} for r in rows]}, fh)
        os.replace(tmp, path)
        _EARN_MEMO[ticker] = rows
        return rows
    except Exception as e:
        rows = _read_earn_cache(ticker)
        if rows is None:
            logging.warning(f'{ticker}: earnings calendar failed ({e}) and no cache — skipping')
        else:
            logging.warning(f'{ticker}: earnings calendar failed ({e}) — SERVING STALE CACHE')
        _EARN_MEMO[ticker] = rows
        return rows


def _history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """
    Daily or weekly OHLCV for `ticker`, sourced from the yfinance cache.
    If interval='1wk', resamples daily bars to weekly.
    period is approximate: '1y' ~252 days, '2y' ~504 days.
    """
    period_days = {'1y': 365, '6mo': 180, '2y': 730, '3mo': 90}
    n_days = period_days.get(period, 365)

    df = _fetch_daily(ticker)
    if df.empty:
        return df

    cutoff = _now() - pd.Timedelta(days=n_days)
    df = df[df.index >= cutoff]

    if interval == '1wk':
        weekly = df.resample('W').agg({
            'open':   'first',
            'high':   'max',
            'low':    'min',
            'close':  'last',
            'volume': 'sum',
        }).dropna()
        return weekly

    return df


def _sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).mean()


def _upcoming_earnings(ticker: str) -> int | None:
    """Return days until next earnings, or None if unavailable."""
    if _ASOF is not None:
        return None        # backtest: the live calendar is今天's — don't leak future
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        if cal is None:
            return None
        # yfinance ≥0.2 returns a dict-of-lists or DataFrame
        if isinstance(cal, dict):
            dates = cal.get('Earnings Date', [])
            if not dates:
                return None
            dt = pd.Timestamp(dates[0])
        elif isinstance(cal, pd.DataFrame):
            col = [c for c in cal.columns if 'Earnings' in c]
            if not col:
                return None
            dt = pd.Timestamp(cal[col[0]].iloc[0])
        else:
            return None
        return int((dt - pd.Timestamp.now(tz=dt.tzinfo)).days)
    except Exception:
        return None


# ── Layer 1: Market state ─────────────────────────────────────────────────────
def _sticky_above(weekly: pd.DataFrame, ma_win: int = MA_WEEKLY,
                  band: float = STATE_BAND_PCT) -> tuple[bool, bool]:
    """Above/below the MA with a ±band hysteresis, replayed over the series.

    Walks every weekly bar: a close beyond MA×(1±band) sets the side; a close
    inside the band keeps the previous side. Deterministic from the bars alone —
    no persisted state, so an --asof run (truncated series) reproduces exactly
    what a live run would have said that week.

    Returns (above, in_band); in_band flags that the latest close sits inside
    the band and the side is therefore carried, not fresh. Falls back to a raw
    compare if the whole window never left the band (practically impossible).
    """
    closes = weekly['close'].astype(float)
    ma = _sma(closes, ma_win)
    above = None
    for c, m in zip(closes, ma):
        if pd.isna(m) or m <= 0:
            continue
        if c > m * (1 + band):
            above = True
        elif c < m * (1 - band):
            above = False
    last_c, last_m = float(closes.iloc[-1]), float(ma.iloc[-1])
    in_band = abs(last_c / last_m - 1) <= band
    if above is None:
        above = last_c >= last_m
    return above, in_band


def get_market_state() -> tuple[str, dict]:
    """
    Returns ('STRONG'|'MIXED'|'WEAK'|'ERROR', {ticker: {...}})
    STRONG  → both QQQ and SOXX weekly close above 20-week MA
    MIXED   → one above, one below
    WEAK    → both below
    Above/below is judged with the ±STATE_BAND_PCT hysteresis (_sticky_above):
    inside the band a barometer keeps its previous side, so the 3-state machine
    doesn't flip scan mode on every wiggle across the raw line.
    """
    info = {}
    for ticker in BAROMETERS:
        df = _history(ticker, period='2y', interval='1wk')
        if df.empty or len(df) < MA_WEEKLY + 2:
            logging.warning(f"Insufficient weekly data for {ticker}")
            info[ticker] = None
            continue
        ma = _sma(df['close'], MA_WEEKLY).iloc[-1]
        close = df['close'].iloc[-1]
        above, in_band = _sticky_above(df)
        info[ticker] = {
            'close':      round(float(close), 2),
            'ma20w':      round(float(ma), 2),
            'above':      above,
            'in_band':    in_band,
            'pct_vs_ma':  round((float(close) / float(ma) - 1) * 100, 1),
        }

    if any(v is None for v in info.values()):
        return 'ERROR', info

    n_above = sum(1 for v in info.values() if v['above'])
    state = {2: 'STRONG', 1: 'MIXED', 0: 'WEAK'}[n_above]
    return state, info


def get_leadership_breadth(leaders: list[str] | None = None) -> tuple[float, dict]:
    """Fraction of leadership names whose weekly close is below their 20-week MA.

    The generals (MAG7) lead the index in and out: when a majority roll under
    their 20wMA the QQQ/SOXX barometers often still read STRONG (a slow top).
    We use this to demote scan aggressiveness regardless of the barometer state
    (see _gate_state). Below is judged with the same ±STATE_BAND_PCT hysteresis
    as the barometers — a general hovering 1% under its line would otherwise
    flip the gate week after week. Returns (frac_below, {ticker: bool_below});
    falls back to (0.0, {}) when no leader has usable data so the gate simply
    never triggers.
    """
    leaders = leaders or MAG7
    detail = {}
    for t in leaders:
        try:
            wk = _history(t, period='2y', interval='1wk')
            if wk.empty or len(wk) < MA_WEEKLY + 1:
                continue
            above, _ = _sticky_above(wk)
            detail[t] = not above
        except Exception:
            continue
    if not detail:
        return 0.0, {}
    return round(sum(detail.values()) / len(detail), 2), detail


def _read_regime_state() -> dict | None:
    """最近一次 live regime 快照 (us_regime_state.json)。缺失/损坏/超过
    REGIME_MAX_AGE_D 天 → None (体制监控挂了只降级为无体制信息, 不挡扫描)。"""
    try:
        import json
        with open(REGIME_STATE_FILE, encoding='UTF-8') as f:
            st = json.load(f)
        age = (datetime.date.today() - datetime.date.fromisoformat(st['date'])).days
        if age > REGIME_MAX_AGE_D:
            logging.warning(f'regime 快照过期 ({st["date"]}, {age}d) — 忽略体制门控')
            return None
        st['age_d'] = age
        return st
    except FileNotFoundError:
        return None
    except Exception as e:
        logging.warning(f'regime 快照读取失败 ({e}) — 忽略体制门控')
        return None


def _gate_state(state: str, lead_frac: float) -> str:
    """Demote the barometer state one notch when leadership breadth is broken.

    STRONG→MIXED, MIXED→WEAK when ≥ LEADERSHIP_BREACH_FRAC of the generals are
    below their 20wMA; WEAK and ERROR are left unchanged.
    """
    if lead_frac < LEADERSHIP_BREACH_FRAC:
        return state
    return {'STRONG': 'MIXED', 'MIXED': 'WEAK'}.get(state, state)


# ── Layer 2 + 3: Entry signals ────────────────────────────────────────────────
def _key_support_below(daily, entry):
    """Highest real support level below `entry`, from clustered daily swing lows.

    A swing low is a daily low that is the minimum over ±KEY_PIVOT_WIN bars.
    Candidates are limited to the band [entry−15%, entry−2%]; the nearest ones
    (within KEY_CLUSTER_PCT of each other) are merged into one level so a
    repeatedly-tested support counts once. Returns that level, or None.
    """
    if daily is None or daily.empty or len(daily) < 2 * KEY_PIVOT_WIN + 1:
        return None
    lows = daily['low'].tail(KEY_LOOKBACK_D).reset_index(drop=True)
    w = KEY_PIVOT_WIN
    pivots = [float(lows.iloc[i]) for i in range(w, len(lows) - w)
              if lows.iloc[i] == lows.iloc[i - w:i + w + 1].min()]
    if not pivots:
        return None

    hi = entry * (1 - KEY_STOP_MIN_PCT)   # stop ceiling: ≥2% below entry
    lo = entry * (1 - KEY_STOP_MAX_PCT)   # stop floor: ≤15% below entry
    cands = sorted({p for p in pivots if lo <= p <= hi}, reverse=True)
    if not cands:
        return None
    top = cands[0]
    cluster = [p for p in cands if (top - p) / top <= KEY_CLUSTER_PCT]
    return round(sum(cluster) / len(cluster), 2)


def _breakout_signal(weekly: pd.DataFrame, daily: pd.DataFrame | None = None) -> dict | None:
    """
    Breakout buy (used in STRONG market):
    - Weekly close clears 10-week consolidation high
    - Current-week volume ≥ 1.5× 5-week avg
    Stop  = 10-week range low
    Target= entry + range height  (ensures a measured-move target)
    """
    if len(weekly) < CONSOLIDATION_W + 3:
        return None

    cur_close = float(weekly['close'].iloc[-1])
    cur_vol   = float(weekly['volume'].iloc[-1])

    # consolidation = 10 bars before the current bar
    consol = weekly.iloc[-CONSOLIDATION_W - 1 : -1]
    range_high = float(consol['close'].max())
    range_low  = float(consol['low'].min())
    avg_vol_5w = float(weekly['volume'].iloc[-6:-1].mean())

    if cur_close <= range_high:
        return None  # no breakout

    entry = round(cur_close, 2)

    # Stop at the nearest real support below entry; fall back to the deep
    # consolidation low only when no key level is found in range.
    key_stop = _key_support_below(daily, entry)
    if key_stop is not None and entry - key_stop > 0:
        stop, stop_basis = key_stop, 'key-level'
    else:
        stop, stop_basis = round(range_low, 2), 'range-low'
    risk = entry - stop
    if risk <= 0:
        return None

    height = range_high - range_low
    target = round(entry + height, 2)
    rr     = round((target - entry) / risk, 2)

    high_vol  = cur_vol >= avg_vol_5w * VOL_BREAKOUT_MULT
    vol_ratio = round(cur_vol / avg_vol_5w, 2) if avg_vol_5w > 0 else 0.0

    return {
        'type':        'BREAKOUT',
        'entry':       entry,
        'stop':        stop,
        'stop_basis':  stop_basis,
        'target':      target,
        'rr':          rr,
        'rr_ok':       rr >= MIN_RR,
        'range_high':  round(range_high, 2),
        'range_low':   round(range_low, 2),
        'high_vol':    high_vol,
        'vol_ratio':   vol_ratio,
        'confidence':  'HIGH' if (high_vol and rr >= MIN_RR) else 'LOW',
    }


def _pullback_signal(daily: pd.DataFrame) -> dict | None:
    """
    Pullback buy (used in MIXED/WEAK market):
    - Price touching 20d or 50d MA from above
    - Volume contracting (< 5-day avg)
    - Reversal (green) candle
    Stop  = support MA × (1 − 3%)
    Target= 60-day high
    """
    if len(daily) < 55:
        return None

    ma20  = float(_sma(daily['close'], 20).iloc[-1])
    ma50  = float(_sma(daily['close'], 50).iloc[-1])
    close = float(daily['close'].iloc[-1])
    open_ = float(daily['open'].iloc[-1])
    vol   = float(daily['volume'].iloc[-1])
    avg5v = float(daily['volume'].iloc[-6:-1].mean())

    # Determine which MA is being touched
    touch_ma20 = (abs(close / ma20 - 1) <= PULLBACK_TOL) and (close >= ma20 * 0.97)
    touch_ma50 = (abs(close / ma50 - 1) <= PULLBACK_TOL) and (close >= ma50 * 0.97)

    if touch_ma20:
        support_label, support_val = 'MA20d', ma20
    elif touch_ma50:
        support_label, support_val = 'MA50d', ma50
    else:
        return None

    entry = round(close, 2)
    stop  = round(support_val * (1 - PULLBACK_STOP_PCT), 2)
    risk  = entry - stop
    if risk <= 0:
        return None

    target_60h = float(daily['high'].iloc[-60:].max())
    target = round(target_60h if target_60h > entry else entry * 1.12, 2)
    rr = round((target - entry) / risk, 2)

    declining_vol    = vol < avg5v
    reversal_candle  = close > open_   # green candle

    return {
        'type':           'PULLBACK',
        'entry':          entry,
        'stop':           stop,
        'target':         target,
        'rr':             rr,
        'rr_ok':          rr >= MIN_RR,
        'support_label':  support_label,
        'support_val':    round(support_val, 2),
        'ma20d':          round(ma20, 2),
        'ma50d':          round(ma50, 2),
        'declining_vol':  declining_vol,
        'reversal':       reversal_candle,
        'vol_ratio':      round(vol / avg5v, 2) if avg5v > 0 else 0.0,
        'confidence':     'HIGH' if (declining_vol and reversal_candle and rr >= MIN_RR) else 'LOW',
    }


def _gap_confirmation(daily: pd.DataFrame) -> dict | None:
    """信噪比 flag for the latest bar: was the 隔夜跳空 confirmed by the close?

    心法 (memory: overnight-vs-intraday-information): the overnight 段 (昨收→今开)
    carries the directional information; the intraday 段 (今开→今收) is mostly
    noise / mean-reversion. A gap is信息 only when the close holds the open —
    高开站稳开盘价 (close ≥ open) or 低开压住开盘价 (close ≤ open). 高开低走 /
    低开高走 is a faded gap (gap-fill), to be distrusted even if the day is green.

    Returns None when the latest bar has no meaningful gap (|gap| < GAP_MIN_PCT).
    """
    if daily is None or len(daily) < 2:
        return None
    prev_close = float(daily['close'].iloc[-2])
    o = float(daily['open'].iloc[-1])
    c = float(daily['close'].iloc[-1])
    if prev_close <= 0 or o <= 0:
        return None
    gap      = (o / prev_close - 1) * 100
    intraday = (c / o - 1) * 100
    if abs(gap) < GAP_MIN_PCT:
        return None
    up        = gap > 0
    confirmed = (up and c >= o) or (not up and c <= o)
    arrow     = '↑' if up else '↓'
    # Magnitude tier (de-noise floor 2% / confirm 3% / strong 5%). Only a
    # *confirmed* gap earns a tier badge — a faded gap is distrusted at any size.
    mag = abs(gap)
    if confirmed and mag >= GAP_STRONG_PCT:
        tier = '强'
    elif confirmed and mag >= GAP_CONFIRM_PCT:
        tier = '确认'
    elif confirmed and mag >= GAP_WEAK_PCT:
        tier = '弱'
    else:
        tier = ''                       # <2% noise floor, or a faded gap
    label = f"gap{arrow}{'✓' if confirmed else '✗fade'}"
    if tier:
        label += f"·{tier}"
    return {
        'gap_pct':      round(gap, 2),
        'intraday_pct': round(intraday, 2),
        'direction':    1 if up else -1,
        'confirmed':    confirmed,
        'tier':         tier or '—',
        'label':        label,
    }


def _weekly_ma_breach(weekly: pd.DataFrame) -> bool:
    """True if latest weekly close is below 20-week MA (exit rule)."""
    if len(weekly) < MA_WEEKLY + 1:
        return False
    ma = float(_sma(weekly['close'], MA_WEEKLY).iloc[-1])
    return float(weekly['close'].iloc[-1]) < ma


def _weekly_below_fast_ma(weekly: pd.DataFrame) -> bool:
    """True if latest weekly close is below the 10-week MA (momentum cooling).
    Gates the partial trim: take profit when a winner's momentum cracks, not at a
    fixed price target — so an explosive continuation keeps running."""
    if len(weekly) < MA_WEEKLY_FAST + 1:
        return False
    ma = float(_sma(weekly['close'], MA_WEEKLY_FAST).iloc[-1])
    return float(weekly['close'].iloc[-1]) < ma


def scan_stock(ticker: str, market_state: str) -> dict:
    r = {
        'ticker':        ticker,
        'close':         None,
        'ma20w':         None,
        'ma20d':         None,
        'ma50d':         None,
        'signal':        None,
        'exit_signal':   False,
        'earnings_days': None,
        'gap':           None,
        'error':         None,
    }
    try:
        weekly = _history(ticker, period='2y', interval='1wk')
        daily  = _history(ticker, period='1y', interval='1d')

        if weekly.empty or daily.empty:
            r['error'] = 'no data'
            return r

        r['close'] = round(float(daily['close'].iloc[-1]), 2)
        if len(weekly) >= MA_WEEKLY + 1:
            r['ma20w'] = round(float(_sma(weekly['close'], MA_WEEKLY).iloc[-1]), 2)
        if len(daily) >= 21:
            r['ma20d'] = round(float(_sma(daily['close'], 20).iloc[-1]), 2)
        if len(daily) >= 51:
            r['ma50d'] = round(float(_sma(daily['close'], 50).iloc[-1]), 2)

        r['exit_signal']   = _weekly_ma_breach(weekly)
        r['earnings_days'] = _upcoming_earnings(ticker)
        r['gap']           = _gap_confirmation(daily)

        if market_state == 'STRONG':
            r['signal'] = _breakout_signal(weekly, daily)
            if r['signal'] is None:
                # also show if a pullback is forming as secondary note
                pb = _pullback_signal(daily)
                if pb:
                    r['signal'] = pb  # show the pullback in a STRONG market too
        else:
            r['signal'] = _pullback_signal(daily)

        # Entry-side gates: the setup is demoted to an informational
        # 'gated_signal' so every actionable consumer (entry table, signal CSV,
        # ledger, resonance) sees no Setup, while the report still shows what
        # was suppressed and why (_gate label). Priority: regime DEFEND first
        # (systemwide), then the MIXED no-new-entry gate.
        if r['signal'] is not None and _REGIME_SUPPRESS:
            r['gated_signal'] = {**r['signal'], '_gate': '体制DEFEND'}
            r['signal'] = None
        elif r['signal'] is not None and market_state == 'MIXED' and MIXED_NO_NEW_ENTRY:
            r['gated_signal'] = {**r['signal'], '_gate': 'MIXED'}
            r['signal'] = None

    except Exception as e:
        r['error'] = str(e)
        logging.debug(traceback.format_exc())
    return r


# ── Layers 0 & 1: fast risk control (hard stop / event-driven) ─────────────────
def _crash_event_check(daily: pd.DataFrame) -> tuple[int | None, dict]:
    """Classify a holding's recent DAILY action into the fast-risk layers.

    Returns (layer, detail):
      0 — 熔断/硬止损: peak→latest decline over the last CRASH_WINDOW_D sessions
          ≤ -HARD_STOP_PCT. A collapse is its own conclusion — act now.
      1 — 事件驱动: an abnormal drop — the same window decline ≤ -EVENT_DROP_PCT,
          OR a single-day return worse than -EVENT_VOL_MULT × trailing daily σ.
          Go investigate the cause now (don't wait for the weekly close).
      None — nothing abnormal; the slow 20wMA rule (Layer 2) governs.

    Deliberately on daily bars: Layers 0/1 exist precisely to fire between weekly
    closes, faster than the 20wMA exit they pre-empt (twenty_week_trend_system).
    Layer 0 outranks Layer 1; the decline is measured from the local peak so a
    single gap-down or a multi-session slide both register.
    """
    if daily is None or len(daily) < 3:
        return None, {}
    closes = daily['close']
    latest = float(closes.iloc[-1])
    if latest <= 0:
        return None, {}

    # 单日或数日内: decline from the local peak over the trailing window.
    win  = closes.iloc[-(CRASH_WINDOW_D + 1):]
    peak = float(win.max())
    win_drop = (latest / peak - 1) * 100 if peak > 0 else 0.0

    # Abnormal single-day move vs the stock's own recent volatility.
    rets    = closes.pct_change(fill_method=None).dropna()
    day_ret = float(rets.iloc[-1]) * 100 if len(rets) else 0.0
    sigma   = float(rets.tail(EVENT_VOL_LOOKBACK).std()) * 100 if len(rets) >= 5 else 0.0
    vol_mult = round(abs(day_ret) / sigma, 1) if sigma > 0 else None
    vol_trigger = sigma > 0 and day_ret <= -EVENT_VOL_MULT * sigma

    det = {
        'win_drop': round(win_drop, 1),
        'day_ret':  round(day_ret, 1),
        'sigma':    round(sigma, 1),
        'window':   CRASH_WINDOW_D,
        'vol_mult': vol_mult,
    }

    if win_drop <= -HARD_STOP_PCT:
        det['reason'] = f'近{CRASH_WINDOW_D}日 {win_drop:.0f}% 崩塌'
        return 0, det
    if win_drop <= -EVENT_DROP_PCT:
        det['reason'] = f'近{CRASH_WINDOW_D}日 {win_drop:.0f}% 异常下跌'
        return 1, det
    if vol_trigger:
        det['reason'] = f'单日 {day_ret:.0f}% = {vol_mult}×σ 异常波动'
        return 1, det
    return None, det


def crash_event_scan(positions: pd.DataFrame | None) -> dict:
    """Run the Layer 0/1 fast-risk check on every live US holding (daily bars).

    Returns {ticker: (layer, detail)} only for holdings where a layer fired, so
    both position_alerts and position_stop_status can let it take over the slower
    weekly-close logic. Empty when there are no positions.
    """
    out: dict = {}
    if positions is None or positions.empty:
        return out
    for _, pos in positions.iterrows():
        code = pos.get('code', '')
        if not code.startswith('US.'):
            continue
        ticker = code[3:]
        if float(pos.get('qty', 0)) <= 0:
            continue
        try:
            daily = _history(ticker, period='1y', interval='1d')
        except Exception:
            continue
        layer, det = _crash_event_check(daily)
        if layer is not None:
            out[ticker] = (layer, det)
    return out


# ── Layer 4: Position management ──────────────────────────────────────────────
def get_futu_positions(host: str, port: int) -> pd.DataFrame | None:
    """Live US positions from Futu OpenD.

    Returns a DataFrame (possibly empty = connected but no US positions) on
    success, or None when OpenD is genuinely unreachable. futu-api ≥10 replaced
    the per-market OpenUSTradeContext with OpenSecTradeContext + filter_trdmarket.
    """
    try:
        from futu import OpenSecTradeContext, TrdEnv, TrdMarket, RET_OK
        logging.getLogger('FTConsoleLog').setLevel(logging.WARNING)
        ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=host, port=port)
        ret, df = ctx.position_list_query(trd_env=TrdEnv.REAL)
        ctx.close()
        if ret != RET_OK:
            logging.warning(f"Futu position query failed: {df}")
            return None
        return df  # empty df = connected with no positions (not "unavailable")
    except Exception as e:
        logging.warning(f"Cannot connect to FutuOpenD ({host}:{port}): {e}")
        return None


def position_alerts(positions: pd.DataFrame, weekly_cache: dict,
                    risk_layers: dict | None = None) -> list[dict]:
    alerts = []
    if positions is None or positions.empty:
        return alerts
    risk_layers = risk_layers or {}

    for _, pos in positions.iterrows():
        code = pos.get('code', '')
        if not code.startswith('US.'):
            continue
        ticker   = code[3:]
        cost     = float(pos.get('cost_price', 0))
        qty      = float(pos.get('qty', 0))
        unreal   = float(pos.get('unrealized_pl', 0))

        if cost <= 0 or qty <= 0:
            continue

        cost_total = cost * qty
        pl_pct = (unreal / cost_total) * 100 if cost_total > 0 else 0.0
        cur_price = cost + unreal / qty

        actions  = []
        priority = 2          # 0 = hard stop, 1 = event, 2 = normal weekly mgmt

        rl = risk_layers.get(ticker)
        if rl:
            # Layer 0/1 fired → take over and skip the slow weekly-close logic
            # (慢趋势用均线管, 快暴跌用硬止损管 — high layer pre-empts the rest).
            layer, det = rl
            if layer == 0:
                priority = 0
                actions.append(
                    f'🛑 HARD STOP (Layer0·熔断) — {det["reason"]} → 立即处理, '
                    f'不等周五 (cur ~{cur_price:.2f}, P/L {pl_pct:+.1f}%)')
            else:
                priority = 1
                actions.append(
                    f'⚠ EVENT CHECK (Layer1·事件驱动) — {det["reason"]} → 立即查因: '
                    f'不可逆(造假/退市/暴雷/行业塌方)直接清, 原因不明先减半 '
                    f'(cur ~{cur_price:.2f}, P/L {pl_pct:+.1f}%)')
        else:
            # Layer 2/4: the slow 温水 rules, evaluated only off the weekly close.
            # Trim only on WEAKNESS: up ≥ TRIM_PCT *and* momentum cracking (weekly
            # close below the 10-week MA). A fixed +25% target would sell half of
            # the rare explosive continuation that carries most of the return — so
            # we wait for the move to actually cool before taking partial profit.
            weakening = (ticker in weekly_cache
                         and _weekly_below_fast_ma(weekly_cache[ticker]))

            if pl_pct >= TRIM_PCT and weakening:
                actions.append(f'TRIM 50% — P/L +{pl_pct:.1f}% 且周收跌破10周线·动量转弱 (cur ~{cur_price:.2f})')
            elif pl_pct >= BREAKEVEN_PCT:
                actions.append(f'MOVE STOP → breakeven {cost:.2f}  (P/L +{pl_pct:.1f}%)')

            if ticker in weekly_cache and _weekly_ma_breach(weekly_cache[ticker]):
                actions.append(f'EXIT — weekly close below 20-week MA')

        if actions:
            alerts.append({
                'ticker':   ticker,
                'code':     code,
                'cost':     cost,
                'qty':      qty,
                'pl_pct':   round(pl_pct, 1),
                'priority': priority,
                'actions':  actions,
            })
    # Surface the fast-risk layers first: hard stop, then event, then normal mgmt.
    return sorted(alerts, key=lambda a: a['priority'])


def position_stop_status(positions, weekly_cache, risk_layers=None):
    """Per-holding stop status under the layered stop discipline.

    Effective stop = max of three levels. The init/BE pair is Layer 1.5: it
    closes the gap where a breakout entry sits far above its 20wMA and a slow
    slide (too shallow for Layer 0/1) can cost several R with every layer
    silent — the setup's own stop must stay live until a higher level takes over.
      init — the setup's technical stop, recorded in select.yml US_SWING_STOPS
             when the trade is taken. Evaluated at the DAILY close (ADR-0002).
      BE   — breakeven (cost), armed once P/L ≥ +BREAKEVEN_PCT. Daily close.
      20w  — the 20-week MA thesis-invalidation line (Layer 2), judged only at
             the weekly close as before.
    A fast-risk layer (risk_layers) overrides everything. Each row also carries
    open_risk (qty × (close − eff_stop), the input to the portfolio heat line)
    and er_days (next earnings) so the report can force the pre-ER decision.
    """
    rows = []
    if positions is None or positions.empty:
        return rows
    risk_layers = risk_layers or {}

    def _fast_override(ticker):
        rl = risk_layers.get(ticker)
        if not rl:
            return None
        return '🛑 HARD-STOP → ACT NOW' if rl[0] == 0 else '⚠ EVENT → INVESTIGATE NOW'

    for _, pos in positions.iterrows():
        code = pos.get('code', '')
        if not code.startswith('US.'):
            continue
        ticker = code[3:]
        qty    = float(pos.get('qty', 0))
        cost   = float(pos.get('cost_price', 0))
        if qty <= 0:
            continue

        er_days = _upcoming_earnings(ticker)

        weekly = weekly_cache.get(ticker)
        if weekly is None or weekly.empty:
            weekly = _history(ticker, period='2y', interval='1wk')
        try:
            daily = _history(ticker, period='1y', interval='1d')
        except Exception:
            daily = pd.DataFrame()

        close = None
        if not daily.empty:
            close = float(daily['close'].iloc[-1])
        elif not weekly.empty:
            close = float(weekly['close'].iloc[-1])

        base = {'ticker': ticker, 'qty': qty, 'cost': cost, 'er_days': er_days,
                'init_stop': INIT_STOPS.get(ticker), 'be_armed': False,
                'open_risk': None}
        if close is None:
            rows.append({**base, 'close': None, 'stop': None, 'basis': None,
                         'dist_pct': None,
                         'status': _fast_override(ticker) or 'NO DATA'})
            continue

        ma20w = None
        if not weekly.empty and len(weekly) >= MA_WEEKLY + 1:
            ma20w = float(_sma(weekly['close'], MA_WEEKLY).iloc[-1])
        weekly_close = float(weekly['close'].iloc[-1]) if not weekly.empty else close

        init_stop = INIT_STOPS.get(ticker)
        pl_pct    = (close / cost - 1) * 100 if cost > 0 else None
        be_stop   = cost if (pl_pct is not None and pl_pct >= BREAKEVEN_PCT) else None
        base['be_armed'] = be_stop is not None

        # Daily-evaluated protection (Layer 1.5) vs the weekly line (Layer 2).
        hard_stop  = max((s for s in (init_stop, be_stop) if s is not None), default=None)
        hard_basis = 'BE' if (be_stop is not None
                              and (init_stop is None or be_stop >= init_stop)) else 'init'
        candidates = [(v, n) for n, v in
                      (('init', init_stop), ('BE', be_stop), ('20w', ma20w))
                      if v is not None]
        eff_stop, basis = max(candidates) if candidates else (None, None)

        if _fast_override(ticker):
            status = _fast_override(ticker)
        elif hard_stop is not None and close < hard_stop:
            # Fires any day of the week — this check must not wait for Friday.
            status = f'STOP HIT ({hard_basis}) → EXIT TODAY'
        elif ma20w is not None and weekly_close < ma20w:
            status = 'BREACHED → EXIT TODAY'
        elif eff_stop is None:
            status = 'NO STOP'
        elif (close / eff_stop - 1) <= STOP_NEAR_PCT:
            status = 'APPROACHING'
        else:
            status = 'OK'

        dist = round((close / eff_stop - 1) * 100, 1) if eff_stop else None
        open_risk = round(qty * max(close - eff_stop, 0.0), 0) if eff_stop else None
        rows.append({**base, 'close': round(close, 2),
                     'stop': round(eff_stop, 2) if eff_stop else None,
                     'basis': basis, 'dist_pct': dist, 'status': status,
                     'open_risk': open_risk})
    return rows


# ── Report ─────────────────────────────────────────────────────────────────────
def _fmt(val, decimals=2):
    if val is None:
        return '—'
    return f'{val:.{decimals}f}'


def print_report(market_state, baro_info, results, pos_alerts, output_file=None,
                 equity=0.0, stop_status=None, futu_ok=True,
                 baro_state=None, lead_frac=0.0, lead_detail=None,
                 regime=None, regime_action=None):
    lines = []

    def p(*args):
        line = ' '.join(str(a) for a in args)
        lines.append(line)
        print(line)

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    p()
    p('=' * 72)
    p(f'  US TECH SWING SCANNER  —  {now_str}')
    p('=' * 72)

    # ── 主要矛盾 (矛盾论: 说不出主要矛盾就不开新仓; 提示不门控) ────────────────
    # 手写判断层, 机械止损覆盖不到的那一格: 区分"主要矛盾未变的正常回调(忍)"
    # 和"主要矛盾切换(跑)"。周日复盘更新 select.yml US_PRINCIPAL_CONTRADICTION。
    pc_text = str(PRINCIPAL.get('text') or '').strip()
    pc_date = PRINCIPAL.get('date')
    pc_age = None
    if pc_date is not None:
        try:
            pc_age = (datetime.date.today() - pd.Timestamp(pc_date).date()).days
        except Exception:
            pc_age = None
    p()
    if pc_text and pc_age is not None and pc_age <= PRINCIPAL_STALE_D:
        p(f'[ 主要矛盾 ({pc_date}) ]  {pc_text}')
        p('  ↳ 回调不破此矛盾 = 忍; 此矛盾被证伪/切换 = 跑 — 与分层止损互补的判断层')
    elif pc_text:
        p(f'[ 主要矛盾 ⚠ 已 {pc_age} 天未复核 ({pc_date}) ]  {pc_text}')
        p(f'  ↳ 超过 {PRINCIPAL_STALE_D} 天的判断按过期读 (认识落后于实际) — '
          f'周日复盘更新 select.yml US_PRINCIPAL_CONTRADICTION')
    else:
        p('[ 主要矛盾 ⚠ 未填 ]  说不出当前市场的主要矛盾 = 不具备开新仓的认识条件')
        p('  ↳ 在 select.yml US_PRINCIPAL_CONTRADICTION 用一句话写下, 周日复盘复核 (提示不门控)')

    # ── Market state ──────────────────────────────────────────────────────────
    p()
    p(f'[ MARKET STATE: {market_state} ]')
    baro_rows = []
    for t, d in baro_info.items():
        if d:
            side = 'ABOVE ✓' if d['above'] else 'BELOW ✗'
            if d.get('in_band'):
                side += ' ·带内粘滞'
            baro_rows.append([
                t, _fmt(d['close']), _fmt(d['ma20w']),
                f"{d['pct_vs_ma']:+.1f}%",
                side,
            ])
    p(tab_mod.tabulate(baro_rows,
                       headers=['ETF', 'Close', '20W-MA', 'vs MA', 'State'],
                       tablefmt='simple'))
    # Leadership-breadth gate: the generals lead the index in and out, so a
    # broken leadership demotes the scan mode even when QQQ/SOXX read STRONG.
    if lead_detail:
        below = sorted(t for t, b in lead_detail.items() if b)
        p(f'  Leadership breadth (generals < 20wMA): {lead_frac:.0%}'
          f"  [{', '.join(below) if below else 'none'}]")
        if baro_state is not None and market_state != baro_state:
            p(f'  ⚠ GATE: leadership broken (≥{LEADERSHIP_BREACH_FRAC:.0%}) '
              f'→ scan demoted {baro_state} → {market_state}')
    if regime:
        imm_txt = ' · 熊反免疫期' if regime.get('immunity') else ''
        p(f'  Regime (t_us_regime_monitor, {regime["date"]}): '
          f'{regime["state"]} 共振{regime.get("confluence", "?")}/6{imm_txt}'
          + (f'  →  ⚠ GATE: {regime_action}' if regime_action else '  → 不降档'))
    elif regime_action:                      # 快照缺失/过期时说明一句
        p(f'  Regime: {regime_action}')
    mode_msg = {
        'STRONG': '  → Scan mode: BREAKOUT  (weekly close above 10-week range high + volume ≥1.5×)',
        'MIXED':  ('  → Scan mode: NO NEW ENTRY  (MIXED 过渡期不开新仓 — huice 回测唯一稳定'
                   '负期望口袋; setup 降级为观察行 ⛔)' if MIXED_NO_NEW_ENTRY else
                   '  → Scan mode: PULLBACK   (price touching 20d/50d MA + declining vol + reversal)'),
        'WEAK':   '  → Scan mode: CAUTION    (pullback setups only; reduce exposure)',
        'ERROR':  '  → Scan mode: UNKNOWN    (market data error)',
    }
    if _REGIME_SUPPRESS:
        p('  → Scan mode: NO NEW ENTRY  (体制 DEFEND — Setup 全部降级为观察行 ⛔)')
    else:
        p(mode_msg.get(market_state, ''))

    # ── Entry signals ─────────────────────────────────────────────────────────
    p()
    if equity and equity > 0:
        p(f'[ ENTRY SIGNALS ]   (equity ${equity:,.0f}  ·  1R = ${equity*RISK_PCT:,.0f}  ·  cap {MAX_POSITION_PCT:.0%}/name)')
    else:
        p('[ ENTRY SIGNALS ]   (set US_SWING_EQUITY in select.yml for share counts)')
    sig_rows, watch_rows = [], []

    for r in results:
        ticker = r['ticker']
        close  = _fmt(r['close'])
        er     = f"ER {r['earnings_days']}d" if r['earnings_days'] is not None else ''
        layout = ' ← LAYOUT WINDOW' if (r['earnings_days'] is not None
                                         and LAYOUT_DAYS_MIN <= r['earnings_days'] <= LAYOUT_DAYS_MAX) else ''

        if r['error']:
            watch_rows.append([ticker, close, '—', '—', '—', r['error']])
            continue

        gap_note = r['gap']['label'] if r.get('gap') else ''
        s = r['signal']
        if s:
            vol_note = ''
            if s['type'] == 'BREAKOUT':
                basis = 'key' if s.get('stop_basis') == 'key-level' else 'rng'
                vol_note = f"vol×{s['vol_ratio']:.1f}{'✓' if s['high_vol'] else '✗'} stop:{basis}"
            elif s['type'] == 'PULLBACK':
                flags = []
                if s.get('declining_vol'):  flags.append('vol↓')
                if s.get('reversal'):       flags.append('grn')
                vol_note = ' '.join(flags)

            rr_str = f"{s['rr']:.1f}:1 {'✓' if s['rr_ok'] else '✗'}"
            conf   = s['confidence']
            shares, _r, cap_bound = _position_size(s['entry'], s['stop'], equity)
            sh_str = (f'{shares}{"*" if cap_bound else ""}' if shares else '—')
            # Earnings blackout: don't open a new position into the event —
            # zero the share suggestion so the setup can't be acted on as-is.
            blackout = (r['earnings_days'] is not None
                        and 0 <= r['earnings_days'] <= ER_BLACKOUT_D)
            if blackout:
                sh_str = '—'
            bo_note = f'⛔ER{r["earnings_days"]}d·静默期不进(财报后跳空确认再看) ' if blackout else ''
            notes  = f"{bo_note}{s['type']} [{conf}] {vol_note} {gap_note} {er}{layout}".strip()
            sig_rows.append([
                ticker, close,
                _fmt(s['entry']), _fmt(s['stop']), _fmt(s['target']),
                rr_str, sh_str, notes,
            ])
        else:
            exit_flag = '⚠ EXIT (MA breach)' if r['exit_signal'] else ''
            g = r.get('gated_signal')
            gate_note = (f'⛔{g.get("_gate", "MIXED")}不开新仓 {g["type"]} '
                         f'entry {_fmt(g["entry"])} / stop {_fmt(g["stop"])}' if g else '')
            notes     = ' | '.join(filter(None, [exit_flag, gate_note, gap_note, er + layout]))
            watch_rows.append([
                ticker, close,
                _fmt(r['ma20d']), _fmt(r['ma50d']), _fmt(r['ma20w']), notes,
            ])

    if sig_rows:
        p(tab_mod.tabulate(
            sig_rows,
            headers=['Ticker', 'Close', 'Entry', 'Stop', 'Target', 'R:R', 'Shares', 'Notes'],
            tablefmt='simple',
        ))
        if any(row[6].endswith('*') for row in sig_rows):
            p('  * share count limited by the 25%/name cap, not by 1R risk')
    else:
        gated = [r for r in results if r.get('gated_signal')]
        if gated:
            why = sorted({r['gated_signal'].get('_gate', 'MIXED') for r in gated})
            p(f'  No actionable entry — {len(gated)} setup(s) suppressed by gate(s): '
              f'{"、".join(why)} (见观察表 ⛔ 行; docs/huice_backtest_findings.md).')
        else:
            p('  No entry signals this week.')

    # ── Watch list ────────────────────────────────────────────────────────────
    if watch_rows:
        p()
        p('[ WATCH LIST — no signal yet ]')
        p(tab_mod.tabulate(
            watch_rows,
            headers=['Ticker', 'Close', 'MA20d', 'MA50d', 'MA20w', 'Notes'],
            tablefmt='simple',
        ))

    # ── AI capex chain ────────────────────────────────────────────────────────
    p()
    p('[ AI CAPEX CHAIN — Hyperscaler earnings as leading indicator for semis ]')
    hs_rows = []
    for r in results:
        if r['ticker'] not in HYPERSCALERS:
            continue
        ed  = r['earnings_days']
        ed_s = f"ER in {ed}d" if ed is not None else 'ER unknown'
        win = '← LAYOUT WINDOW (buy semis now)' if (
            ed is not None and LAYOUT_DAYS_MIN <= ed <= LAYOUT_DAYS_MAX) else ''
        hs_rows.append([r['ticker'], _fmt(r['close']), ed_s, win])
    p(tab_mod.tabulate(hs_rows,
                       headers=['Hyperscaler', 'Close', 'Earnings', 'Action'],
                       tablefmt='simple'))
    p('  Capex guidance ↑ at earnings → buy NVDA / AVGO / AMAT / LRCX / KLAC')

    # ── Holdings stop status ──────────────────────────────────────────────────
    p()
    p('[ HOLDINGS — stop status (eff stop = max(init, BE, 20wMA); init/BE 按日收盘, 20wMA 按周收盘) ]')
    if not futu_ok:
        p('  Futu unavailable — holdings stop status skipped (start OpenD to include it).')
    elif not stop_status:
        p('  No US holdings in the Futu account.')
    else:
        hold_rows = []
        for h in stop_status:
            dist = f"{h['dist_pct']:+.1f}%" if h['dist_pct'] is not None else '—'
            er   = f"{h['er_days']}d" if h.get('er_days') is not None else '—'
            hold_rows.append([
                h['ticker'], f"{h['qty']:.0f}", _fmt(h['close']),
                _fmt(h['stop']), h.get('basis') or '—', dist, er, h['status'],
            ])
        p(tab_mod.tabulate(
            hold_rows,
            headers=['Ticker', 'Qty', 'Close', 'Stop', 'Basis', 'vs Stop', 'ER', 'Status'],
            tablefmt='simple',
        ))
        if any('HARD-STOP' in h['status'] for h in stop_status):
            p('  🛑 HARD-STOP (Layer0·熔断): 崩塌已接管 — 立即处理, 不等周线/周五.')
        if any('EVENT' in h['status'] for h in stop_status):
            p('  ⚠ EVENT (Layer1·事件驱动): 异常下跌 — 立即查因(不可逆清/原因不明先减半).')
        if any('STOP HIT' in h['status'] for h in stop_status):
            p('  ⛔ STOP HIT (Layer1.5): 日收盘跌破 初始止损/保本线 — 今日离场, 不等周五.')
        if any(h['status'].startswith('BREACHED') for h in stop_status):
            p('  ⚠ BREACHED holdings: exit today at the open per the swing exit rule.')

        # Layer 1.5 depends on the init stop being registered — nag until it is.
        # BE-armed holdings are exempt: breakeven (=cost) already sits at or
        # above any entry-time stop, so an init stop would be inert there.
        no_init = sorted(h['ticker'] for h in stop_status
                         if h.get('init_stop') is None and not h.get('be_armed'))
        if no_init:
            p(f"  ⚠ 未登记初始止损: {', '.join(no_init)} — 在 select.yml US_SWING_STOPS 补一行, "
              f'否则 Layer1.5 缺位、只剩 20 周线兜底 (突破买点离线远时缝隙可达数 R).')

        # ── 易位判据 (矛盾论: 主要方面易位) — 止损价之外的那半句条件 ─────────
        # 止损价是"易位判据"的价格化身; 这里展示事件/条件那一半, 让止损日执行
        # 的是"性质已变"的结论, 而不是和浮亏讨价还价。
        held = [h['ticker'] for h in stop_status]
        for t in held:
            if t in FLIP_NOTES:
                p(f'  ⚖ {t} 易位判据: {FLIP_NOTES[t]}')
        no_flip = sorted(t for t in held if t not in FLIP_NOTES)
        if no_flip:
            p(f"  ⚠ 未写易位判据: {', '.join(no_flip)} — 在 select.yml US_SWING_FLIP 每仓一句话: "
              f'什么价格/事件出现 = 承认这笔仓的主要方面已易位 (多头逻辑不再居支配).')

        # ── Portfolio open heat: the pool is one theme; risk it as one trade ──
        known   = [h for h in stop_status if h.get('open_risk') is not None]
        unknown = sorted(h['ticker'] for h in stop_status if h.get('open_risk') is None)
        heat    = sum(h['open_risk'] for h in known)
        if equity and equity > 0:
            heat_pct = heat / equity * 100
            p(f'  OPEN HEAT 组合开放风险: ${heat:,.0f} = {heat_pct:.1f}% of equity '
              f'(cap {HEAT_CAP_PCT:.0%} — 池内高相关, 按全部止损同日打穿计)')
            if heat > equity * HEAT_CAP_PCT:
                p(f'  ⛔ OVER HEAT CAP: 超出主题风险预算 → 不进新仓; 上移最弱仓止损或减仓, '
                  f'直到 heat ≤ {HEAT_CAP_PCT:.0%}.')
        else:
            p(f'  OPEN HEAT 组合开放风险: ${heat:,.0f} (set US_SWING_EQUITY for the % and cap check)')
        if unknown:
            p(f"  ⚠ heat 未计入 (无有效止损): {', '.join(unknown)} — 实际风险被低估.")

        # ── Pre-earnings forced decision (weeks-long holds eat every report) ──
        er_soon = [h for h in stop_status
                   if h.get('er_days') is not None and 0 <= h['er_days'] <= ER_BLACKOUT_D]
        for h in er_soon:
            p(f"  📅 {h['ticker']} 财报 {h['er_days']}d 内 → 今天写下财报决策并留档: "
              f'满仓扛 / 减半扛 / 出掉. 不决策 = 默认减半 (事后没判断力, 规则先行).')

    # ── Position management alerts ────────────────────────────────────────────
    if pos_alerts:
        p()
        p('[ POSITION MANAGEMENT ALERTS ]   (Layer0 熔断 / Layer1 事件 优先于周线规则)')
        for a in pos_alerts:
            p(f"  {a['ticker']:6s}  cost {a['cost']:.2f}  qty {a['qty']:.0f}  P/L {a['pl_pct']:+.1f}%")
            for action in a['actions']:
                p(f"    → {action}")

    # ── Pre-trade checklist ───────────────────────────────────────────────────
    p()
    p('[ PRE-TRADE CHECKLIST — fill before any entry, do NOT trade intraday ]')
    for item in [
        'Catalyst confirmed?  (earnings window / sector capex cycle)',
        'Revenue growth >15%, EPS accelerating or surprise?',
        'Valuation not at historical extreme?',
        'Entry = last close (decided after market close, not intraday)',
        'Stop = technical level written (NOT a fixed %)  →  ______',
        f'Stop registered in select.yml US_SWING_STOPS (Layer1.5 生效的前提)  →  ______',
        '主要矛盾一句话说得出且未过期 (晨报顶部)?  说不出 = 不开新仓',
        '易位判据写了吗: 什么条件出现=这笔仓主要方面易位 → select.yml US_SWING_FLIP  →  ______',
        'Target written, R:R ≥ 2:1 confirmed  →  ______',
        'Exit condition defined (what would invalidate thesis)?',
        'Share count from table (risk 1R = 1% equity, cap 25%/name)?',
        'Concurrent positions ≤ 5?',
        f'Earnings > {ER_BLACKOUT_D} days away?  (≤{ER_BLACKOUT_D}d = 静默期, 不进新仓)',
        f'Portfolio OPEN HEAT after this entry still ≤ {HEAT_CAP_PCT:.0%} of equity?',
    ]:
        p(f'  □  {item}')
    p()

    # ── Legend / field explanations ───────────────────────────────────────────
    p('[ 字段说明 / LEGEND ]')
    p(f'  MARKET STATE 市场状态: STRONG=QQQ与SOXX均在{MA_WEEKLY}周线上方 / MIXED=一上一下 / WEAK=均在下方')
    p(f'    粘滞带 hysteresis: 收盘在自身{MA_WEEKLY}周线±{STATE_BAND_PCT:.0%}带内 → 沿用前一次判定(表中标"带内粘滞"),')
    p(f'      只有收盘越过带缘才换向 — 防震荡市里三态机在均线附近反复切换扫描模式(周中假信号的主因)')
    p(f'    Leadership breadth 领导股广度: 将军(MAG7)中跌破各自{MA_WEEKLY}周线的比例(同样±{STATE_BAND_PCT:.0%}粘滞); ≥{LEADERSHIP_BREACH_FRAC:.0%}触发GATE')
    p(f'      → 将军先于指数倒下=慢顶, 此时即便barometer为STRONG也降一档(STRONG→MIXED→WEAK), 退出BREAKOUT改保守扫描')
    p('  ENTRY SIGNALS 入场信号 (机会, 尚未持有):')
    p('    Entry 建议入场价 · Stop 建议止损价 · Target 目标价')
    p(f'    R:R 盈亏比=(目标-入场)/(入场-止损); ✓ = ≥{MIN_RR:.0f}:1 值得做, ✗ = 不值得')
    p(f'    Shares 建议买入股数 (=账户{RISK_PCT:.0%}风险÷每股风险, 单票≤{MAX_POSITION_PCT:.0%}仓位); * = 被仓位上限压过')
    p('      ⚠ Shares 是"建议买入量", 不是你的持仓! 你的持仓在 HOLDINGS 区')
    p('    Notes: stop:key=止损在关键支撑位 / stop:rng=回退到区间低点; vol×N=量比; ER Nd=N天后财报')
    p(f'    ⛔ER·静默期: 距财报≤{ER_BLACKOUT_D}d 的 setup 不进 (Shares 清零) — 单日-20%事件多半就是财报; 财报后跳空确认是更好的入场')
    p(f'    gap↑/↓=隔夜跳空方向; ✓=收盘站稳开盘(信息) / ✗fade=高开低走(噪声, 别信);')
    p(f'      分档(实测 SP500∪NDX 5y, 仅✓时给): 弱≥{GAP_WEAK_PCT:.0f}%(lift7x) · 确认≥{GAP_CONFIRM_PCT:.0f}%(lift17x) · 强≥{GAP_STRONG_PCT:.0f}%(lift48x); <{GAP_WEAK_PCT:.0f}%是去噪地板,非信号')
    p('      用法: gap·确认/强 出现在 ⟨已持有⟩→上移止损/加固; 出现在 ⟨未持有信号⟩→多数已晚, 别追开盘(大跳空日内段均值转负)')
    p('  HOLDINGS 持仓止损 (你的真实持仓, 需开富途 OpenD):')
    p(f'    Stop=有效止损=max(init 初始技术止损, BE 保本线, 20wMA {MA_WEEKLY}周线) · Basis=当前哪条最高')
    p(f'      init/BE 按【日收盘】判 (Layer1.5, 补突破买点远离周线的缝隙) · 20wMA 只按【周收盘】判')
    p(f'      init 来源: select.yml US_SWING_STOPS — 成交当天登记 setup 止损价, 离场后删除该行')
    p(f'    Status: OK / APPROACHING(距止损≤{STOP_NEAR_PCT:.0%}) / STOP HIT(日收破init/BE→今日离场) / BREACHED(周收破20周线→今日离场)')
    p(f'    ER=距下次财报天数; ≤{ER_BLACKOUT_D}d 强制写财报决策(满仓扛/减半/出掉), 不写=默认减半')
    p(f'    OPEN HEAT=全部持仓止损同日打穿的总损失; 池内高相关=一个主题仓, 预算上限 {HEAT_CAP_PCT:.0%} equity')
    p('  POSITION MANAGEMENT 持仓管理 (对已有仓位的操作):')
    p(f'    MOVE STOP→breakeven: 浮盈≥{BREAKEVEN_PCT:.0f}% → 止损上移到成本价(锁不亏; 阈值高=留够缓冲, 正常回踩不被洗出)')
    p(f'    TRIM 50%: 浮盈≥{TRIM_PCT:.0f}% 且周收跌破{MA_WEEKLY_FAST}周线(动量转弱) → 减半 (走弱才减, 不按价格目标砍, 给爆发续涨留右尾)')
    p(f'    EXIT: 周收盘跌破{MA_WEEKLY}周线 → 清仓 (优先于止盈)')
    p('  ⚡ 快速风控 (Layer0/1, 对持仓按"日线"评估, 优先于周线规则; 触发即接管、跳过下面所有确认):')
    p(f'    Layer0 🛑硬止损/熔断: 近{CRASH_WINDOW_D}日 峰→现 跌幅≤-{HARD_STOP_PCT:.0f}% = 崩塌 → 立即处理, 绝不等周五')
    p(f'    Layer1 ⚠事件驱动: 跌幅≤-{EVENT_DROP_PCT:.0f}% 或 单日>{EVENT_VOL_MULT:.0f}×波动率(σ) → 立即查因(不可逆清仓/原因不明先减半)')
    p('    一标的只用一套框架: 别涨时讲技术、该止损时改讲"基本面好不可能跌" (慢趋势用均线管, 快暴跌用硬止损管)')
    p()

    # Write to file
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        logging.info(f"Report saved → {output_file}")


def write_signal_csv(results, path):
    """Machine-readable per-ticker technical signal, for t_us_resonance.py to join.

    One row per scanned ticker; signal fields are blank when there is no setup.
    Purely additive — does not affect the text Morning Report.
    """
    rows = []
    for r in results:
        s = r.get('signal') or {}
        g = r.get('gap') or {}
        rows.append({
            'ticker':        r['ticker'],
            'close':         r.get('close'),
            'signal_type':   s.get('type', ''),
            'confidence':    s.get('confidence', ''),
            'entry':         s.get('entry'),
            'stop':          s.get('stop'),
            'target':        s.get('target'),
            'rr':            s.get('rr'),
            'rr_ok':         s.get('rr_ok'),
            'exit_signal':   r.get('exit_signal'),
            'earnings_days': r.get('earnings_days'),
            'er_blackout':   (r.get('earnings_days') is not None
                              and 0 <= r['earnings_days'] <= ER_BLACKOUT_D),
            'gap_pct':       g.get('gap_pct'),
            'gap_dir':       g.get('direction'),
            'gap_confirmed': g.get('confirmed'),
            'gap_tier':      g.get('tier'),
        })
    pd.DataFrame(rows).to_csv(path, index=False)
    logging.info(f'Tech signal CSV → {path}')


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--host',     dest='host',    default='127.0.0.1')
    parser.add_option('--port',     dest='port',    default=11111, type='int')
    parser.add_option('--no-futu',  dest='no_futu', action='store_true', default=False,
                      help='Skip FutuOpenD position query')
    parser.add_option('--ticker',   dest='ticker',  default=None,
                      help='Scan a single ticker instead of the full universe')
    parser.add_option('--output',   dest='output',  default=None,
                      help='Save report to this file path')
    parser.add_option('--asof',     dest='asof',    default=None,
                      help='Backtest as-of YYYY-MM-DD: use only bars ≤ that day and '
                           'anchor "today" there (reproduces that day\'s report). '
                           'Forces --no-futu; earnings countdown is suppressed.')
    parser.add_option('--no-mixed-gate', dest='no_mixed_gate', action='store_true',
                      default=False,
                      help='恢复 MIXED 也出 PULLBACK Setup 的旧行为 (回测对照用; '
                           'live 默认 MIXED 不开新仓)')
    parser.add_option('--no-regime-gate', dest='no_regime_gate', action='store_true',
                      default=False,
                      help='关闭体制联动门控 (regime DEFEND 抑制新仓 / WATCH·熊反'
                           '免疫期降档; live 默认开)')
    opts, _ = parser.parse_args()

    global _ASOF, MIXED_NO_NEW_ENTRY, REGIME_GATE, _REGIME_SUPPRESS
    if opts.no_mixed_gate:
        MIXED_NO_NEW_ENTRY = False
        logging.info('MIXED no-new-entry gate DISABLED (--no-mixed-gate)')
    if opts.no_regime_gate:
        REGIME_GATE = False
        logging.info('Regime gate DISABLED (--no-regime-gate)')
    if opts.asof:
        try:
            _ASOF = pd.Timestamp(opts.asof).normalize()
        except ValueError:
            parser.error(f'--asof 无法解析: {opts.asof}')
        opts.no_futu = True          # 历史持仓无意义, 且避免查 live Futu
        logging.info(f'AS-OF backtest mode: anchoring to {_ASOF.date()} (no-futu forced)')

    logging.info('US Tech Swing Scanner starting')

    # Layer 1
    logging.info('Checking market state …')
    baro_state, baro_info = get_market_state()
    lead_frac, lead_detail = get_leadership_breadth()
    market_state = _gate_state(baro_state, lead_frac)
    if market_state != baro_state:
        logging.info(f'Leadership breadth {lead_frac:.0%} of generals below 20wMA '
                     f'→ demote {baro_state}→{market_state}')

    # 体制联动 (regime DEFEND / WATCH / 熊反免疫期): 只动入场侧, 在领导股门之后叠加。
    # --asof 不读快照 (live 状态对历史日期是未来函数), 回测行为不变。
    regime, regime_action = None, None
    if REGIME_GATE and _ASOF is None:
        regime = _read_regime_state()
        if regime is None:
            regime_action = '无可用 regime 快照 (缺失/过期) — 本次不做体制门控'
        elif regime['state'] == 'DEFEND':
            _REGIME_SUPPRESS = True
            regime_action = 'DEFEND → 不开新仓 (全部 Setup 降级为观察行)'
        elif regime['state'] == 'WATCH' or (regime.get('immunity')
                                            and REGIME_IMMUNITY_DEMOTE):
            prev = market_state
            market_state = {'STRONG': 'MIXED', 'MIXED': 'WEAK'}.get(market_state,
                                                                    market_state)
            why = ('WATCH 早减仓' if regime['state'] == 'WATCH'
                   else f'熊反免疫期 (post-DEFEND, SPY 未连续站回200日线)')
            regime_action = f'{why} → 扫描模式降档 {prev}→{market_state}'
        elif regime.get('immunity'):
            # 免疫期默认仅提示 (REGIME_IMMUNITY_DEMOTE 注释里有验证依据):
            # 熊反与真 V 复苏实时不可分, 硬降档在 2025 V 复苏里净亏。
            regime_action = ('⚠ 熊反免疫期生效 (仅提示, 不降档) — 若此后走的是'
                             '熊反, 追高单将是最贵的假信号; 参 findings §2.8 自行裁量')
        if regime_action and regime is not None:
            logging.info(f'Regime gate: {regime["state"]}'
                         f'{" +immunity" if regime.get("immunity") else ""}'
                         f' ({regime["date"]}) — {regime_action}')
    logging.info(f'Market state: {market_state} (barometer {baro_state})')

    # Layer 2 + 3
    universe = [opts.ticker.upper()] if opts.ticker else UNIVERSE
    results, weekly_cache = [], {}

    for ticker in universe:
        logging.info(f'Scanning {ticker} …')
        r = scan_stock(ticker, market_state)
        results.append(r)
        try:
            weekly_cache[ticker] = _history(ticker, period='2y', interval='1wk')
        except Exception:
            pass

    # Layer 4
    alerts, stop_status = [], []
    futu_ok = not opts.no_futu
    if not opts.no_futu:
        logging.info('Querying Futu positions …')
        pos_df = get_futu_positions(opts.host, opts.port)
        futu_ok = pos_df is not None
        # Layers 0/1 first (daily-bar crash/event check), then the weekly mgmt that
        # they pre-empt — both readers share the same risk_layers dict.
        risk_layers = crash_event_scan(pos_df)
        if risk_layers:
            logging.warning('Fast risk-control triggered: '
                            + ', '.join(f'{t}=L{l}' for t, (l, _) in risk_layers.items()))
        alerts = position_alerts(pos_df, weekly_cache, risk_layers)
        stop_status = position_stop_status(pos_df, weekly_cache, risk_layers)

    # Default output path. Under --asof, tag with the as-of date so a backtest
    # run never overwrites the live dated report.
    date_str = (_ASOF.strftime('%Y%m%d') if _ASOF is not None
                else datetime.datetime.now().strftime('%Y%m%d'))
    out_file = opts.output
    if out_file is None:
        res_root = '/home/ryan/DATA/result'
        if os.path.isdir(res_root):
            out_dir = os.path.join(res_root, 'us_tech_swing')
            os.makedirs(out_dir, exist_ok=True)
            out_file = f'{out_dir}/us_tech_swing_{date_str}.txt'

    print_report(market_state, baro_info, results, alerts, out_file,
                 equity=ACCOUNT_EQUITY, stop_status=stop_status, futu_ok=futu_ok,
                 baro_state=baro_state, lead_frac=lead_frac, lead_detail=lead_detail,
                 regime=regime, regime_action=regime_action)

    # Machine-readable signal CSV (consumed by t_us_resonance.py)
    res_root = '/home/ryan/DATA/result'
    if os.path.isdir(res_root):
        out_dir = os.path.join(res_root, 'us_tech_swing')
        os.makedirs(out_dir, exist_ok=True)
        try:
            write_signal_csv(results, f'{out_dir}/us_tech_signal_{date_str}.csv')
        except Exception as e:
            logging.warning(f'tech signal CSV write failed: {e}')

    # Signal attribution ledger (信号归因日志): freeze every emitted Setup as a
    # virtual trade so t_us_signal_attrib.py can measure which signal type
    # actually pays. Live runs only — an --asof row would be a look-ahead lie.
    if _ASOF is None:
        try:
            from signal_ledger import log_signals
            ledger_rows = []
            for r in results:
                s = r.get('signal')
                if not s:
                    continue
                g = r.get('gap') or {}
                ledger_rows.append({
                    'ticker':       r['ticker'],
                    'signal_type':  s.get('type'),
                    'confidence':   s.get('confidence'),
                    'market_state': market_state,
                    'close':        r.get('close'),
                    'entry':        s.get('entry'),
                    'stop':         s.get('stop'),
                    'target':       s.get('target'),
                    'rr':           s.get('rr'),
                    'rr_ok':        s.get('rr_ok'),
                    'er_days':      r.get('earnings_days'),
                    'er_blackout':  (r.get('earnings_days') is not None
                                     and 0 <= r['earnings_days'] <= ER_BLACKOUT_D),
                    'gap_tier':     g.get('tier'),
                })
            log_signals(ledger_rows, source='tech_swing')
        except Exception as e:
            logging.warning(f'signal ledger write failed: {e}')


if __name__ == '__main__':
    main()
