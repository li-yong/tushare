# coding: utf-8
"""
US Tech Swing Trade Scanner  (session 81 framework)

4-layer medium-term trading system for Mag7 + Semiconductors:
  Layer 1 — Market state  : QQQ + SOXX vs 20-week MA → STRONG / MIXED / WEAK
  Layer 2 — Entry signal  : breakout (STRONG) or pullback to MA (MIXED/WEAK)
  Layer 3 — Pre-trade setup: entry / stop (technical) / target / R:R ≥ 2:1
  Layer 4 — Position mgmt : breakeven stop at +30%, trim only on weakness
                            (≥+25% AND weekly close < 10-week MA), exit on 20wMA breach

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


def _load_watchlist():
    """Load the swing watchlist + account equity from select.yml; fall back to defaults.

    YAML entries may be bare tickers or `TICKER: label` maps — we take the
    ticker either way, so the same loader works across select.yml's styles.
    """
    global MAG7, SEMIS, AI_CHAIN, HYPERSCALERS, BAROMETERS, ACCOUNT_EQUITY
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
def get_market_state() -> tuple[str, dict]:
    """
    Returns ('STRONG'|'MIXED'|'WEAK'|'ERROR', {ticker: {...}})
    STRONG  → both QQQ and SOXX weekly close above 20-week MA
    MIXED   → one above, one below
    WEAK    → both below
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
        info[ticker] = {
            'close':      round(float(close), 2),
            'ma20w':      round(float(ma), 2),
            'above':      bool(close > ma),
            'pct_vs_ma':  round((float(close) / float(ma) - 1) * 100, 1),
        }

    if any(v is None for v in info.values()):
        return 'ERROR', info

    n_above = sum(1 for v in info.values() if v['above'])
    state = {2: 'STRONG', 1: 'MIXED', 0: 'WEAK'}[n_above]
    return state, info


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

    except Exception as e:
        r['error'] = str(e)
        logging.debug(traceback.format_exc())
    return r


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


def position_alerts(positions: pd.DataFrame, weekly_cache: dict) -> list[dict]:
    alerts = []
    if positions is None or positions.empty:
        return alerts

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

        # Trim only on WEAKNESS: up ≥ TRIM_PCT *and* momentum cracking (weekly
        # close below the 10-week MA). A fixed +25% target would sell half of the
        # rare explosive continuation that carries most of the return — so we wait
        # for the move to actually cool before taking partial profit.
        weakening = (ticker in weekly_cache
                     and _weekly_below_fast_ma(weekly_cache[ticker]))

        actions = []
        if pl_pct >= TRIM_PCT and weakening:
            actions.append(f'TRIM 50% — P/L +{pl_pct:.1f}% 且周收跌破10周线·动量转弱 (cur ~{cur_price:.2f})')
        elif pl_pct >= BREAKEVEN_PCT:
            actions.append(f'MOVE STOP → breakeven {cost:.2f}  (P/L +{pl_pct:.1f}%)')

        if ticker in weekly_cache and _weekly_ma_breach(weekly_cache[ticker]):
            actions.append(f'EXIT — weekly close below 20-week MA')

        if actions:
            alerts.append({
                'ticker':  ticker,
                'code':    code,
                'cost':    cost,
                'qty':     qty,
                'pl_pct':  round(pl_pct, 1),
                'actions': actions,
            })
    return alerts


def position_stop_status(positions, weekly_cache):
    """Per-holding stop status, evaluated at the latest weekly close (ADR-0002).

    Stop level = 20-week MA (the thesis-invalidation line). A holding outside
    the watchlist has its weekly bars fetched on demand. Returns one row per
    live US holding.
    """
    rows = []
    if positions is None or positions.empty:
        return rows

    for _, pos in positions.iterrows():
        code = pos.get('code', '')
        if not code.startswith('US.'):
            continue
        ticker = code[3:]
        qty    = float(pos.get('qty', 0))
        cost   = float(pos.get('cost_price', 0))
        if qty <= 0:
            continue

        weekly = weekly_cache.get(ticker)
        if weekly is None or weekly.empty:
            weekly = _history(ticker, period='2y', interval='1wk')
        if weekly.empty or len(weekly) < MA_WEEKLY + 1:
            rows.append({'ticker': ticker, 'qty': qty, 'cost': cost,
                         'close': None, 'stop': None, 'dist_pct': None,
                         'status': 'NO DATA'})
            continue

        close = float(weekly['close'].iloc[-1])
        stop  = float(_sma(weekly['close'], MA_WEEKLY).iloc[-1])
        dist  = (close / stop - 1) * 100
        if close < stop:
            status = 'BREACHED → EXIT TODAY'
        elif dist <= STOP_NEAR_PCT * 100:
            status = 'APPROACHING'
        else:
            status = 'OK'
        rows.append({'ticker': ticker, 'qty': qty, 'cost': cost,
                     'close': round(close, 2), 'stop': round(stop, 2),
                     'dist_pct': round(dist, 1), 'status': status})
    return rows


# ── Report ─────────────────────────────────────────────────────────────────────
def _fmt(val, decimals=2):
    if val is None:
        return '—'
    return f'{val:.{decimals}f}'


def print_report(market_state, baro_info, results, pos_alerts, output_file=None,
                 equity=0.0, stop_status=None, futu_ok=True):
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

    # ── Market state ──────────────────────────────────────────────────────────
    p()
    p(f'[ MARKET STATE: {market_state} ]')
    baro_rows = []
    for t, d in baro_info.items():
        if d:
            baro_rows.append([
                t, _fmt(d['close']), _fmt(d['ma20w']),
                f"{d['pct_vs_ma']:+.1f}%",
                'ABOVE ✓' if d['above'] else 'BELOW ✗',
            ])
    p(tab_mod.tabulate(baro_rows,
                       headers=['ETF', 'Close', '20W-MA', 'vs MA', 'State'],
                       tablefmt='simple'))
    mode_msg = {
        'STRONG': '  → Scan mode: BREAKOUT  (weekly close above 10-week range high + volume ≥1.5×)',
        'MIXED':  '  → Scan mode: PULLBACK   (price touching 20d/50d MA + declining vol + reversal)',
        'WEAK':   '  → Scan mode: CAUTION    (pullback setups only; reduce exposure)',
        'ERROR':  '  → Scan mode: UNKNOWN    (market data error)',
    }
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
            notes  = f"{s['type']} [{conf}] {vol_note} {gap_note} {er}{layout}".strip()
            sig_rows.append([
                ticker, close,
                _fmt(s['entry']), _fmt(s['stop']), _fmt(s['target']),
                rr_str, sh_str, notes,
            ])
        else:
            exit_flag = '⚠ EXIT (MA breach)' if r['exit_signal'] else ''
            notes     = ' | '.join(filter(None, [exit_flag, gap_note, er + layout]))
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
    p('[ HOLDINGS — stop status (stop = 20-week MA, evaluated at weekly close) ]')
    if not futu_ok:
        p('  Futu unavailable — holdings stop status skipped (start OpenD to include it).')
    elif not stop_status:
        p('  No US holdings in the Futu account.')
    else:
        hold_rows = []
        for h in stop_status:
            dist = f"{h['dist_pct']:+.1f}%" if h['dist_pct'] is not None else '—'
            hold_rows.append([
                h['ticker'], f"{h['qty']:.0f}", _fmt(h['close']),
                _fmt(h['stop']), dist, h['status'],
            ])
        p(tab_mod.tabulate(
            hold_rows,
            headers=['Ticker', 'Qty', 'Close', 'Stop(20wMA)', 'vs Stop', 'Status'],
            tablefmt='simple',
        ))
        if any(h['status'].startswith('BREACHED') for h in stop_status):
            p('  ⚠ BREACHED holdings: exit today at the open per the swing exit rule.')

    # ── Position management alerts ────────────────────────────────────────────
    if pos_alerts:
        p()
        p('[ POSITION MANAGEMENT ALERTS ]')
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
        'Target written, R:R ≥ 2:1 confirmed  →  ______',
        'Exit condition defined (what would invalidate thesis)?',
        'Share count from table (risk 1R = 1% equity, cap 25%/name)?',
        'Concurrent positions ≤ 5?',
    ]:
        p(f'  □  {item}')
    p()

    # ── Legend / field explanations ───────────────────────────────────────────
    p('[ 字段说明 / LEGEND ]')
    p(f'  MARKET STATE 市场状态: STRONG=QQQ与SOXX均在{MA_WEEKLY}周线上方 / MIXED=一上一下 / WEAK=均在下方')
    p('  ENTRY SIGNALS 入场信号 (机会, 尚未持有):')
    p('    Entry 建议入场价 · Stop 建议止损价 · Target 目标价')
    p(f'    R:R 盈亏比=(目标-入场)/(入场-止损); ✓ = ≥{MIN_RR:.0f}:1 值得做, ✗ = 不值得')
    p(f'    Shares 建议买入股数 (=账户{RISK_PCT:.0%}风险÷每股风险, 单票≤{MAX_POSITION_PCT:.0%}仓位); * = 被仓位上限压过')
    p('      ⚠ Shares 是"建议买入量", 不是你的持仓! 你的持仓在 HOLDINGS 区')
    p('    Notes: stop:key=止损在关键支撑位 / stop:rng=回退到区间低点; vol×N=量比; ER Nd=N天后财报')
    p(f'    gap↑/↓=隔夜跳空方向; ✓=收盘站稳开盘(信息) / ✗fade=高开低走(噪声, 别信);')
    p(f'      分档(实测 SP500∪NDX 5y, 仅✓时给): 弱≥{GAP_WEAK_PCT:.0f}%(lift7x) · 确认≥{GAP_CONFIRM_PCT:.0f}%(lift17x) · 强≥{GAP_STRONG_PCT:.0f}%(lift48x); <{GAP_WEAK_PCT:.0f}%是去噪地板,非信号')
    p('      用法: gap·确认/强 出现在 ⟨已持有⟩→上移止损/加固; 出现在 ⟨未持有信号⟩→多数已晚, 别追开盘(大跳空日内段均值转负)')
    p('  HOLDINGS 持仓止损 (你的真实持仓, 需开富途 OpenD):')
    p(f'    Qty 持有股数 · Stop(20wMA)={MA_WEEKLY}周线止损位 · vs Stop 距止损%')
    p(f'    Status: OK / APPROACHING(距止损≤{STOP_NEAR_PCT:.0%}逼近) / BREACHED(跌破→今日离场)')
    p('  POSITION MANAGEMENT 持仓管理 (对已有仓位的操作):')
    p(f'    MOVE STOP→breakeven: 浮盈≥{BREAKEVEN_PCT:.0f}% → 止损上移到成本价(锁不亏; 阈值高=留够缓冲, 正常回踩不被洗出)')
    p(f'    TRIM 50%: 浮盈≥{TRIM_PCT:.0f}% 且周收跌破{MA_WEEKLY_FAST}周线(动量转弱) → 减半 (走弱才减, 不按价格目标砍, 给爆发续涨留右尾)')
    p(f'    EXIT: 周收盘跌破{MA_WEEKLY}周线 → 清仓 (优先于止盈)')
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
    opts, _ = parser.parse_args()

    global _ASOF
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
    market_state, baro_info = get_market_state()
    logging.info(f'Market state: {market_state}')

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
        alerts = position_alerts(pos_df, weekly_cache)
        stop_status = position_stop_status(pos_df, weekly_cache)

    # Default output path. Under --asof, tag with the as-of date so a backtest
    # run never overwrites the live dated report.
    date_str = (_ASOF.strftime('%Y%m%d') if _ASOF is not None
                else datetime.datetime.now().strftime('%Y%m%d'))
    out_file = opts.output
    if out_file is None:
        out_dir = '/home/ryan/DATA/result'
        if os.path.isdir(out_dir):
            out_file = f'{out_dir}/us_tech_swing_{date_str}.txt'

    print_report(market_state, baro_info, results, alerts, out_file,
                 equity=ACCOUNT_EQUITY, stop_status=stop_status, futu_ok=futu_ok)

    # Machine-readable signal CSV (consumed by t_us_resonance.py)
    out_dir = '/home/ryan/DATA/result'
    if os.path.isdir(out_dir):
        try:
            write_signal_csv(results, f'{out_dir}/us_tech_signal_{date_str}.csv')
        except Exception as e:
            logging.warning(f'tech signal CSV write failed: {e}')


if __name__ == '__main__':
    main()
