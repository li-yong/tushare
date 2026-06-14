# coding: utf-8
"""
US Steady-Climber Screener  —  "小步慢涨, 跌一点就很快补回去"

Finds stocks whose price grinds upward in small, low-volatility steps and
whose dips are bought back quickly (short time spent under water). This is the
opposite of a high-beta name that lurches up and crashes; we want the smooth,
staircase-style climber.

The four traits, made quantitative over a lookback window (default 90 bars):

  小步 (small steps)      : low daily-return std, no single-day crash (> -12%;
                            tolerates one sharp earnings reaction, blocks shocks)
  慢涨 (slow climb)       : positive slope of log(close) linear regression
  稳 (hugs the trendline) : high R^2 of that regression  → smooth, not jumpy
  快补回 (fast recovery)  : short max "underwater" streak (days below prior peak)
                            + shallow max drawdown

Ranking score (higher = better):
    steady_score = R^2 * Sharpe / recovery_penalty
  where Sharpe = annualised_return / annualised_vol, and recovery_penalty grows
  with both max drawdown and the longest underwater streak.

Data source: yfinance (split/div-adjusted), sharing the swing scanner's per-
ticker cache at /home/ryan/DATA/DAY_Global/US_yf (ADR-0001).

Usage:
  python t_us_steady_climb.py                       # scan NASDAQ-100 (default)
  python t_us_steady_climb.py --universe sp500      # scan S&P 500
  python t_us_steady_climb.py --tickers NVDA,MSFT   # scan a custom list
  python t_us_steady_climb.py --lookback 120 --top 30
  python t_us_steady_climb.py --min-r2 0.7 --max-underwater 15   # stricter
"""

import os
import sys
import csv
import logging
import datetime
from optparse import OptionParser

import numpy as np
import pandas as pd
import yfinance as yf

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# ── Shared cache (same dir/format as t_us_tech_swing.py, ADR-0001) ────────────
BAR_CACHE_DIR    = '/home/ryan/DATA/DAY_Global/US_yf'
BAR_FETCH_PERIOD = '2y'
RESULT_DIR       = '/home/ryan/DATA/result'
UNIVERSE_DIR     = '/home/ryan/DATA/pickle/INDEX_US_HK'
UNIVERSE_FILES   = {
    'ndx':    'nasdqa100.csv',
    'nasdaq': 'nasdqa100.csv',
    'sp500':  'sp500.csv',
    'sp400':  'sp400.csv',
    'dow':    'dow.csv',
}

_DAILY_MEMO = {}


def _cache_path(ticker):
    return os.path.join(BAR_CACHE_DIR, f'{ticker}.csv')


def _cache_is_fresh(path):
    """True if the cache was written today (skip the network)."""
    if not os.path.exists(path):
        return False
    mtime = datetime.date.fromtimestamp(os.path.getmtime(path))
    return mtime == datetime.date.today()


def _read_cache(ticker):
    path = _cache_path(ticker)
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=['date'])
    return df.set_index('date')


def fetch_daily(ticker):
    """Daily OHLCV from yfinance, split/div-adjusted, cached per ticker.

    Mirrors t_us_tech_swing._fetch_daily: today's cache is trusted; on a fetch
    failure we serve the last-good cache with a staleness warning.
    """
    if ticker in _DAILY_MEMO:
        return _DAILY_MEMO[ticker]

    path = _cache_path(ticker)
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
            logging.warning(f'{ticker}: yfinance failed ({e}) and no cache — skip')
        else:
            last = stale.index[-1].date()
            age = (datetime.date.today() - last).days
            logging.warning(f'{ticker}: yfinance failed ({e}) — STALE cache, '
                            f'last bar {last} ({age}d old)')
        _DAILY_MEMO[ticker] = stale
        return stale


def load_universe(name):
    """Return a list of tickers for a named index, from the local membership CSV."""
    fname = UNIVERSE_FILES.get(name.lower())
    if not fname:
        raise ValueError(f'unknown universe {name!r}; choose from '
                         f'{sorted(set(UNIVERSE_FILES))}')
    path = os.path.join(UNIVERSE_DIR, fname)
    df = pd.read_csv(path)
    return [str(c).strip().upper() for c in df['code'].dropna()]


# ── Metrics ───────────────────────────────────────────────────────────────────

def _max_underwater_days(close):
    """Longest run of bars where close sits below its prior running peak.

    This is the "跌一点就很快补回去" measure: a resilient staircase climber
    reclaims new highs fast, so its longest underwater streak stays short.
    """
    peak = close.cummax()
    underwater = (close < peak).to_numpy()
    best = run = 0
    for u in underwater:
        run = run + 1 if u else 0
        best = max(best, run)
    return best


def analyse(ticker, lookback):
    """Compute steady-climb metrics over the last `lookback` bars. None if too short."""
    df = fetch_daily(ticker)
    if df.empty or len(df) < lookback + 5:
        return None

    win = df.iloc[-lookback:]
    close = win['close']
    rets = close.pct_change().dropna()
    if len(rets) < lookback // 2 or close.iloc[0] <= 0:
        return None

    # log-price linear regression → slope (trend) and R^2 (smoothness)
    y = np.log(close.to_numpy())
    x = np.arange(len(y), dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    fit = slope * x + intercept
    ss_res = float(np.sum((y - fit) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    ann_ret = (np.exp(slope * 252) - 1.0)          # annualised trend return
    daily_vol = float(rets.std())
    ann_vol = daily_vol * np.sqrt(252)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

    # drawdown / recovery
    peak = close.cummax()
    dd = close / peak - 1.0
    max_dd = float(dd.min())                        # most negative (<= 0)
    underwater_days = _max_underwater_days(close)
    worst_day = float(rets.min())
    up_ratio = float((rets > 0).mean())
    total_ret = float(close.iloc[-1] / close.iloc[0] - 1.0)
    avg_dollar_vol = float((win['close'] * win['volume']).mean())

    # composite: smooth + risk-adjusted climb, penalised by depth & duration of dips
    recovery_penalty = (1.0 + abs(max_dd) / 0.10) * (1.0 + underwater_days / 20.0)
    steady_score = max(r2, 0.0) * sharpe / recovery_penalty

    return {
        'ticker': ticker,
        'last': float(close.iloc[-1]),
        'total_ret': total_ret,
        'ann_ret': ann_ret,
        'r2': r2,
        'daily_vol': daily_vol,
        'sharpe': sharpe,
        'max_dd': max_dd,
        'underwater_days': underwater_days,
        'worst_day': worst_day,
        'up_ratio': up_ratio,
        'avg_dollar_vol': avg_dollar_vol,
        'steady_score': steady_score,
    }


def passes(m, o):
    """Hard filters for the four traits. Returns True if the stock qualifies."""
    return (
        m['ann_ret'] > o.min_ann_ret and                 # 慢涨: actually climbing
        m['r2'] >= o.min_r2 and                           # 稳: hugs the trendline
        m['daily_vol'] <= o.max_daily_vol and             # 小步: low volatility
        m['worst_day'] >= o.max_one_day_drop and          # 小步: no single crash
        m['max_dd'] >= o.max_drawdown and                 # shallow holes
        m['underwater_days'] <= o.max_underwater and      # 快补回: fast recovery
        m['avg_dollar_vol'] >= o.min_dollar_vol           # tradable liquidity
    )


# ── Reporting ─────────────────────────────────────────────────────────────────

def _fmt_pct(x):
    return f'{x * 100:+.1f}%'


def print_report(rows, o, scanned, output_file=None):
    lines = []
    lines.append('=' * 96)
    lines.append(f'US Steady-Climber Screen  小步慢涨·跌一点快补回   '
                 f'{datetime.date.today()}  (lookback={o.lookback} bars)')
    lines.append(f'universe={o.universe if not o.tickers else "custom"}  '
                 f'scanned={scanned}  passed={len(rows)}')
    lines.append('=' * 96)
    lines.append(
        f'{"#":>2} {"TICKER":<7}{"LAST":>9}{"TOTAL":>8}{"ANN":>8}'
        f'{"R2":>6}{"VOL":>7}{"SHARPE":>7}{"MAXDD":>8}{"U/W":>5}{"WORST":>8}'
        f'{"UP%":>6}{"SCORE":>8}'
    )
    lines.append('-' * 96)
    for i, m in enumerate(rows, 1):
        lines.append(
            f'{i:>2} {m["ticker"]:<7}{m["last"]:>9.2f}'
            f'{_fmt_pct(m["total_ret"]):>8}{_fmt_pct(m["ann_ret"]):>8}'
            f'{m["r2"]:>6.2f}{_fmt_pct(m["daily_vol"]):>7}{m["sharpe"]:>7.2f}'
            f'{_fmt_pct(m["max_dd"]):>8}{m["underwater_days"]:>5}'
            f'{_fmt_pct(m["worst_day"]):>8}{m["up_ratio"] * 100:>5.0f}%'
            f'{m["steady_score"]:>8.2f}'
        )
    lines.append('-' * 96)
    lines.append('TOTAL=区间涨幅  ANN=年化趋势  R2=平滑度(越高越像台阶式上行)  '
                 'VOL=日波动  U/W=最长水下天数(越小补回越快)')
    report = '\n'.join(lines)
    print(report)
    if output_file:
        with open(output_file, 'w') as fh:
            fh.write(report + '\n')
        logging.info(f'report written to {output_file}')


def write_csv(rows, path):
    if not rows:
        return
    cols = ['ticker', 'last', 'total_ret', 'ann_ret', 'r2', 'daily_vol',
            'sharpe', 'max_dd', 'underwater_days', 'worst_day', 'up_ratio',
            'avg_dollar_vol', 'steady_score']
    with open(path, 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for m in rows:
            w.writerow({c: m[c] for c in cols})
    logging.info(f'signals written to {path}')


def main():
    p = OptionParser()
    p.add_option('--universe', default='ndx',
                 help='index to scan: ndx|sp500|sp400|dow (default ndx)')
    p.add_option('--tickers', default=None,
                 help='comma-separated custom list, overrides --universe')
    p.add_option('--lookback', type='int', default=90,
                 help='trading-day window for the metrics (default 90)')
    p.add_option('--top', type='int', default=25,
                 help='show top N by steady_score (default 25)')
    p.add_option('--min-r2', dest='min_r2', type='float', default=0.60,
                 help='min regression R^2, the smoothness gate (default 0.60)')
    p.add_option('--min-ann-ret', dest='min_ann_ret', type='float', default=0.05,
                 help='min annualised trend return (default 0.05 = 5%)')
    p.add_option('--max-daily-vol', dest='max_daily_vol', type='float', default=0.035,
                 help='max daily-return std (default 0.035 = 3.5%)')
    p.add_option('--max-one-day-drop', dest='max_one_day_drop', type='float',
                 default=-0.12, help='worst single-day return allowed (default -0.12; '
                      'tolerates one earnings gap, still blocks crash days)')
    p.add_option('--max-drawdown', dest='max_drawdown', type='float', default=-0.20,
                 help='max drawdown allowed, e.g. -0.20 (default -0.20)')
    p.add_option('--max-underwater', dest='max_underwater', type='int', default=35,
                 help='max longest underwater streak in bars (default 35; lower = '
                      'stricter "fast recovery")')
    p.add_option('--min-dollar-vol', dest='min_dollar_vol', type='float', default=5e6,
                 help='min avg daily $-volume for liquidity (default 5e6)')
    p.add_option('--no-file', action='store_true', default=False,
                 help='do not write report/CSV to the result dir')
    o, _ = p.parse_args()

    if o.tickers:
        universe = [t.strip().upper() for t in o.tickers.split(',') if t.strip()]
    else:
        universe = load_universe(o.universe)
    logging.info(f'scanning {len(universe)} tickers...')

    rows = []
    for i, t in enumerate(universe, 1):
        try:
            m = analyse(t, o.lookback)
        except Exception as e:
            logging.warning(f'{t}: analysis failed ({e})')
            continue
        if m and passes(m, o):
            rows.append(m)
        if i % 25 == 0:
            logging.info(f'  ...{i}/{len(universe)}')

    rows.sort(key=lambda m: m['steady_score'], reverse=True)
    top = rows[:o.top]

    out_txt = out_csv = None
    if not o.no_file:
        os.makedirs(RESULT_DIR, exist_ok=True)
        stamp = datetime.date.today().isoformat()
        out_txt = os.path.join(RESULT_DIR, f'us_steady_climb_{stamp}.txt')
        out_csv = os.path.join(RESULT_DIR, f'us_steady_climb_{stamp}.csv')

    print_report(top, o, scanned=len(universe), output_file=out_txt)
    if out_csv:
        write_csv(rows, out_csv)   # full passing set, not just top N


if __name__ == '__main__':
    main()
