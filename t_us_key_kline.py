# coding: utf-8
"""
US Key K-line Annotator  (关键K线标注脚本)

Given a single US ticker, identify its "key K-lines" over the past N months,
draw a candlestick chart (with a volume sub-panel), annotate each key bar with
its type, and print a text legend.

Philosophy (see docs/key_kline_methodology.md): a K-line's "key-ness" lives not
in its SHAPE but in its SUBJECT — position (key level) + volume. So every
detector below carries a volume/position condition; we never read shape naked.
This tool serves already-curated quality names (the watchlist); its job is
TIMING + STOP placement, not direction. We therefore hunt trend-CONTINUATION
bars (breakout / first-kiss / pocket-pivot / earnings-gap) and skip
bottom-reversal bottom-fishing. Exit logic stays in t_us_tech_swing.py.

Data layer is reused from t_us_tech_swing.py (ADR-0001: yfinance is the only
bar source, with stale-cache fallback). Earnings dates come from
yfinance get_earnings_dates(); offline / failure degrades gracefully.

Usage:
  python t_us_key_kline.py --ticker NVDA                  # default period=1y
  python t_us_key_kline.py --ticker NVDA --period 2y
  python t_us_key_kline.py --ticker NVDA --output /tmp/nvda.png
  python t_us_key_kline.py --ticker NVDA --no-earnings    # skip earnings fetch (offline)

CWD is assumed to be the repo root /home/ryan/tushare_ryan.

See docs/key_kline_blueprint.md for the full spec.
"""

import os
import sys
import logging
import datetime
from optparse import OptionParser

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use('Agg')  # headless: write PNG, never pop a window
import matplotlib.pyplot as plt
import mplfinance as mpf
import yfinance as yf

import tabulate as tab_mod

# Reuse the yfinance cache + fetch layer from the swing scanner (do NOT re-fetch
# bars another way). Importing it triggers its module-level _load_watchlist(),
# which only reads select.yml; Futu is imported lazily inside functions, so this
# import does not touch the network or OpenD.
from t_us_tech_swing import _fetch_daily

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)


def _setup_cjk_font():
    """Return a FontProperties for a system CJK font, or None.

    DejaVu Sans (matplotlib's default) has no CJK glyphs, and mplfinance styles
    overwrite the font rcParams — so rather than fight rcParams we apply this
    FontProperties directly to every text object (title + annotations) so
    突破/初吻/财报 render instead of tofu boxes. None → labels may show as boxes.
    """
    from matplotlib import font_manager
    preferred = ['Noto Sans CJK SC', 'Noto Sans CJK JP', 'WenQuanYi Micro Hei',
                 'Droid Sans Fallback']
    by_name = {f.name: f.fname for f in font_manager.fontManager.ttflist}
    for name in preferred:
        if name in by_name:
            matplotlib.rcParams['axes.unicode_minus'] = False
            return font_manager.FontProperties(fname=by_name[name])
    logging.warning('No CJK font found — Chinese annotations may render as boxes.')
    return None


CJK_FONT = _setup_cjk_font()

# ── Constants (all thresholds live here; tune in one place) ────────────────────
RESULT_DIR   = '/home/ryan/DATA/result'

PERIOD_DAYS  = {'3mo': 90, '6mo': 180, '1y': 365, '2y': 730}

VOL_AVG_N    = 20      # window for the "average volume" baseline

# 5.1 Breakout
CONSOL_DAYS  = 60      # look-back window for the consolidation range high
VOL_MULT     = 1.5     # breakout day volume must be >= VOL_MULT * vol_avg20
BREAKOUT_GAP = 10      # min bars between two breakout labels (de-dup)
RANGE_ON     = 'high'  # build range_high from 'high' or 'close'

# 5.2 First kiss (pullback to rising MA20)
KISS_LOOKAHEAD = 60    # search this many bars after a breakout
KISS_TOL_UP    = 0.015 # low may poke 1.5% above MA20
KISS_TOL_DOWN  = 0.03  # ... down to 3% below MA20
KISS_MA_SLOPE  = 5     # MA20 must be rising vs this many bars ago
KISS_VOL_DRY   = 1.0   # 初吻须缩量: volume <= KISS_VOL_DRY * vol_avg20 (主语=缩量)

# 5.3 Pocket pivot
PP_MIN_GAP   = 5       # min bars between pocket-pivot labels (de-dup)
PP_DOWN_LOOK = 10      # compare today's vol vs max down-day vol over prior N bars

# 5.4 Earnings gap
EARN_LIMIT   = 12      # how many earnings dates to pull
GAP_PCT      = 0.02    # open must gap up >= 2% over prior close
EARN_VOL_MULT = 1.5    # 财报跳空续涨命须放量: volume >= EARN_VOL_MULT * vol_avg20
# double-fate classification
FATE_POS_HI  = 0.50    # close-in-range >= 0.5  → upper half (continuation)
FATE_POS_LO  = 0.33    # close-in-range <  0.33 → lower third (trap)

# 5.5 Volume climax (optional, event-only)
CLIMAX_VOL_MULT = 2.5
CLIMAX_RNG_MULT = 1.5  # day range >= 1.5 * ATR(14) approx

# Annotation colors per type
TYPE_COLORS = {
    'BREAKOUT':     '#1a9850',  # green
    'FIRST_KISS':   '#2c7fb8',  # blue
    'POCKET_PIVOT': '#756bb1',  # purple
    'EARNINGS_GAP': '#d73027',  # red (overridden by fate below)
    'CLIMAX':       '#999999',  # grey
}
FATE_COLORS = {
    'CONTINUATION': '#d4af37',  # gold
    'TRAP':         '#d73027',  # red
}


# ── Data / indicators ──────────────────────────────────────────────────────────
def prepare_frame(ticker: str, period: str) -> pd.DataFrame:
    """Attach indicators on the FULL history, then slice to `period`.

    Indicators (MA / vol-avg / ATR / range-high) are computed BEFORE slicing so
    they are "warm" at the left edge of the displayed window — otherwise MA50/
    MA150 and the 60-day breakout range would be NaN / truncated for the first
    weeks of the chart, silently swallowing early signals (review #4 warmup)."""
    df = _fetch_daily(ticker)
    if df.empty:
        return df

    df['ma20']      = df['close'].rolling(20).mean()
    df['ma50']      = df['close'].rolling(50).mean()
    df['ma150']     = df['close'].rolling(150).mean()
    df['vol_avg20'] = df['volume'].rolling(VOL_AVG_N).mean()
    # Consolidation-range high over the prior CONSOL_DAYS bars (shift(1) excludes
    # today); precomputed on full history so a breakout near the window's left
    # edge sees its real prior range, not a window-truncated one.
    df['range_high'] = df[RANGE_ON].rolling(CONSOL_DAYS).max().shift(1)
    # ATR(14) approximation for the climax detector
    prev_close = df['close'].shift(1)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - prev_close).abs(),
        (df['low'] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()

    n_days = PERIOD_DAYS.get(period, 365)
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=n_days)
    return df[df.index >= cutoff].copy()


# ── Detectors ───────────────────────────────────────────────────────────────────
# Each returns a list of KeyBar dicts:
#   {'date': Timestamp, 'type': str, 'price': float, 'stop': float|None,
#    'note': str, 'fate': str|None}
# 'stop' = the WHERE (止损位) this entry-type bar implies; None for event-only bars.

def detect_breakout(df: pd.DataFrame) -> list:
    """5.1 Platform breakout: first close above the CONSOL_DAYS-range high on
    volume, while the broader trend is up (close > MA150 when available)."""
    out = []
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    vol = df['volume'].values
    vavg = df['vol_avg20'].values
    rng_hi = df['range_high'].values
    ma150 = df['ma150'].values
    last_idx = -10**9

    for i in range(1, len(df)):
        if np.isnan(vavg[i]) or vavg[i] <= 0:
            continue
        range_high = rng_hi[i]  # prior-CONSOL_DAYS high, today excluded
        if np.isnan(range_high):
            continue
        # Trend gate (review #5): a breakout counts as continuation only if the
        # stock leads its own long-term MA. NaN MA150 (short history) → allow.
        if not np.isnan(ma150[i]) and close[i] <= ma150[i]:
            continue
        first_cross = close[i] > range_high and close[i - 1] <= range_high
        if not first_cross:
            continue
        ratio = vol[i] / vavg[i]
        if ratio < VOL_MULT:
            continue
        if i - last_idx < BREAKOUT_GAP:
            continue
        last_idx = i
        out.append({
            'date': df.index[i], 'type': 'BREAKOUT', 'price': float(high[i]),
            'note': f'放量突破{CONSOL_DAYS}日高 vol×{ratio:.1f}', 'fate': None,
            'stop': float(low[i]),  # 跌破突破当根 low → 止损
        })
    return out


def detect_first_kiss(df: pd.DataFrame, breakouts: list) -> list:
    """5.2 First pullback that kisses a rising MA20 on DRYING volume and closes
    green. The 缩量 condition is this bar's "subject": a pullback to MA20 on heavy
    volume is distribution, not a healthy first kiss (方法论 §3.2, review #1)."""
    out = []
    seen = set()
    idx = df.index
    low = df['low'].values
    close = df['close'].values
    open_ = df['open'].values
    ma20 = df['ma20'].values
    ma50 = df['ma50'].values
    vol = df['volume'].values
    vavg = df['vol_avg20'].values

    for bk in breakouts:
        b = idx.get_loc(bk['date'])
        if isinstance(b, slice):
            b = b.start
        end = min(b + KISS_LOOKAHEAD, len(df) - 1)
        for k in range(b + 1, end + 1):
            if k - KISS_MA_SLOPE < 0:
                continue
            if np.isnan(ma20[k]) or np.isnan(ma50[k]) or np.isnan(ma20[k - KISS_MA_SLOPE]):
                continue
            if np.isnan(vavg[k]) or vavg[k] <= 0:
                continue
            touch = (low[k] <= ma20[k] * (1 + KISS_TOL_UP)
                     and low[k] >= ma20[k] * (1 - KISS_TOL_DOWN))
            rising = ma20[k] > ma20[k - KISS_MA_SLOPE]
            trend_ok = close[k] > ma50[k]
            green = close[k] > open_[k]
            dry = vol[k] <= KISS_VOL_DRY * vavg[k]  # 缩量 = 真·初吻
            if touch and rising and trend_ok and green and dry:
                if idx[k] in seen:        # another breakout already kissed here
                    break
                seen.add(idx[k])
                out.append({
                    'date': idx[k], 'type': 'FIRST_KISS', 'price': float(low[k]),
                    'note': f'突破后首次回踩MA20·缩量站稳 vol×{vol[k] / vavg[k]:.1f}',
                    'fate': None,
                    'stop': float(low[k]),  # 跌破初吻 low → 止损
                })
                break  # only the FIRST kiss per breakout
    return out


def detect_pocket_pivot(df: pd.DataFrame) -> list:
    """5.3 Up-day whose volume tops every down-day's volume over the prior 10 bars,
    while price sits above MA50."""
    out = []
    idx = df.index
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    vol = df['volume'].values
    ma50 = df['ma50'].values
    last_idx = -10**9

    for i in range(PP_DOWN_LOOK + 1, len(df)):
        if np.isnan(ma50[i]):
            continue
        if not (close[i] > close[i - 1]):
            continue
        if not (close[i] > ma50[i]):
            continue
        down_vols = [vol[j] for j in range(i - PP_DOWN_LOOK, i)
                     if close[j] < close[j - 1]]
        if not down_vols:
            continue
        if vol[i] <= max(down_vols):
            continue
        if i - last_idx < PP_MIN_GAP:
            continue
        last_idx = i
        out.append({
            'date': idx[i], 'type': 'POCKET_PIVOT', 'price': float(high[i]),
            'note': 'Pocket Pivot 加油', 'fate': None,
            'stop': float(low[i]),  # 跌破加油根 low / MA50 → 止损
        })
    return out


def detect_earnings_gaps(df: pd.DataFrame, ticker: str, fetch: bool = True) -> list:
    """5.4 Earnings-day up-gap (王中之王) + continuation/trap double-fate.

    Degrades gracefully: any failure / empty result / --no-earnings → []."""
    if not fetch:
        logging.info('Earnings fetch skipped (--no-earnings)')
        return []
    try:
        ed = yf.Ticker(ticker).get_earnings_dates(limit=EARN_LIMIT)
    except Exception as e:
        logging.warning(f'{ticker}: get_earnings_dates failed ({e}) — no earnings annotations')
        return []
    if ed is None or ed.empty:
        logging.warning(f'{ticker}: no earnings dates returned — no earnings annotations')
        return []

    # tz-aware index (America/New_York) → naive dates for alignment
    earn_dates = pd.to_datetime(ed.index).tz_localize(None)
    surprise = (ed['Surprise(%)'].values
                if 'Surprise(%)' in ed.columns else [np.nan] * len(ed))

    idx = df.index
    open_ = df['open'].values
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    vol = df['volume'].values
    vavg = df['vol_avg20'].values

    out = []
    seen = set()
    for ed_ts, sp in zip(earn_dates, surprise):
        # Reaction bar = first bar dated >= earnings date. Earnings print after
        # the close (16:00 ET), so the reaction is usually the NEXT session.
        g = idx.searchsorted(ed_ts.normalize(), side='left')
        if g <= 0 or g >= len(df):
            continue  # earnings before our window starts, or in the future
        if g in seen:
            continue
        seen.add(g)

        if open_[g] <= close[g - 1] * (1 + GAP_PCT):
            continue  # not an up-gap

        rng = high[g] - low[g]
        pos = (close[g] - low[g]) / rng if rng > 0 else 0.5
        held = close[g] >= close[g - 1]
        filled = low[g] <= close[g - 1]
        # Volume is the gap's "subject": 放量=真主语在抢筹 (方法论 §4). A
        # low-volume up-gap is not 王中之王 and cannot earn 续涨命 (review #2).
        vol_ratio = vol[g] / vavg[g] if (not np.isnan(vavg[g]) and vavg[g] > 0) else np.nan
        vol_ok = (not np.isnan(vol_ratio)) and vol_ratio >= EARN_VOL_MULT

        if (close[g] < close[g - 1]) or filled or (pos < FATE_POS_LO):
            fate, fate_cn = 'TRAP', '高开低走陷阱'
        elif held and pos >= FATE_POS_HI and vol_ok:
            fate, fate_cn = 'CONTINUATION', '续涨命'
        elif held and pos >= FATE_POS_HI:
            fate, fate_cn = 'CONTINUATION', '缩量续涨·存疑'
        else:
            fate, fate_cn = 'CONTINUATION', '中性·需观察'

        vol_tag = f' vol×{vol_ratio:.1f}' if not np.isnan(vol_ratio) else ''
        if not np.isnan(sp):
            beat = '财报↑(beat)' if sp > 0 else ('财报↓(miss)' if sp < 0 else '财报(inline)')
            note = f'{beat} {fate_cn} (Surprise {sp:+.1f}%){vol_tag}'
        else:
            note = f'财报跳空 {fate_cn}{vol_tag}'

        out.append({
            'date': idx[g], 'type': 'EARNINGS_GAP', 'price': float(high[g]),
            'note': note, 'fate': fate,
            'stop': float(close[g - 1]),  # 跌破缺口下沿(前收) → 止损
        })
    return out


def detect_volume_climax(df: pd.DataFrame) -> list:
    """5.5 Climax volume — flagged as an event only, no direction read."""
    out = []
    idx = df.index
    high = df['high'].values
    low = df['low'].values
    vol = df['volume'].values
    vavg = df['vol_avg20'].values
    atr = df['atr14'].values

    for i in range(len(df)):
        if np.isnan(vavg[i]) or vavg[i] <= 0 or np.isnan(atr[i]) or atr[i] <= 0:
            continue
        big_vol = vol[i] >= CLIMAX_VOL_MULT * vavg[i]
        big_rng = (high[i] - low[i]) >= CLIMAX_RNG_MULT * atr[i]
        if big_vol and big_rng:
            out.append({
                'date': idx[i], 'type': 'CLIMAX', 'price': float(high[i]),
                'note': f'高潮量 vol×{vol[i] / vavg[i]:.1f}', 'fate': None,
                'stop': None,  # event-only, no entry → no stop
            })
    return out


# ── Aggregation ─────────────────────────────────────────────────────────────────
# Priority for resolving same-day collisions / draw order (high → low).
_TYPE_PRIORITY = {
    'EARNINGS_GAP': 0, 'BREAKOUT': 1, 'FIRST_KISS': 2,
    'POCKET_PIVOT': 3, 'CLIMAX': 4,
}


def collect_key_bars(df: pd.DataFrame, ticker: str, fetch_earnings: bool = True) -> list:
    """Run every detector, sort, and report (does not drop same-day duplicates —
    a bar can legitimately be both an earnings gap and a breakout)."""
    breakouts = detect_breakout(df)
    bars = []
    bars += breakouts
    bars += detect_first_kiss(df, breakouts)
    bars += detect_pocket_pivot(df)
    bars += detect_earnings_gaps(df, ticker, fetch=fetch_earnings)
    bars += detect_volume_climax(df)
    bars.sort(key=lambda b: (b['date'], _TYPE_PRIORITY.get(b['type'], 9)))
    return bars


# ── Plotting ─────────────────────────────────────────────────────────────────────
def _annotation_color(kb: dict) -> str:
    if kb['type'] == 'EARNINGS_GAP' and kb.get('fate') in FATE_COLORS:
        return FATE_COLORS[kb['fate']]
    return TYPE_COLORS.get(kb['type'], '#000000')


def _annotation_label(kb: dict) -> str:
    t = kb['type']
    if t == 'BREAKOUT':
        return '突破'
    if t == 'FIRST_KISS':
        return '初吻'
    if t == 'POCKET_PIVOT':
        return 'PP'
    if t == 'EARNINGS_GAP':
        arrow = '财报↑' if '续涨' in kb['note'] or 'beat' in kb['note'] else '财报'
        tag = '续涨' if kb.get('fate') == 'CONTINUATION' else '陷阱'
        return f'{arrow}·{tag}'
    if t == 'CLIMAX':
        return '量'
    return t


def plot_chart(df: pd.DataFrame, key_bars: list, ticker: str, out_png: str):
    """Render candles + volume + MA20/MA50, then annotate each key bar.

    mplfinance's primary x-axis is INTEGER position (not date), so we map each
    key bar's date to its iloc position before annotating (see blueprint §9)."""
    start = df.index[0].strftime('%Y-%m-%d')
    end = df.index[-1].strftime('%Y-%m-%d')

    mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', inherit=True)
    style = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc,
                               gridstyle=':', y_on_right=False)

    plot_df = df[['open', 'high', 'low', 'close', 'volume']]
    fig, axes = mpf.plot(
        plot_df, type='candle', volume=True, mav=(20, 50),
        style=style, figsize=(16, 9), returnfig=True,
        ylabel='Price', ylabel_lower='Volume',
        warn_too_much_data=10**6,
    )
    ax = axes[0]
    # Set the title ourselves so we can apply the CJK font (mplfinance's own
    # title path goes through styled rcParams that lack CJK glyphs).
    fig.suptitle(f'{ticker} 关键K线  {start}~{end}',
                 fontproperties=CJK_FONT, fontsize=14, fontweight='bold')
    span = df['high'].max() - df['low'].min()
    offset = span * 0.04 if span > 0 else 1.0

    # Stagger same-position labels so they don't overprint.
    pos_count = {}
    for kb in key_bars:
        try:
            pos = df.index.get_loc(kb['date'])
        except KeyError:
            continue
        if isinstance(pos, slice):
            pos = pos.start

        below = kb['type'] == 'FIRST_KISS'
        n = pos_count.get((pos, below), 0)
        pos_count[(pos, below)] = n + 1

        color = _annotation_color(kb)
        label = _annotation_label(kb)
        if below:
            y = kb['price'] - offset * (1 + n)
            ytext = y - offset * 1.5
            va = 'top'
        else:
            y = kb['price'] + offset * (1 + n)
            ytext = y + offset * 1.5
            va = 'bottom'

        ax.annotate(
            label, xy=(pos, kb['price']), xytext=(pos, ytext),
            ha='center', va=va, fontsize=9, fontweight='bold', color=color,
            fontproperties=CJK_FONT,
            arrowprops=dict(arrowstyle='->', color=color, lw=1.2),
        )

    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    fig.savefig(out_png, dpi=110, bbox_inches='tight')
    plt.close(fig)
    logging.info(f'Chart saved: {out_png}')


# ── Text legend ──────────────────────────────────────────────────────────────────
def print_legend(key_bars: list, ticker: str):
    if not key_bars:
        print(f'\n{ticker}: 未检测到关键K线(数据不足或区间内无信号)。')
        return
    rows = []
    for kb in key_bars:
        stop = kb.get('stop')
        rows.append([
            kb['date'].strftime('%Y-%m-%d'),
            kb['type'],
            f"{kb['price']:.2f}",
            f"{stop:.2f}" if stop is not None else '—',
            kb.get('fate') or '—',
            kb['note'],
        ])
    print(f'\n{ticker} 关键K线图例')
    print(tab_mod.tabulate(rows, headers=['日期', '类型', '价位', '止损', '命', '说明'],
                           tablefmt='github'))
    counts = {}
    for kb in key_bars:
        counts[kb['type']] = counts.get(kb['type'], 0) + 1
    summary = '  '.join(f'{k}×{v}' for k, v in sorted(counts.items()))
    print(f'\n统计: {summary}  (共 {len(key_bars)} 根)')


# ── Main ─────────────────────────────────────────────────────────────────────────
def main():
    parser = OptionParser(usage='%prog --ticker SYM [options]')
    parser.add_option('--ticker', dest='ticker', default=None,
                      help='US ticker to analyze (required)')
    parser.add_option('--period', dest='period', default='1y',
                      help='look-back window: 3mo|6mo|1y|2y (default 1y)')
    parser.add_option('--output', dest='output', default=None,
                      help='PNG output path (default: result/key_kline_<ticker>_<date>.png)')
    parser.add_option('--no-earnings', dest='no_earnings', action='store_true',
                      default=False, help='Skip earnings fetch (offline mode)')
    opts, _ = parser.parse_args()

    if not opts.ticker:
        parser.error('--ticker is required')
    ticker = opts.ticker.upper()
    period = opts.period

    logging.info(f'Key K-line scan: {ticker} period={period}')
    df = prepare_frame(ticker, period)
    if df.empty:
        logging.error(f'{ticker}: no bar data available — aborting')
        sys.exit(1)

    if df['ma50'].notna().sum() == 0:
        logging.warning(f'{ticker}: 数据不足 ({len(df)} bars) — MA50 never forms; '
                        'breakout/pocket-pivot detectors may yield nothing.')
    if len(df) < CONSOL_DAYS + BREAKOUT_GAP:
        logging.warning(f'{ticker}: 区间仅 {len(df)} bar < 突破回看 {CONSOL_DAYS} 日 — '
                        'breakout/初吻 基本不会触发,建议加大 --period(如 1y/2y)。')

    key_bars = collect_key_bars(df, ticker, fetch_earnings=not opts.no_earnings)

    out_png = opts.output
    if out_png is None:
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        base = RESULT_DIR if os.path.isdir(RESULT_DIR) else '.'
        out_png = f'{base}/key_kline_{ticker}_{date_str}.png'

    plot_chart(df, key_bars, ticker, out_png)
    print_legend(key_bars, ticker)


if __name__ == '__main__':
    main()
