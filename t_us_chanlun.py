# coding: utf-8
"""
US Chanlun Annotator  (缠论标注脚本 · 笔中枢简化版)

Single-ticker analysis tool (缠论版 t_us_key_kline): given a US ticker, build
the 缠论 structure on DAILY bars — 包含处理→分型→笔→笔中枢 — mark the three
classes of buy/sell points (1B/2B/3B/1S/2S/3S), draw an annotated candlestick
chart, and print a 现状 block (当前走势类型 / 最近信号 / 失效位 = natural stop
anchor / 背驰状态). --ticker is hand-run; --hold runs daily from us_daily_run.sh
(tee'd to result/us_chanlun/us_chanlun_hold_<date>.txt) as an exit REFERENCE
layer — exit policy stays with the swing scanner (ADR-0002). No ledger writes.

Design decisions (grilled 2026-07-04):
  • 纯日线单级别. "次级别" is approximated by the 笔 (缠师-sanctioned practical
    shortcut). No hourly layer — yfinance hourly only reaches back 730d and
    would kill --asof backtests; 区间套 precision entry is a phase-2 idea.
  • 笔中枢简化: 中枢 = ≥3 consecutive 笔 overlapping. The 线段 layer (特征序列)
    is deliberately skipped — it is the most ambiguous / bug-prone part of every
    chanlun implementation, and US overnight gaps land exactly on its murkiest
    rule (缺口两情况). Daily 笔中枢 unfolds over weeks ≈ the swing horizon.
  • 新笔近似: a stroke needs its two fractal extremes ≥ NEW_PEN_GAP merged bars
    apart (fractals do not share bars). 老笔/新笔之争 resolved pragmatically.
  • 背驰 = 价格创新高/低 (hard precondition) + MACD |hist| area < DIVERGENCE_RATIO ×
    the previous same-direction leaving stroke + ≥2 中枢 in trend arrangement.
    盘整背驰 (single 中枢) is ONLY reported in the 现状 block, never marked as a
    signal — first gate against 背了又背 (indicator-design-state-vs-debt: the
    reading is a GPS fix, not a debt to collect).
  • US gaps: a >GAP_PCT overnight gap does NOT alter the structure algorithm
    (merging/fractals run as usual) but is flagged on the chart and, if recent,
    downgrades structural confidence in the 现状 block. Honest annotation, not a
    hard rule.
  • 中枢延伸: later strokes still overlapping [ZD,ZG] extend the pivot; NO 九段
    level-upgrade (single-level tool).
  • Data layer reused from t_us_tech_swing (ADR-0001: yfinance only, stale-cache
    fallback). --asof follows the house convention (sets _sw._ASOF).
  • Fixed left edge (MU/NVDA validation, 2026-07-04): structure is always
    computed on FULL history; --period only limits drawing/printing. A rolling
    window makes the division path-dependent — committed signals vanished
    months later as their supporting pivots slid off the left edge.
  • 实际确认日: every displayed signal carries the first date it was actually
    detectable (stroke endpoints commit only with later bars; melt-up 3Bs
    confirm at +12~21% above the printed price). Chart labels sit on the
    confirmation bar; the structural extreme is the hollow circle.

Usage:
  python t_us_chanlun.py --ticker NVDA                # chart + 现状 (default 2y)
  python t_us_chanlun.py --ticker NVDA --period 1y
  python t_us_chanlun.py --ticker NVDA --output /tmp/nvda_chan.png
  python t_us_chanlun.py --hold                       # US_SWING_STOPS 持仓体检表
                                                      # (sell-side focused, no plots)
  python t_us_chanlun.py --ticker MU --asof 2025-05-01   # point-in-time replay

CWD is assumed to be the repo root /home/ryan/tushare_ryan.
"""

import os
import logging
import datetime
from optparse import OptionParser

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use('Agg')  # headless: write PNG, never pop a window
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
import mplfinance as mpf
import tabulate as tab_mod

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Reuse the yfinance cache + fetch layer from the swing scanner (do NOT re-fetch
# bars another way). Futu is imported lazily inside its functions, so this
# import touches neither the network nor OpenD.
import t_us_tech_swing as _sw
from t_us_tech_swing import _fetch_daily


def _setup_cjk_font():
    """FontProperties for a system CJK font, or None (labels become tofu)."""
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
RESULT_DIR       = '/home/ryan/DATA/result'
WATCHLIST_FILE   = '/home/ryan/tushare_ryan/select.yml'

PERIOD_DAYS      = {'6mo': 180, '1y': 365, '2y': 730, '3y': 1095}

NEW_PEN_GAP      = 4      # 新笔近似: fractal extremes ≥4 merged bars apart
PIVOT_PENS       = 3      # 中枢 = at least 3 consecutive overlapping strokes
DIVERGENCE_RATIO = 0.8    # 背驰: leaving-stroke |hist| area < 0.8 × previous leg
GAP_PCT          = 0.05   # overnight gap flagged when |open/prev_close-1| > 5%
GAP_LOOKBACK     = 20     # 现状 block warns if a big gap sits in the last N bars
CONF_MAX_BARS    = 30     # confirmation replay horizon per signal (MU: 1-6 td)

# Point-in-time backtest anchor (set via --asof). None = live. Data truncation
# rides on t_us_tech_swing's _fetch_daily (we set _sw._ASOF); this local copy
# anchors filenames + the 现状 header date.
_ASOF: 'pd.Timestamp | None' = None


def _now() -> 'pd.Timestamp':
    return _ASOF if _ASOF is not None else pd.Timestamp.today().normalize()


# ════════════════════════════════════════════════════════════════════════════
# 1. 结构构件: 包含处理 → 分型 → 笔 → 笔中枢
# ════════════════════════════════════════════════════════════════════════════

def merge_bars(df: pd.DataFrame) -> list:
    """包含处理 (inclusion merging) on the display window.

    Returns a list of merged bars: {'h','l','ih','il'} where ih/il are the
    window ilocs of the ORIGINAL bar supplying that extreme (fractal extremes
    must land on real bars for plotting). Direction rule: 向上处理取高高,
    向下处理取低低. Gaps need no special casing here — non-overlap just means
    no inclusion.
    """
    highs, lows = df['high'].values, df['low'].values
    merged, direction = [], 1
    for i in range(len(df)):
        h, l = float(highs[i]), float(lows[i])
        if not merged:
            merged.append({'h': h, 'l': l, 'ih': i, 'il': i})
            continue
        last = merged[-1]
        contains = (h >= last['h'] and l <= last['l']) or (h <= last['h'] and l >= last['l'])
        if contains:
            if direction == 1:                      # 向上: 高点取高, 低点取高
                if h > last['h']:
                    last['h'], last['ih'] = h, i
                if l > last['l']:
                    last['l'], last['il'] = l, i
            else:                                   # 向下: 高点取低, 低点取低
                if h < last['h']:
                    last['h'], last['ih'] = h, i
                if l < last['l']:
                    last['l'], last['il'] = l, i
        else:
            direction = 1 if h > last['h'] else -1
            merged.append({'h': h, 'l': l, 'ih': i, 'il': i})
    return merged


def find_fractals(merged: list) -> list:
    """分型 on merged bars: {'kind':'top'|'bot', 'mi', 'i', 'price'}.

    After inclusion merging adjacent bars cannot tie, so strict > suffices.
    """
    out = []
    for m in range(1, len(merged) - 1):
        a, b, c = merged[m - 1], merged[m], merged[m + 1]
        if b['h'] > a['h'] and b['h'] > c['h']:
            out.append({'kind': 'top', 'mi': m, 'i': b['ih'], 'price': b['h']})
        elif b['l'] < a['l'] and b['l'] < c['l']:
            out.append({'kind': 'bot', 'mi': m, 'i': b['il'], 'price': b['l']})
    return out


def find_points(fractals: list) -> list:
    """笔端点序列: alternate top/bot fractals, 新笔 gap rule, extreme-replace.

    Same-kind successor that is more extreme replaces the provisional endpoint;
    an opposite fractal closer than NEW_PEN_GAP merged bars is noise and
    skipped. Guard against the rare monotonic-run inversion (a 'bot' printing
    above the pending 'top') by skipping it.
    """
    if not fractals:
        return []
    pts = [fractals[0]]
    for f in fractals[1:]:
        last = pts[-1]
        if f['kind'] == last['kind']:
            more_extreme = (f['price'] > last['price']) if f['kind'] == 'top' \
                else (f['price'] < last['price'])
            if more_extreme:
                pts[-1] = f
        else:
            if f['mi'] - last['mi'] < NEW_PEN_GAP:
                continue
            if f['kind'] == 'bot' and f['price'] >= last['price']:
                continue
            if f['kind'] == 'top' and f['price'] <= last['price']:
                continue
            pts.append(f)
    return pts


def build_pens(pts: list) -> list:
    """笔: consecutive endpoint pairs → {'dir', 'p0', 'p1', 'hi', 'lo'}."""
    pens = []
    for a, b in zip(pts, pts[1:]):
        pens.append({'dir': 1 if b['price'] > a['price'] else -1,
                     'p0': a, 'p1': b,
                     'hi': max(a['price'], b['price']),
                     'lo': min(a['price'], b['price'])})
    return pens


def find_pivots(pens: list) -> list:
    """笔中枢: ≥PIVOT_PENS consecutive strokes sharing [ZD,ZG]; then extension.

    Formation: strokes i..i+2 with ZG=min(hi)>ZD=max(lo). Extension: each later
    stroke still overlapping [ZD,ZG] joins (ZG/ZD stay frozen from the first
    three; GG/DD track the walls). The first fully-outside stroke ends the
    pivot and becomes the 进入笔 of the next search. No 九段 level upgrade.
    """
    pivots, n = [], len(pens)
    i = 1                                   # pens[0] is the first 进入笔
    while i + PIVOT_PENS <= n:
        seg = pens[i:i + PIVOT_PENS]
        zg = min(p['hi'] for p in seg)
        zd = max(p['lo'] for p in seg)
        if zg > zd:
            j = i + PIVOT_PENS
            while j < n and pens[j]['lo'] <= zg and pens[j]['hi'] >= zd:
                j += 1
            body = pens[i:j]
            pivots.append({'zg': zg, 'zd': zd,
                           'gg': max(p['hi'] for p in body),
                           'dd': min(p['lo'] for p in body),
                           'i0': i, 'i1': j - 1})
            i = j + 1                       # pens[j] leaves → 进入笔 of the next
        else:
            i += 1
    return pivots


def trend_rel(prev: dict, cur: dict) -> str:
    """两中枢关系: 'up' (cur.ZD>prev.ZG) / 'down' (cur.ZG<prev.ZD) / 'range'."""
    if cur['zd'] > prev['zg']:
        return 'up'
    if cur['zg'] < prev['zd']:
        return 'down'
    return 'range'


# ════════════════════════════════════════════════════════════════════════════
# 2. 动力学: MACD 柱面积 + 三类买卖点
# ════════════════════════════════════════════════════════════════════════════

def macd_hist(close: pd.Series) -> pd.Series:
    """Standard MACD(12,26,9) histogram. Computed on FULL history by the caller
    then sliced, so the window's left edge carries warm values."""
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    return (dif - dea) * 2


def pen_area(pen: dict, hist: pd.Series) -> float:
    """Momentum of one stroke = Σ|hist| over its bars, start-exclusive so two
    adjacent strokes never double-count the shared endpoint bar."""
    return float(hist.iloc[pen['p0']['i'] + 1: pen['p1']['i'] + 1].abs().sum())


def _entry_pen(pens: list, pivots: list, k: int, want: int):
    """The trend leg connecting pivot k-1 to pivot k (divergence leg A).

    Two traps make the naive pens[i0-1] wrong: that slot is often the previous
    pivot's 回抽 (opposite direction), AND the true thrust is frequently the
    pivot's own FIRST forming pen (a monster leg whose top three-pen overlap
    becomes the box, e.g. NVDA 86→183 → box [168,183]). So: among the
    same-direction pens between the previous pivot's end and pens[i0]
    inclusive, take the one with the largest range."""
    lo = pivots[k - 1]['i1'] + 1 if k >= 1 else max(pivots[k]['i0'] - 2, 0)
    cands = [p for p in pens[lo: pivots[k]['i0'] + 1] if p['dir'] == want]
    if cands:
        return max(cands, key=lambda p: p['hi'] - p['lo'])
    for j in range(pivots[k]['i0'] - 1, max(pivots[k]['i0'] - 3, -1), -1):
        if j >= 0 and pens[j]['dir'] == want:
            return pens[j]
    return None


def find_signals(pens: list, pivots: list, hist: pd.Series) -> list:
    """三类买卖点 on committed structure.

    3B/3S — 中枢破坏: leaving stroke exits [ZD,ZG], the pullback stroke fails
            to re-enter. Marked at the pullback endpoint; 失效位 = ZG (3B) / ZD (3S).
    1S/1B — 趋势背驰: needs ≥2 pivots in trend arrangement (盘整背驰 is 现状-only).
            Compare 离开笔 C (fresh trend extreme past the last pivot) against
            进入笔 A of that pivot: new price extreme + area(C) < RATIO×area(A).
    2S/2B — the next same-direction stroke after a 1S/1B fails to make a new
            extreme. Only exists downstream of a marked first-class point.
    """
    signals, n = [], len(pens)

    for k, pv in enumerate(pivots):
        # 3B/3S. Because ZG/ZD freeze while the pivot extends, the true 离开笔
        # starts inside the box and gets absorbed as extension — so pens[j], the
        # first FULLY-outside stroke, is usually the 回抽 itself (opposite
        # direction, held outside). The thrust-then-pullback shape (both pens
        # fully outside) is the rarer second case.
        j = pv['i1'] + 1
        if j < n:
            first, nxt = pens[j], (pens[j + 1] if j + 1 < n else None)
            if first['dir'] < 0 and first['lo'] > pv['zg']:
                signals.append({'kind': '3B', 'pt': first['p1'], 'ref': pv['zg'],
                                'note': f"回抽低点 {first['lo']:.2f} > ZG {pv['zg']:.2f}"})
            elif first['dir'] > 0 and first['hi'] < pv['zd']:
                signals.append({'kind': '3S', 'pt': first['p1'], 'ref': pv['zd'],
                                'note': f"回抽高点 {first['hi']:.2f} < ZD {pv['zd']:.2f}"})
            elif first['dir'] > 0 and first['lo'] > pv['zg'] \
                    and nxt is not None and nxt['lo'] > pv['zg']:
                signals.append({'kind': '3B', 'pt': nxt['p1'], 'ref': pv['zg'],
                                'note': f"回抽低点 {nxt['lo']:.2f} > ZG {pv['zg']:.2f}"})
            elif first['dir'] < 0 and first['hi'] < pv['zd'] \
                    and nxt is not None and nxt['hi'] < pv['zd']:
                signals.append({'kind': '3S', 'pt': nxt['p1'], 'ref': pv['zd'],
                                'note': f"回抽高点 {nxt['hi']:.2f} < ZD {pv['zd']:.2f}"})

        if k == 0 or trend_rel(pivots[k - 1], pv) == 'range':
            continue
        rel = trend_rel(pivots[k - 1], pv)
        want = 1 if rel == 'up' else -1
        penA = _entry_pen(pens, pivots, k, want)
        if penA is None:
            continue
        for j2 in range(pv['i1'] + 1, n):
            c = pens[j2]
            if c['dir'] != want:
                continue
            new_extreme = (c['hi'] > max(pv['gg'], penA['hi'])) if want > 0 \
                else (c['lo'] < min(pv['dd'], penA['lo']))
            if not new_extreme:
                break                       # first leaving leg failed → no 1st-class point here
            aA, aC = pen_area(penA, hist), pen_area(c, hist)
            if aA > 0 and aC < DIVERGENCE_RATIO * aA:
                k1 = '1S' if want > 0 else '1B'
                ext = c['hi'] if want > 0 else c['lo']
                signals.append({'kind': k1, 'pt': c['p1'], 'ref': ext,
                                'note': f"归一面积比 {aC / aA:.2f} < {DIVERGENCE_RATIO}, "
                                        f"新极值 {ext:.2f}"})
                if j2 + 2 < n and pens[j2 + 2]['dir'] == want:
                    nxt = pens[j2 + 2]
                    failed = (nxt['hi'] < c['hi']) if want > 0 else (nxt['lo'] > c['lo'])
                    if failed:
                        k2 = '2S' if want > 0 else '2B'
                        signals.append({'kind': k2, 'pt': nxt['p1'], 'ref': ext,
                                        'note': f"次一同向笔未创新{'高' if want > 0 else '低'}"
                                                f" (vs {ext:.2f})"})
            break                           # only the first new-extreme leaving leg counts
    signals.sort(key=lambda s: s['pt']['i'])
    return signals


# ════════════════════════════════════════════════════════════════════════════
# 3. 现状 (the block that answers WHEN + WHERE, mirroring key_kline's 哲学)
# ════════════════════════════════════════════════════════════════════════════

def _conf_txt(df: pd.DataFrame, sg: dict) -> str:
    """' → 确认 <date> @<close>' suffix, or 尚未确认 when the endpoint is still
    provisional. The confirmation close is the first ACTIONABLE price."""
    ci = sg.get('conf_i')
    if ci is None:
        return ' → 尚未确认'
    return (f" → 确认 {df.index[ci].strftime('%m-%d')}"
            f" @{df['close'].iloc[ci]:.2f}")


def find_gaps(df: pd.DataFrame) -> list:
    """Window ilocs of >GAP_PCT overnight gaps (结构可信度 annotation only)."""
    ratio = (df['open'] / df['close'].shift(1) - 1).abs()
    return [i for i, v in enumerate(ratio.values) if pd.notna(v) and v > GAP_PCT]


def _compute_signals(df: pd.DataFrame, hist: pd.Series):
    """merge→分型→笔→中枢→买卖点 on one frame. Shared by analyze() and the
    confirmation replay."""
    merged = merge_bars(df)
    pts = find_points(find_fractals(merged))
    pens = build_pens(pts)
    pivots = find_pivots(pens)
    return merged, pts, pens, pivots, find_signals(pens, pivots, hist)


def confirm_signals(df: pd.DataFrame, hist: pd.Series, signals: list):
    """实际确认日: first bar at which each signal is detectable, found by
    replaying the pipeline on left-edge-fixed truncations of the window (the
    stroke endpoint needs later bars to commit its fractal — MU showed 1-6 td,
    at +12~21% above the printed 3B price in a melt-up). Sets conf_i (window
    iloc) or None = 尚未确认 (signal endpoint still provisional)."""
    for sg in signals:
        key = (sg['kind'], sg['pt']['i'])
        sg['conf_i'] = None
        stop = min(sg['pt']['i'] + CONF_MAX_BARS, len(df))
        for t in range(sg['pt']['i'] + 2, stop + 1):
            *_, sub = _compute_signals(df.iloc[:t], hist.iloc[:t])
            if any((s['kind'], s['pt']['i']) == key for s in sub):
                sg['conf_i'] = t - 1
                break


def analyze(df: pd.DataFrame, hist: pd.Series, disp_i0: int = 0) -> dict:
    """Full pipeline on the FULL frame → structure + signals + 现状.

    Confirmation replay only runs for signals inside the display window —
    decades-old signals are neither printed nor plotted, and each replay walks
    the whole frame."""
    merged, pts, pens, pivots, signals = _compute_signals(df, hist)
    confirm_signals(df, hist, [s for s in signals if s['pt']['i'] >= disp_i0])
    gaps = find_gaps(df)

    st = {'merged': merged, 'pts': pts, 'pens': pens, 'pivots': pivots,
          'signals': signals, 'gaps': gaps}
    close = float(df['close'].iloc[-1])
    st['close'] = close

    # ── 走势类型 ────────────────────────────────────────────────────────────
    if not pivots:
        st['trend'] = '结构不足'
        st['pivot_txt'] = '—'
    else:
        pv = pivots[-1]
        rel = trend_rel(pivots[-2], pv) if len(pivots) >= 2 else 'range'
        pos = ('中枢上方' if close > pv['zg'] else
               '中枢下方' if close < pv['zd'] else '中枢内')
        base = {'up': '上涨趋势', 'down': '下跌趋势', 'range': '盘整'}[rel]
        st['trend'] = f'{base}·{pos}'
        st['pivot_txt'] = f"[{pv['zd']:.2f}, {pv['zg']:.2f}]"
        st['rel'] = rel

    # ── 最近信号 + 失效位 (natural stop anchor) ─────────────────────────────
    if signals:
        sg = signals[-1]
        st['last_sig'] = (f"{sg['kind']} {df.index[sg['pt']['i']].strftime('%m-%d')}"
                          f" @{sg['pt']['price']:.2f}{_conf_txt(df, sg)}")
        watch = {'3B': f"日收盘跌回 ZG {sg['ref']:.2f} → 3B失效",
                 '3S': f"日收盘升回 ZD {sg['ref']:.2f} → 3S失效",
                 '1B': f"跌破 1B 低点 {sg['ref']:.2f} → 背了又背, 离场",
                 '1S': f"升破 1S 高点 {sg['ref']:.2f} → 背驰失败(强趋势), 卖点作废",
                 '2B': f"跌破前低 → 转折失败",
                 '2S': f"升破前高 {sg['ref']:.2f} → 2S失效"}
        st['watch'] = watch[sg['kind']]
        # A pivot formed AFTER the last signal supersedes its 失效位 — the old
        # anchor (e.g. a 3B's ZG far below after a melt-up) is no longer live.
        if pivots:
            pv_start_i = pens[pivots[-1]['i0']]['p0']['i']
            if sg['pt']['i'] <= pv_start_i:
                st['watch'] = (f"中枢 {st['pivot_txt']} 边界 "
                               f"(信号后已生成新中枢, 原失效位过时)")
    else:
        st['last_sig'] = '无'
        st['watch'] = (f"中枢 {st['pivot_txt']} 边界" if pivots else '结构不足, 无锚')

    # ── 背驰状态 (developing, GPS reading — not a marked signal) ───────────
    st['divergence'] = '无'
    if pivots and pts:
        pv = pivots[-1]
        last_pt = pts[-1]
        dev_dir = 1 if close > last_pt['price'] else -1
        penA = _entry_pen(pens, pivots, len(pivots) - 1, dev_dir)
        if penA is not None:
            dev = {'p0': last_pt, 'p1': {'i': len(df) - 1, 'price': close}}
            new_ext = (close > max(pv['gg'], penA['hi'])) if dev_dir > 0 \
                else (close < min(pv['dd'], penA['lo']))
            if new_ext:
                aA, aD = pen_area(penA, hist), pen_area(dev, hist)
                shrunk = aA > 0 and aD < DIVERGENCE_RATIO * aA
                trending = len(pivots) >= 2 and st.get('rel') in ('up', 'down')
                if shrunk and trending:
                    st['divergence'] = (f"{'1S' if dev_dir > 0 else '1B'}背驰候选 "
                                        f"(当前笔归一面积比 {aD / aA:.2f} < 0.8, 笔未完成)")
                elif shrunk:
                    st['divergence'] = (f"盘整背驰提示: 预期回试中枢 {st['pivot_txt']}"
                                        f" (仅1个中枢, 不构成一类买卖点)")

    # ── 缺口降级提示 ────────────────────────────────────────────────────────
    recent = [g for g in gaps if g >= len(df) - GAP_LOOKBACK]
    st['gap_warn'] = (f"近{GAP_LOOKBACK}日有 {len(recent)} 个 >{GAP_PCT:.0%} 跳空 "
                      f"— 分型/笔结构可信度降级" if recent else '')
    return st


def print_status(ticker: str, df: pd.DataFrame, st: dict, disp_i0: int = 0):
    print(f"\n══ 现状 {ticker}  {_now().strftime('%Y-%m-%d')}"
          f"  close {st['close']:.2f} ══")
    print(f"  走势   : {st['trend']}   最近中枢 {st['pivot_txt']}")
    print(f"  最近信号: {st['last_sig']}")
    print(f"  失效·关注位: {st['watch']}")
    print(f"  背驰   : {st['divergence']}")
    if st['gap_warn']:
        print(f"  ⚠ {st['gap_warn']}")
    sigs = [s for s in st['signals'] if s['pt']['i'] >= disp_i0]
    if sigs:
        print(f"  历史信号 ({len(sigs)}, 显示窗口内):")
        for sg in sigs:
            print(f"    {df.index[sg['pt']['i']].strftime('%Y-%m-%d')}  "
                  f"{sg['kind']}  @{sg['pt']['price']:.2f}"
                  f"{_conf_txt(df, sg)}  {sg['note']}")


# ════════════════════════════════════════════════════════════════════════════
# 4. Chart
# ════════════════════════════════════════════════════════════════════════════

def plot_chart(df: pd.DataFrame, st: dict, ticker: str, out_png: str,
               disp_i0: int = 0):
    """Candles + volume, then 笔折线 / 中枢方框 / 买卖点标注 / 缺口菱形.

    Structure carries FULL-frame ilocs; the chart shows df.iloc[disp_i0:], so
    every x is shifted by -disp_i0. Elements straddling the left edge get
    negative x and are clipped by the axes — pens/pivots born before the
    window still enter the picture at their true slope/height.
    """
    disp = df.iloc[disp_i0:]
    start, end = disp.index[0].strftime('%Y-%m-%d'), disp.index[-1].strftime('%Y-%m-%d')
    mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', inherit=True)
    style = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc,
                               gridstyle=':', y_on_right=False)
    fig, axes = mpf.plot(disp[['open', 'high', 'low', 'close', 'volume']],
                         type='candle', volume=True, style=style,
                         figsize=(16, 9), returnfig=True,
                         ylabel='Price', ylabel_lower='Volume',
                         warn_too_much_data=10 ** 6)
    ax = axes[0]
    fig.suptitle(f'{ticker} 缠论·日线笔中枢  {start}~{end}',
                 fontproperties=CJK_FONT, fontsize=14, fontweight='bold')
    # Freeze the candle-set limits NOW: structure elements reaching left of the
    # display window carry negative x, which must clip, not stretch the axes.
    xlim, ylim = ax.get_xlim(), ax.get_ylim()

    span = disp['high'].max() - disp['low'].min()
    off = span * 0.03 if span > 0 else 1.0

    # 笔 (committed) + provisional last leg to the current close (dashed)
    pts = st['pts']
    if pts:
        ax.plot([p['i'] - disp_i0 for p in pts], [p['price'] for p in pts],
                color='#444444', lw=1.5, zorder=4)
        ax.plot([pts[-1]['i'] - disp_i0, len(df) - 1 - disp_i0],
                [pts[-1]['price'], st['close']],
                color='#444444', lw=1.2, ls='--', zorder=4)

    # 中枢 rectangles
    for pv in st['pivots']:
        pens = st['pens']
        x0 = pens[pv['i0']]['p0']['i'] - disp_i0
        x1 = pens[pv['i1']]['p1']['i'] - disp_i0
        if x1 < 0:
            continue
        ax.add_patch(Rectangle((x0 - 0.5, pv['zd']), x1 - x0 + 1, pv['zg'] - pv['zd'],
                               facecolor='#3366cc', alpha=0.10,
                               edgecolor='#3366cc', lw=1.0, zorder=3))

    # 买卖点 — the LABEL sits on the 实际确认日 bar (the first day the signal
    # was knowable / actionable), not on the structural extreme, so the chart
    # never suggests the pullback low was a fillable price. A dotted connector
    # ties the label back to the structural point (hollow circle). Unconfirmed
    # (still-provisional endpoint) → '?' at the structural bar itself.
    for sg in st['signals']:
        if sg['pt']['i'] < disp_i0:
            continue
        i, price = sg['pt']['i'] - disp_i0, sg['pt']['price']
        is_buy = sg['kind'].endswith('B')
        color = '#1a7f37' if is_buy else '#c62828'
        ci = sg.get('conf_i')
        ax.scatter(i, price, s=42, facecolors='none', edgecolors=color,
                   lw=1.3, zorder=6)
        if ci is None:
            ytxt = price - off * 2.2 if is_buy else price + off * 2.2
            ax.annotate(f"{sg['kind']}?", xy=(i, price), xytext=(i, ytxt),
                        ha='center', va='top' if is_buy else 'bottom',
                        fontsize=10, fontweight='bold', color=color,
                        arrowprops=dict(arrowstyle='->', color=color, lw=1.2),
                        zorder=6, fontproperties=CJK_FONT)
            continue
        cx, cpx = ci - disp_i0, float(df['close'].iloc[ci])
        ax.plot([i, cx], [price, cpx], color=color, lw=0.9, ls=':', zorder=5)
        ytxt = cpx - off * 2.2 if is_buy else cpx + off * 2.2
        label = f"{sg['kind']}✓{df.index[ci].strftime('%m-%d')}"
        ax.annotate(label, xy=(cx, cpx), xytext=(cx, ytxt),
                    ha='center', va='top' if is_buy else 'bottom',
                    fontsize=10, fontweight='bold', color=color,
                    arrowprops=dict(arrowstyle='->', color=color, lw=1.3),
                    zorder=6, fontproperties=CJK_FONT)

    # 缺口 (>GAP_PCT) — amber diamonds above the bar
    for g in st['gaps']:
        if g >= disp_i0:
            ax.scatter(g - disp_i0, df['high'].iloc[g] + off * 0.6, marker='D',
                       s=18, color='#f59e0b', zorder=5)

    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.legend(handles=[
        Line2D([], [], color='#444444', lw=1.5, label='笔'),
        Rectangle((0, 0), 1, 1, facecolor='#3366cc', alpha=0.25, label='中枢 [ZD,ZG]'),
        Line2D([], [], color='#f59e0b', marker='D', ls='', label=f'>{GAP_PCT:.0%} 跳空'),
        Line2D([], [], color='#1a7f37', marker=r'$B$', ls='', label='买点 (标签=确认日)'),
        Line2D([], [], color='#c62828', marker=r'$S$', ls='', label='卖点 (○=结构点)'),
    ], loc='upper left', fontsize=9, framealpha=0.6, prop=CJK_FONT)

    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    fig.savefig(out_png, dpi=110, bbox_inches='tight')
    plt.close(fig)
    logging.info(f'Chart saved: {out_png}')


# ════════════════════════════════════════════════════════════════════════════
# 5. Modes
# ════════════════════════════════════════════════════════════════════════════

def _window(ticker: str, period: str):
    """Full-history fetch → (df, hist, disp_i0).

    df/hist are the FULL frame; disp_i0 is the iloc where the display window
    starts. The structure is always computed on full history — a rolling left
    edge makes the division path-dependent (validation: NVDA's 1S 2024-11-08
    lived in every weekly snapshot for 6 months, then vanished as the 2y window
    slid past its supporting pivots; MU's mid-2025 3B flipped between two
    versions). With the left edge pinned to the first available bar, a live
    day only appends on the right and committed history can never re-divide.
    --period only chooses how much to draw/print.

    hist is normalized by close (|hist|/price): raw MACD area scales ~linearly
    with price level, so on a 2-3x trend the later leg ALWAYS out-areas the
    earlier one and 背驰 can structurally never fire (MU A@221-455 vs C@652-1089
    gave raw ratio 1.08, normalized 1.0 — honest; NVDA's real top gave 0.05).
    Normalization keeps the 0.8 ratio meaningful across price regimes."""
    full = _fetch_daily(ticker)
    if full is None or full.empty:
        return pd.DataFrame(), pd.Series(dtype=float), 0
    histn = macd_hist(full['close']) / full['close']
    cutoff = _now() - pd.Timedelta(days=PERIOD_DAYS.get(period, 730))
    disp_i0 = int(full.index.searchsorted(cutoff))
    return full, histn, disp_i0


def run_ticker(ticker: str, period: str, output: str | None):
    df, hist, disp_i0 = _window(ticker, period)
    if df.empty or len(df) - disp_i0 < 30:
        logging.error(f'{ticker}: no/insufficient bars — abort')
        return
    st = analyze(df, hist, disp_i0)
    print_status(ticker, df, st, disp_i0)
    if output is None:
        base = os.path.join(RESULT_DIR, 'us_chanlun') if os.path.isdir(RESULT_DIR) else '.'
        output = os.path.join(base, f"us_chanlun_{ticker}_{_now().strftime('%Y%m%d')}.png")
    plot_chart(df, st, ticker, output, disp_i0)


def _hold_tickers() -> list:
    """持仓代理 = select.yml US_SWING_STOPS keys (hand-maintained live positions;
    no OpenD dependency, works offline). Holdings-from-Futu stays tech_swing's job."""
    try:
        import yaml
        with open(WATCHLIST_FILE) as fh:
            cfg = yaml.safe_load(fh) or {}
        return [str(t).upper() for t in (cfg.get('US_SWING_STOPS') or {})]
    except Exception as e:
        logging.error(f'select.yml load failed: {e}')
        return []


def run_hold(period: str):
    tickers = _hold_tickers()
    if not tickers:
        logging.error('US_SWING_STOPS is empty — nothing to examine')
        return
    rows = []
    for t in tickers:
        try:
            df, hist, disp_i0 = _window(t, period)
            if df.empty or len(df) - disp_i0 < 30:
                rows.append([t, '—', '数据不足', '—', '—', '—', '—'])
                continue
            st = analyze(df, hist, disp_i0)
            rows.append([t, f"{st['close']:.2f}", st['trend'], st['pivot_txt'],
                         st['last_sig'], st['divergence'],
                         st['watch'] + (' ⚠gap' if st['gap_warn'] else '')])
        except Exception as e:
            logging.warning(f'{t}: {e}')
            rows.append([t, '—', f'error: {e}', '—', '—', '—', '—'])
    print(f"\n══ 持仓缠论体检 (卖点侧)  {_now().strftime('%Y-%m-%d')} ══")
    print(tab_mod.tabulate(
        rows, headers=['TICKER', '收盘', '走势', '中枢[ZD,ZG]', '最近信号',
                       '背驰状态', '失效·关注位'],
        tablefmt='simple', stralign='left'))
    print('\n  单票详情/出图: python t_us_chanlun.py --ticker SYM')


def main():
    global _ASOF
    p = OptionParser(usage='usage: %prog [--ticker SYM | --hold] [options]')
    p.add_option('--ticker', dest='ticker', help='single-ticker chart + 现状')
    p.add_option('--hold', action='store_true', dest='hold', default=False,
                 help='US_SWING_STOPS 持仓体检表 (sell-side focused, no plots)')
    p.add_option('--period', dest='period', default='2y',
                 help='display window: 6mo/1y/2y/3y (default 2y)')
    p.add_option('--output', dest='output', default=None,
                 help='PNG path (default result/us_chanlun/us_chanlun_<t>_<date>.png)')
    p.add_option('--asof', dest='asof', default=None,
                 help='YYYY-MM-DD point-in-time replay (truncates bars, anchors 今天)')
    opts, _args = p.parse_args()

    if opts.asof:
        _ASOF = pd.Timestamp(opts.asof).normalize()
        _sw._ASOF = _ASOF
        logging.info(f'AS-OF mode: anchoring to {_ASOF.date()}')

    if opts.hold:
        run_hold(opts.period)
    elif opts.ticker:
        run_ticker(opts.ticker.upper(), opts.period, opts.output)
    else:
        p.error('need --ticker SYM or --hold')


if __name__ == '__main__':
    main()
