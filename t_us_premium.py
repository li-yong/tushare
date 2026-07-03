# coding: utf-8
"""
US Premium Scanner — 贵气/股性 (优质标的三层框架 第2层)

方法论 docs/key_kline_methodology.md §0:贵气 = 市场心甘情愿为其故事付溢价。
量化为三个子项,加权成 premium_score(0–100):
  1. above_ma50_pct  长期在均线上方天数占比   (纯K线)
  2. rs / rs_new_high RS线(vs QQQ)创新高/领涨  (纯K线)
  3. val_elastic      估值弹性(估值历史分位低=有上探空间) (Futu 估值分位, 见 us_fundamentals)

这是"三层互不依赖"中的独立一层:自己取数、自己出分、自己写 CSV。
组合(与兑现/技术连乘找共振)由 t_us_resonance.py 负责。

Usage:
  python t_us_premium.py                 # 全 watchlist
  python t_us_premium.py --ticker NVDA   # 单票
  python t_us_premium.py --no-futu       # 跳过 Futu(估值分位走 yfinance 兜底/缺失)
"""

import os
import sys
import logging
import datetime
import traceback

import numpy as np
import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _history, _sma, UNIVERSE
import us_fundamentals

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# ── Parameters (集中, 可调) ─────────────────────────────────────────────────────
PREMIUM_LOOKBACK = 150     # "在均线上方天数占比" 的回看窗口
MA_FOR_PREMIUM   = 50      # 用 MA50 作长期均线
RS_BENCH         = 'QQQ'   # RS 相对基准(Nasdaq)
RS_WINDOW        = 252     # RS 分位/新高的回看窗口(~1y)
RS_NEWHIGH_TOL   = 0.01    # RS 在 1y 高点 1% 内算"创新高"
W_ABOVE, W_RS, W_VAL = 0.40, 0.40, 0.20   # 子项权重
PREMIUM_MIN      = 60      # 共振阈值(供 t_us_resonance 读取)
GRADE_A, GRADE_B = 75, 60  # premium_score 分档

RESULT_DIR = '/home/ryan/DATA/result'


def _clip01(x):
    return max(0.0, min(1.0, float(x)))


def _above_ma_pct(daily: pd.DataFrame) -> float | None:
    """近 PREMIUM_LOOKBACK 日, 收盘 > MA50 的天数占比 (0..1)."""
    if daily is None or len(daily) < MA_FOR_PREMIUM + 5:
        return None
    ma = _sma(daily['close'], MA_FOR_PREMIUM)
    valid = (daily['close'] > ma).dropna()
    if valid.empty:
        return None
    tail = valid.tail(PREMIUM_LOOKBACK)
    return round(float(tail.mean()), 4)


def _rs_metrics(daily: pd.DataFrame, bench: pd.DataFrame) -> tuple:
    """RS线 = close / bench_close. 返回 (rs_pctile 0..1, rs_new_high bool)."""
    if daily is None or bench is None or daily.empty or bench.empty:
        return None, None
    df = pd.DataFrame({'c': daily['close'], 'b': bench['close']}).dropna()
    if len(df) < 30:
        return None, None
    rs = (df['c'] / df['b']).tail(RS_WINDOW)
    if len(rs) < 30:
        return None, None
    last = float(rs.iloc[-1])
    pctile = float((rs <= last).mean())               # 最新 RS 在窗口内的分位
    new_high = bool(last >= float(rs.max()) * (1 - RS_NEWHIGH_TOL))
    return round(pctile, 4), new_high


def _val_subscore(valuation: dict | None) -> tuple:
    """估值弹性子项 (0..1) + 透传 pe/pe_pctile/fwd<trail. 无分位则子项=None."""
    if not valuation:
        return None, None, None, None
    pe = valuation.get('pe')
    pct = valuation.get('pe_pctile')        # 0..1 分数, 越低=越便宜=越有弹性
    fwd, trail = valuation.get('fwd_pe'), valuation.get('trail_pe')
    fwd_lt_trail = (fwd is not None and trail is not None and fwd < trail)
    if pct is None:
        return None, pe, None, fwd_lt_trail
    s = _clip01(1 - pct)                     # 低分位 → 高分
    if fwd_lt_trail:                         # 盈利长进估值 → 额外加分
        s = _clip01(s + 0.10)
    return round(s, 4), pe, pct, fwd_lt_trail


def scan_premium(ticker: str, bench: pd.DataFrame, use_futu: bool = True) -> dict:
    r = {'ticker': ticker, 'close': None, 'above_ma50_pct': None,
         'rs_pctile': None, 'rs_new_high': None, 'pe': None, 'pe_pctile': None,
         'fwd_lt_trail': None, 'premium_score': None, 'grade': None,
         'data_flags': '', 'error': None}
    try:
        daily = _history(ticker, period='2y', interval='1d')
        if daily is None or daily.empty:
            r['error'] = 'no data'
            return r
        r['close'] = round(float(daily['close'].iloc[-1]), 2)

        s_above = _above_ma_pct(daily)
        rs_pctile, rs_new_high = _rs_metrics(daily, bench)

        funda = us_fundamentals.fetch_all(ticker, use_futu=use_futu)
        valuation = funda.get('valuation')
        for f in funda.get('flags', []):
            if f.startswith('noval'):
                r['data_flags'] = (r['data_flags'] + ',' + f).strip(',')
        s_val, pe, pe_pctile, fwd_lt_trail = _val_subscore(valuation)

        r['above_ma50_pct'] = s_above
        r['rs_pctile']      = rs_pctile
        r['rs_new_high']    = rs_new_high
        r['pe']             = round(pe, 2) if pe is not None else None
        r['pe_pctile']      = round(pe_pctile, 4) if pe_pctile is not None else None
        r['fwd_lt_trail']   = fwd_lt_trail

        # 加权(只对可用子项归一)
        parts = [(s_above, W_ABOVE), (rs_pctile, W_RS), (s_val, W_VAL)]
        avail = [(s, w) for s, w in parts if s is not None]
        if not avail:
            r['error'] = 'no scoreable components'
            return r
        wsum = sum(w for _, w in avail)
        score = sum(s * w for s, w in avail) / wsum * 100
        r['premium_score'] = round(score, 1)
        r['grade'] = 'A' if score >= GRADE_A else ('B' if score >= GRADE_B else 'C')
        if s_val is None:
            r['data_flags'] = (r['data_flags'] + ',noval_score').strip(',')

    except Exception as e:
        r['error'] = str(e)
        logging.debug(traceback.format_exc())
    return r


def _fmt(v, d=2):
    return '—' if v is None else (f'{v:.{d}f}' if isinstance(v, float) else str(v))


def print_and_write(results, out_file=None):
    rows = []
    for r in sorted(results, key=lambda x: (x['premium_score'] is None, -(x['premium_score'] or 0))):
        if r['error']:
            rows.append([r['ticker'], _fmt(r['close']), '—', '—', '—', '—', '—', f"ERR:{r['error']}"])
            continue
        rows.append([
            r['ticker'], _fmt(r['close']),
            _fmt(r['above_ma50_pct'], 2) if r['above_ma50_pct'] is None else f"{r['above_ma50_pct']*100:.0f}%",
            _fmt(r['rs_pctile'], 2) if r['rs_pctile'] is None else f"{r['rs_pctile']*100:.0f}%",
            'Y' if r['rs_new_high'] else ('—' if r['rs_new_high'] is None else 'n'),
            _fmt(r['pe_pctile'], 2) if r['pe_pctile'] is None else f"{r['pe_pctile']*100:.1f}%",
            f"{r['premium_score']}{r['grade']}" if r['premium_score'] is not None else '—',
            r['data_flags'] or '',
        ])
    table = tab_mod.tabulate(
        rows,
        headers=['Ticker', 'Close', '>MA50%', 'RS%ile', 'RShi', 'PE%ile', 'Score', 'Flags'],
        tablefmt='simple')
    print('\n[ PREMIUM 贵气/L2 ]  score = 0.4·站上MA50天数 + 0.4·RS分位 + 0.2·估值弹性(1-PE分位)')
    print(table)
    print(f'  阈值 PREMIUM_MIN={PREMIUM_MIN} (≥ 视为贵气点亮, 供共振);  Score 后缀 A≥{GRADE_A}/B≥{GRADE_B}/C')

    # CSV
    if out_file:
        df = pd.DataFrame([{
            'ticker': r['ticker'], 'close': r['close'],
            'above_ma50_pct': r['above_ma50_pct'], 'rs_pctile': r['rs_pctile'],
            'rs_new_high': r['rs_new_high'], 'pe': r['pe'], 'pe_pctile': r['pe_pctile'],
            'fwd_lt_trail': r['fwd_lt_trail'], 'premium_score': r['premium_score'],
            'grade': r['grade'], 'data_flags': r['data_flags'] or ('ERR:' + r['error'] if r['error'] else ''),
        } for r in results])
        df.to_csv(out_file, index=False)
        logging.info(f'Premium CSV → {out_file}')


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--ticker', dest='ticker', default=None, help='单票扫描')
    parser.add_option('--no-futu', dest='no_futu', action='store_true', default=False,
                      help='跳过 Futu, 估值分位走 yfinance 兜底或缺失')
    parser.add_option('--output', dest='output', default=None, help='CSV 输出路径')
    opts, _ = parser.parse_args()

    logging.info('US Premium (贵气/L2) scanner starting')
    bench = _history(RS_BENCH, period='2y', interval='1d')
    if bench is None or bench.empty:
        logging.warning(f'RS 基准 {RS_BENCH} 取数失败 — RS 子项将缺失')

    universe = [opts.ticker.upper()] if opts.ticker else UNIVERSE
    results = []
    for t in universe:
        logging.info(f'Premium scan {t} …')
        results.append(scan_premium(t, bench, use_futu=not opts.no_futu))

    out_file = opts.output
    if out_file is None and os.path.isdir(RESULT_DIR):
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        out_dir = os.path.join(RESULT_DIR, 'us_premium')
        os.makedirs(out_dir, exist_ok=True)
        out_file = f'{out_dir}/us_premium_{date_str}.csv'

    print_and_write(results, out_file)
    us_fundamentals.close_futu()


if __name__ == '__main__':
    main()
