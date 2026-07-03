# coding: utf-8
"""
US Delivery Scanner — 业绩兑现/扳机 (优质标的三层框架 第3层)

方法论 docs/key_kline_methodology.md §0:兑现 = "梦"撞上"证据"的那一刻。
量化为三个子项,加权成 delivery_score(0–100):
  1. rev_accel   营收 YoY 水平 + 是否逐季加速      (Futu 财务, 见 us_fundamentals)
  2. eps_beat    EPS 连续超预期 streak + 最新一期方向(PEAD 扳机) (yfinance Surprise%)
  3. analyst_up  分析师上调(up30>down30 且趋势上行) + 目标价上行空间 (yfinance 修正 + Futu/yf 评级)

"三层互不依赖"中的独立一层:自取数、自出分、自写 CSV。
组合(连乘找共振)由 t_us_resonance.py 负责。

Usage:
  python t_us_delivery.py                 # 全 watchlist
  python t_us_delivery.py --ticker NVDA   # 单票
  python t_us_delivery.py --no-futu       # 跳过 Futu(营收走 yfinance 兜底)
"""

import os
import sys
import logging
import datetime
import traceback

import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _history, UNIVERSE
import us_fundamentals

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# ── Parameters (集中, 可调) ─────────────────────────────────────────────────────
DELIVERY_REVQ   = 4        # 营收加速看最近几个季度
REV_YOY_FULL    = 40.0     # YoY 达此值算满分水平(%)
EPS_STREAK_FULL = 4        # 连续超预期达此根数算满分
UPSIDE_FULL     = 0.30     # 目标价上行空间达此值算满分(30%)
W_REV, W_EPS, W_ANALYST = 0.40, 0.30, 0.30
DELIVERY_MIN    = 60       # 共振阈值(供 t_us_resonance 读取)
GRADE_A, GRADE_B = 75, 60

RESULT_DIR = '/home/ryan/DATA/result'


def _clip01(x):
    return max(0.0, min(1.0, float(x)))


def _rev_subscore(revenue: list) -> tuple:
    """返回 (s_rev 0..1, latest_yoy, accel_q). 营收缺失→(None,None,None)."""
    if not revenue:
        return None, None, None
    yoys = [r.get('yoy') for r in revenue if r.get('yoy') is not None]
    if not yoys:
        return None, None, None
    latest_yoy = yoys[0]
    win = yoys[:DELIVERY_REVQ]
    # 逐季加速根数:新一季 yoy 高于其后一季(更旧)
    accel_q = sum(1 for i in range(len(win) - 1) if win[i] > win[i + 1])
    s_level = _clip01(latest_yoy / REV_YOY_FULL)
    s_accel = accel_q / max(1, DELIVERY_REVQ - 1)
    s_rev = _clip01(0.6 * s_level + 0.4 * s_accel)
    return round(s_rev, 4), round(latest_yoy, 1), accel_q


def _eps_subscore(surprises: list) -> tuple:
    """返回 (s_eps 0..1, latest_surprise, streak). 缺失→(None,None,None)."""
    if not surprises:
        return None, None, None
    latest = surprises[0]
    streak = 0
    for s in surprises:        # 新→旧, 连续为正
        if s is not None and s > 0:
            streak += 1
        else:
            break
    s_eps = _clip01(streak / EPS_STREAK_FULL)
    if latest is not None and latest < 0:   # 最新一期 miss → 扳机失效, 折半
        s_eps *= 0.5
    return round(s_eps, 4), round(latest, 2) if latest is not None else None, streak


def _analyst_subscore(revisions: dict | None, consensus: dict | None,
                      close: float | None) -> tuple:
    """返回 (s_analyst 0..1, up30, down30, upside_pct). 全缺→(None,...)."""
    up = down = None
    s_dir = None
    if revisions:
        up, down = revisions.get('up30'), revisions.get('down30')
        trend_up = revisions.get('trend_up')
        if up is not None and down is not None:
            if up > down and trend_up:
                s_dir = 1.0
            elif up > down or trend_up:
                s_dir = 0.5
            else:
                s_dir = 0.0
        elif trend_up is not None:
            s_dir = 1.0 if trend_up else 0.0

    upside_pct = None
    s_up = None
    if consensus and close and close > 0 and consensus.get('target_mean'):
        upside_pct = consensus['target_mean'] / close - 1
        s_up = _clip01(upside_pct / UPSIDE_FULL)

    parts = [(s_dir, 0.6), (s_up, 0.4)]
    avail = [(s, w) for s, w in parts if s is not None]
    if not avail:
        return None, up, down, (round(upside_pct * 100, 1) if upside_pct is not None else None)
    s_analyst = sum(s * w for s, w in avail) / sum(w for _, w in avail)
    return round(s_analyst, 4), up, down, (round(upside_pct * 100, 1) if upside_pct is not None else None)


def scan_delivery(ticker: str, use_futu: bool = True) -> dict:
    r = {'ticker': ticker, 'close': None, 'rev_yoy_latest': None, 'rev_accel_q': None,
         'eps_surprise_latest': None, 'eps_beat_streak': None, 'est_up30': None,
         'est_down30': None, 'analyst_upside_pct': None, 'delivery_score': None,
         'grade': None, 'data_flags': '', 'error': None}
    try:
        funda = us_fundamentals.fetch_all(ticker, use_futu=use_futu)
        flags = list(funda.get('flags', []))

        daily = _history(ticker, period='3mo', interval='1d')
        if daily is not None and not daily.empty:
            r['close'] = round(float(daily['close'].iloc[-1]), 2)

        s_rev, latest_yoy, accel_q = _rev_subscore(funda.get('revenue'))
        s_eps, latest_surp, streak = _eps_subscore(funda.get('surprises'))
        s_analyst, up30, down30, upside = _analyst_subscore(
            funda.get('revisions'), funda.get('consensus'), r['close'])

        r['rev_yoy_latest']      = latest_yoy
        r['rev_accel_q']         = accel_q
        r['eps_surprise_latest'] = latest_surp
        r['eps_beat_streak']     = streak
        r['est_up30']            = up30
        r['est_down30']          = down30
        r['analyst_upside_pct']  = upside

        parts = [(s_rev, W_REV), (s_eps, W_EPS), (s_analyst, W_ANALYST)]
        avail = [(s, w) for s, w in parts if s is not None]
        if not avail:
            r['error'] = 'no scoreable components'
            r['data_flags'] = ','.join(flags)
            return r
        score = sum(s * w for s, w in avail) / sum(w for _, w in avail) * 100
        r['delivery_score'] = round(score, 1)
        r['grade'] = 'A' if score >= GRADE_A else ('B' if score >= GRADE_B else 'C')
        if s_rev is None:     flags.append('norev_score')
        if s_eps is None:     flags.append('noeps_score')
        if s_analyst is None: flags.append('noanalyst_score')
        r['data_flags'] = ','.join(dict.fromkeys(flags))

    except Exception as e:
        r['error'] = str(e)
        logging.debug(traceback.format_exc())
    return r


def _fmt(v, d=1):
    return '—' if v is None else (f'{v:.{d}f}' if isinstance(v, float) else str(v))


def print_and_write(results, out_file=None):
    rows = []
    for r in sorted(results, key=lambda x: (x['delivery_score'] is None, -(x['delivery_score'] or 0))):
        if r['error'] and r['delivery_score'] is None:
            rows.append([r['ticker'], _fmt(r['close'], 2), '—', '—', '—', '—', '—', f"ERR:{r['error']}"])
            continue
        upside = f"{r['analyst_upside_pct']:+.0f}%" if r['analyst_upside_pct'] is not None else '—'
        rev = f"{r['rev_yoy_latest']:.0f}%×{r['rev_accel_q']}" if r['rev_yoy_latest'] is not None else '—'
        eps = f"{r['eps_beat_streak']}@{r['eps_surprise_latest']:+.1f}" if r['eps_beat_streak'] is not None else '—'
        rows.append([
            r['ticker'], _fmt(r['close'], 2), rev, eps,
            f"{r['est_up30']}/{r['est_down30']}" if r['est_up30'] is not None else '—',
            upside,
            f"{r['delivery_score']}{r['grade']}" if r['delivery_score'] is not None else '—',
            r['data_flags'] or '',
        ])
    table = tab_mod.tabulate(
        rows,
        headers=['Ticker', 'Close', 'RevYoY×加速', 'EPSstreak@最新', 'Est↑/↓', 'Upside', 'Score', 'Flags'],
        tablefmt='simple')
    print('\n[ DELIVERY 兑现/L3 ]  score = 0.4·营收加速 + 0.3·EPS连续超预期 + 0.3·分析师上调')
    print(table)
    print(f'  阈值 DELIVERY_MIN={DELIVERY_MIN} (≥ 视为兑现点亮, 供共振);  Score 后缀 A≥{GRADE_A}/B≥{GRADE_B}/C')

    if out_file:
        df = pd.DataFrame([{k: r[k] for k in (
            'ticker', 'close', 'rev_yoy_latest', 'rev_accel_q', 'eps_surprise_latest',
            'eps_beat_streak', 'est_up30', 'est_down30', 'analyst_upside_pct',
            'delivery_score', 'grade')} | {'data_flags': r['data_flags'] or (
                'ERR:' + r['error'] if r['error'] else '')} for r in results])
        df.to_csv(out_file, index=False)
        logging.info(f'Delivery CSV → {out_file}')


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--ticker', dest='ticker', default=None, help='单票扫描')
    parser.add_option('--no-futu', dest='no_futu', action='store_true', default=False,
                      help='跳过 Futu, 营收走 yfinance 兜底')
    parser.add_option('--output', dest='output', default=None, help='CSV 输出路径')
    opts, _ = parser.parse_args()

    logging.info('US Delivery (兑现/L3) scanner starting')
    universe = [opts.ticker.upper()] if opts.ticker else UNIVERSE
    results = []
    for t in universe:
        logging.info(f'Delivery scan {t} …')
        results.append(scan_delivery(t, use_futu=not opts.no_futu))

    out_file = opts.output
    if out_file is None and os.path.isdir(RESULT_DIR):
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        out_dir = os.path.join(RESULT_DIR, 'us_delivery')
        os.makedirs(out_dir, exist_ok=True)
        out_file = f'{out_dir}/us_delivery_{date_str}.csv'

    print_and_write(results, out_file)
    us_fundamentals.close_futu()


if __name__ == '__main__':
    main()
