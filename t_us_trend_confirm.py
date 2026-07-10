# coding: utf-8
"""
US Trend Confirm — in_trend 三条件状态翻转扫描 (回调结束确认 / 跌出强趋势态)

动机 (MU 2026-04-08 案例, docs/mu_1m_decline_study.md 讨论延伸):
  三条件 in_trend = 站上200日线 & 半年≥+20% & 距252日高≤15% (定义复用
  t_us_pullback_shock.annotate, 全库单一事实源)。此前它只作急跌买点的前置
  过滤; 而 "in_trend 从 False 翻 True" 本身 = 回调修复确认日 (市场用价格
  收复跌幅大半证明卖压被消化, MU 4/8 型), 系统里无人输出 — 本脚本补上。

两类事件 (近 --lookback 个交易日内翻转、且今天仍保持翻转后状态):
  CONFIRM (False→True): 回调结束确认。确认成本内生 — 确认日几乎必是大阳
      (正是那根K线把距高拉进15%门槛), 观感"追高"是定义使然, 不是缺陷。
  LOST (True→False): 跌出强趋势态 (三条件之一失守, 表中注明是哪条)。
      持仓名带 ★ — 对持仓这是降档提示, 不是卖出指令 (退出政策归 scanner,
      ADR-0002; 20周线脊梁没破之前只是质地降级)。

状态读数 (GPS), 不给止损/目标。诚实说明: 15% 门槛是拟定参数, 确认日随参数
  ±3 个交易日滑动, 精确日别太当真。

Usage:
  python t_us_trend_confirm.py --scan --universe both          # cron 用
  python t_us_trend_confirm.py --scan --universe ndx --lookback 10
  python t_us_trend_confirm.py --ticker MU                     # 单票历史翻转
  python t_us_trend_confirm.py --scan --asof 2026-04-08        # point-in-time
"""

import os
import sys
import logging
import datetime

import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _fetch_daily
from t_us_pullback_shock import annotate, _load_universe
from t_us_intraday_internals import load_holdings

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR = '/home/ryan/DATA/result/us_trend_confirm'
MIN_BARS   = 252 + 126 + 2      # hi252 + r6m 都要能算


def _cond_lost(row) -> str:
    """LOST 行注明失守的是哪条 (可多条)。"""
    fails = []
    if not row['close'] > row['ma_long']:
        fails.append('破200日线')
    if not row['r6m'] >= 0.20:
        fails.append('半年<+20%')
    if not row['dfh'] >= -0.15:
        fails.append('距高>15%')
    return '+'.join(fails) or '?'


def flips_for(ticker: str, lookback: int, asof=None):
    """返回该票近 lookback 交易日内的 (kind, 翻转日, age, 注记, 现价, dfh%, r6m%)。"""
    df = _fetch_daily(ticker)
    if asof is not None and not df.empty:
        df = df[df.index <= asof]
    if len(df) < MIN_BARS:
        return None
    d = annotate(df)
    it = d['in_trend']
    out = []
    last = d.iloc[-1]
    recent = range(max(1, len(d) - lookback), len(d))
    for i in recent:
        prev, cur = bool(it.iloc[i - 1]), bool(it.iloc[i])
        if prev == cur:
            continue
        age = len(d) - 1 - i
        # 只报"今天仍保持翻转后状态"的事件, 否则是当周噪声来回
        if cur != bool(it.iloc[-1]):
            continue
        kind = 'CONFIRM' if cur else 'LOST'
        note = '回调修复确认' if cur else _cond_lost(last)
        out.append({
            'kind': kind, 'flip_dt': d.index[i].date(), 'age': age, 'note': note,
            'close': round(float(last['close']), 2),
            'dfh%': round(float(last['dfh']) * 100, 1),
            'r6m%': round(float(last['r6m']) * 100, 1),
            'flip_ret%': round(float(d['r1d'].iloc[i]) * 100, 1),  # 翻转日当日涨跌
        })
    return out


def run_scan(universe: str, lookback: int, asof=None):
    tickers = _load_universe(universe)
    holds = set(load_holdings())
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    logging.info(f'scan universe={universe} ({len(tickers)} 只), lookback={lookback}{asof_txt}')

    rows = []
    for t in tickers + sorted(holds - set(tickers)):   # 持仓名池外也要看
        try:
            for ev in (flips_for(t, lookback, asof) or []):
                ev['ticker'] = ('★' if t in holds else '') + t
                rows.append(ev)
        except Exception as e:
            logging.warning(f'{t}: {e}')

    date_tag = (asof.date() if asof is not None else datetime.date.today()).strftime('%Y%m%d')
    print(f'\n== in_trend 三条件翻转扫描 (universe={universe}, 近{lookback}个交易日){asof_txt} ==')
    print('三条件 = >200日线 & 半年≥+20% & 距252日高≤15% (t_us_pullback_shock.annotate)')
    print('CONFIRM=回调结束确认(MU 4/8型, 确认日多为大阳=内生确认成本) · LOST=跌出强趋势态(注明失守条件)')
    print('★=持仓。状态读数非指令; LOST≠卖出(脊梁归20周线), CONFIRM≠追买(去关键K线找择时)。\n')

    if rows:
        cols = ['ticker', 'kind', 'flip_dt', 'age', 'flip_ret%', 'close', 'dfh%', 'r6m%', 'note']
        out = pd.DataFrame(rows)[cols].sort_values(['kind', 'age', 'ticker'])
        print(tab_mod.tabulate(out, headers='keys', tablefmt='github', showindex=False))
        os.makedirs(RESULT_DIR, exist_ok=True)
        csv_f = os.path.join(RESULT_DIR, f'us_trend_confirm_{universe}_{date_tag}.csv')
        out.to_csv(csv_f, index=False)
        logging.info(f'saved {csv_f} ({len(out)} 行)')
    else:
        print('_近期无 in_trend 翻转。_')
    return 0


def run_ticker(ticker: str, asof=None):
    """单票: 全历史翻转清单 + 当前状态。"""
    df = _fetch_daily(ticker)
    if asof is not None and not df.empty:
        df = df[df.index <= asof]
    if len(df) < MIN_BARS:
        logging.error(f'{ticker}: 历史不足 {MIN_BARS} bars')
        return 1
    d = annotate(df)
    it = d['in_trend']
    flips = d[(it != it.shift(1)) & it.notna()].index[1:]
    last = d.iloc[-1]
    print(f'\n== {ticker} in_trend 状态 ==')
    print(f'当前: {"IN (强趋势态)" if bool(it.iloc[-1]) else "OUT — " + _cond_lost(last)}'
          f'  (close {last["close"]:.2f}, 距252日高 {last["dfh"]*100:.1f}%, 半年 {last["r6m"]*100:+.1f}%)')
    print(f'\n历史翻转 (近 2 年):')
    for ts in flips[-20:]:
        if (d.index[-1] - ts).days > 730:
            continue
        cur = bool(it.loc[ts])
        ret = d.loc[ts, 'r1d'] * 100
        print(f"  {ts.date()}  {'→ IN  (CONFIRM)' if cur else '→ OUT (LOST)':<18} 当日 {ret:+.1f}%  close {d.loc[ts,'close']:.2f}")
    return 0


def main():
    parser = OptionParser(usage=__doc__)
    parser.add_option('--scan', action='store_true', dest='scan', default=False)
    parser.add_option('--ticker', dest='ticker', default=None)
    parser.add_option('--universe', dest='universe', default='both', help='sp500|ndx|both')
    parser.add_option('--lookback', dest='lookback', type='int', default=5,
                      help='报告近 N 个交易日内的翻转 (default 5)')
    parser.add_option('--asof', dest='asof', default=None, help='YYYY-MM-DD point-in-time')
    (opt, _) = parser.parse_args()

    asof = pd.Timestamp(opt.asof) if opt.asof else None
    if opt.ticker:
        return run_ticker(opt.ticker.upper(), asof)
    if opt.scan:
        return run_scan(opt.universe, opt.lookback, asof)
    parser.print_help()
    return 1


if __name__ == '__main__':
    sys.exit(main())
