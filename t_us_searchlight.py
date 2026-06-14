# coding: utf-8
"""
US Searchlight — 池外侦察 (方法论第1层的"探照灯", 不改 watchlist)

目的:回答"只盯 Mag7+半导体够不够"。把候选域放宽到 NDX-100, 对【当前 watchlist
池子外】的每只票跑 premium(贵气) × delivery(兑现) 打分, 按"双高"排序, 并标出
所属板块——让"盲区"具体化:看看池外有没有更强的时代赢家、在哪些板块。

纯侦察:只打印 + 写 result/us_searchlight_<date>.csv, 绝不改 select.yml。
是否纳入 = 第1层人工决定。

复用:NDX-100 成分 + 板块 来自 ndx_predictor;打分来自 t_us_premium / t_us_delivery。

Usage:
  python t_us_searchlight.py                # NDX-100 池外, 全打分
  python t_us_searchlight.py --top 30       # 只看前 30
  python t_us_searchlight.py --limit 40     # 只评分前 40 只(按市值, 省时)
"""

import os
import sys
import logging
import datetime

import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _history
from t_us_premium import scan_premium, PREMIUM_MIN
from t_us_delivery import scan_delivery, DELIVERY_MIN
import us_fundamentals

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

WATCHLIST_FILE = '/home/ryan/tushare_ryan/select.yml'
RESULT_DIR = '/home/ryan/DATA/result'


def _current_pool():
    import yaml
    cfg = yaml.safe_load(open(WATCHLIST_FILE)) or {}

    def _t(key):
        out = []
        for row in (cfg.get(key) or []):
            out.append(next(iter(row)) if isinstance(row, dict) else row)
        return [str(x).upper() for x in out]
    return set(_t('US_SWING_MAG7') + _t('US_SWING_SEMIS') + _t('US_SWING_AI_CHAIN'))


def _ndx_universe():
    """NDX-100 成分 + {ticker: sector} 标签, 复用 ndx_predictor。"""
    import ndx_predictor as ndx
    tickers = [str(t).upper() for t in ndx.fetch_current_constituents()]
    sectors = {}
    try:
        sdf = ndx.fetch_nasdaq_sectors()
        # 列名未知 → 自适应找 symbol/sector 两列
        if sdf is not None and len(sdf):
            cols = {c.lower(): c for c in sdf.columns}
            sym = cols.get('symbol') or cols.get('ticker') or list(sdf.columns)[0]
            sec = cols.get('sector') or cols.get('industry')
            if sec:
                for _, r in sdf.iterrows():
                    sectors[str(r[sym]).upper()] = str(r[sec])
    except Exception as e:
        logging.warning(f'sector labels unavailable ({e})')
    return tickers, sectors


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--top', dest='top', type='int', default=25, help='展示前 N(默认25)')
    parser.add_option('--limit', dest='limit', type='int', default=None,
                      help='只评分前 N 只候选(省时)')
    parser.add_option('--no-futu', dest='no_futu', action='store_true', default=False)
    opts, _ = parser.parse_args()

    logging.info('US Searchlight starting — 拉 NDX-100 …')
    pool = _current_pool()
    universe, sectors = _ndx_universe()
    candidates = [t for t in universe if t not in pool]
    if opts.limit:
        candidates = candidates[:opts.limit]
    logging.info(f'池内 {len(pool)} 只 · NDX-100 {len(universe)} 只 · 池外候选 {len(candidates)} 只待评分')

    bench = _history('QQQ', period='2y', interval='1d')

    rows = []
    for i, t in enumerate(candidates, 1):
        logging.info(f'[{i}/{len(candidates)}] scoring {t} …')
        try:
            p = scan_premium(t, bench, use_futu=not opts.no_futu).get('premium_score')
            d = scan_delivery(t, use_futu=not opts.no_futu).get('delivery_score')
        except Exception as e:
            logging.warning(f'{t} scoring failed: {e}')
            p = d = None
        both = (p is not None and p >= PREMIUM_MIN and d is not None and d >= DELIVERY_MIN)
        combined = (p or 0) + (d or 0)
        rows.append({'ticker': t, 'sector': sectors.get(t, ''),
                     'premium': p, 'delivery': d, 'both_lit': both,
                     'combined': round(combined, 1)})

    rows.sort(key=lambda r: (not r['both_lit'], -r['combined']))

    # 输出
    top = rows[:opts.top]
    tbl = [[r['ticker'], (r['sector'] or '')[:22],
            f"{r['premium']:.0f}" if r['premium'] is not None else '—',
            f"{r['delivery']:.0f}" if r['delivery'] is not None else '—',
            '★双高' if r['both_lit'] else '', r['combined']] for r in top]
    print(f'\n[ SEARCHLIGHT — NDX-100 池外 贵气×兑现 前{len(top)} ]'
          f'  (双高 = 贵气≥{PREMIUM_MIN} 且 兑现≥{DELIVERY_MIN})')
    print(tab_mod.tabulate(tbl, headers=['Ticker', 'Sector', 'Prem', 'Deliv', '', 'Σ'],
                           tablefmt='simple'))
    duals = [r for r in rows if r['both_lit']]
    print(f"\n  ★ 池外【双高】(贵气×兑现都达标)共 {len(duals)} 只: "
          f"{', '.join(r['ticker'] for r in duals) or '(无)'}")
    # 按板块汇总双高, 直接回答"其他板块"
    if duals:
        bysec = {}
        for r in duals:
            bysec.setdefault(r['sector'] or '未知', []).append(r['ticker'])
        print('  按板块:')
        for sec, ts in sorted(bysec.items(), key=lambda x: -len(x[1])):
            print(f'    {sec}: {", ".join(ts)}')

    if os.path.isdir(RESULT_DIR):
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        out = f'{RESULT_DIR}/us_searchlight_{date_str}.csv'
        pd.DataFrame(rows).to_csv(out, index=False)
        logging.info(f'Searchlight CSV → {out}')
    print('\n  (纯侦察, 未改 watchlist)')
    us_fundamentals.close_futu()


if __name__ == '__main__':
    main()
