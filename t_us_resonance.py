# coding: utf-8
"""
US Resonance — 三层共振汇总器

把三个独立层的当日产出按 ticker join,用方法论的【连乘语义】找共振:
    爆发前夜 = 贵气(L2) × 兑现(L3) × 技术觉醒  (docs/key_kline_methodology.md §0)

  - 第1层 产业主线 = watchlist 成员资格,天生满足(门票),不计分。
  - 测量独立(三个 CSV 各自产出),组合合取(全部点亮才共振)。

读入(均为当日, /home/ryan/DATA/result/):
    us_premium_<date>.csv   (t_us_premium.py)
    us_delivery_<date>.csv  (t_us_delivery.py)
    us_tech_signal_<date>.csv (t_us_tech_swing.py)
缺/陈旧某份 → 降级到现有轴的合取,并在报告里打印降级说明。

Usage:
  python t_us_resonance.py                 # 用今天的三份 CSV
  python t_us_resonance.py --date 20260613 # 指定日期
"""

import os
import sys
import logging
import datetime

import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_premium import PREMIUM_MIN
from t_us_delivery import DELIVERY_MIN

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)

RESULT_DIR = '/home/ryan/DATA/result'
TECH_REQUIRE_HIGH = False   # True = 仅 confidence=HIGH 的技术信号算点亮


def _load(date_str, name):
    """Load result/<name>_<date>.csv → (DataFrame|None, status_note)."""
    path = os.path.join(RESULT_DIR, f'{name}_{date_str}.csv')
    if not os.path.exists(path):
        return None, f'{name}: 缺失 ({os.path.basename(path)})'
    try:
        df = pd.read_csv(path)
        age = (datetime.date.today()
               - datetime.date.fromtimestamp(os.path.getmtime(path))).days
        note = f'{name}: ok ({len(df)} 行' + (f', {age}d 旧)' if age else ')')
        return df, note
    except Exception as e:
        return None, f'{name}: 读取失败 ({e})'


def _tech_lit(row):
    if row is None:
        return False
    st = str(row.get('signal_type') or '').strip()
    if not st or st.lower() == 'nan':
        return False
    if TECH_REQUIRE_HIGH:
        return str(row.get('confidence') or '').strip().upper() == 'HIGH'
    return True


def build(date_str):
    prem, n_p = _load(date_str, 'us_premium')
    deliv, n_d = _load(date_str, 'us_delivery')
    tech, n_t = _load(date_str, 'us_tech_signal')
    notes = [n_p, n_d, n_t]

    have = {'premium': prem is not None, 'delivery': deliv is not None, 'tech': tech is not None}
    axes = [a for a, ok in have.items() if ok]          # 当前可用的轴

    # 全 ticker 集合
    tickers = set()
    for df in (prem, deliv, tech):
        if df is not None and 'ticker' in df.columns:
            tickers |= set(df['ticker'].astype(str))

    def _row(df, t):
        if df is None:
            return None
        m = df[df['ticker'].astype(str) == t]
        return m.iloc[0].to_dict() if len(m) else None

    rows = []
    for t in sorted(tickers):
        pr, de, te = _row(prem, t), _row(deliv, t), _row(tech, t)
        p_score = pr.get('premium_score') if pr else None
        d_score = de.get('delivery_score') if de else None

        p_lit = have['premium'] and p_score is not None and float(p_score) >= PREMIUM_MIN
        d_lit = have['delivery'] and d_score is not None and float(d_score) >= DELIVERY_MIN
        t_lit = have['tech'] and _tech_lit(te)

        lit_map = {'技术': t_lit, '贵气': p_lit, '兑现': d_lit}
        # 只在可用轴上计点亮
        lit_axes = [name for name, (a, on) in
                    zip(['技术', '贵气', '兑现'],
                        [('tech', t_lit), ('premium', p_lit), ('delivery', d_lit)])
                    if have[a] and on]
        n_lit = len(lit_axes)
        n_axes = len(axes)
        full = (n_axes > 0 and n_lit == n_axes and n_lit >= 1
                and all(have[a] for a in ('tech', 'premium', 'delivery')))
        # 当三轴齐全且全亮 = 真·共振;否则按点亮轴数给星
        stars = '★' * n_lit if n_lit else '—'

        tech_cell = '—'
        if te and str(te.get('signal_type') or '').strip() not in ('', 'nan'):
            tech_cell = f"{te['signal_type']}/{te.get('confidence', '')}"

        rows.append({
            'ticker':   t,
            'tech':     tech_cell,
            'premium':  f"{p_score:.0f}" if p_score is not None else '—',
            'delivery': f"{d_score:.0f}" if d_score is not None else '—',
            'lit':      '·'.join(lit_axes) if lit_axes else '—',
            'reson':    '共振 ' + stars if full else stars,
            '_full':    full,
            '_nlit':    n_lit,
            '_sort':    (float(p_score or 0) + float(d_score or 0)),
            'note':     '',
        })

    rows.sort(key=lambda r: (not r['_full'], -r['_nlit'], -r['_sort']))
    return rows, notes, axes, have


def render(date_str, rows, notes, axes, have):
    out = []

    def p(*a):
        line = ' '.join(str(x) for x in a)
        out.append(line); print(line)

    p()
    p('=' * 78)
    p(f'  US RESONANCE 三层共振  —  {date_str}   (连乘: 贵气 × 兑现 × 技术觉醒)')
    p('=' * 78)
    p('  第1层 产业主线 = watchlist 门票, 天生已过, 不计分。')
    p('  数据源状态:')
    for n in notes:
        p(f'    - {n}')
    missing = [a for a in ('tech', 'premium', 'delivery') if not have[a]]
    if missing:
        p(f'  ⚠ 降级: 缺 {", ".join(missing)} → 共振退化为现有 {len(axes)} 轴的合取。')

    table = tab_mod.tabulate(
        [[r['ticker'], r['tech'], r['premium'], r['delivery'], r['lit'], r['reson']] for r in rows],
        headers=['Ticker', 'Tech(type/conf)', 'Prem', 'Deliv', '点亮轴', '共振'],
        tablefmt='simple')
    p()
    p(table)
    full_hits = [r['ticker'] for r in rows if r['_full']]
    p()
    if full_hits:
        p(f'  ★ 三层全亮(真共振): {", ".join(full_hits)}  ← 爆发前夜候选, 仍需关键K线择时+止损')
    else:
        p('  本期无三层全亮共振。')
    p(f'  阈值: 贵气≥{PREMIUM_MIN} · 兑现≥{DELIVERY_MIN} · 技术=有信号'
      + ('(限HIGH)' if TECH_REQUIRE_HIGH else ''))
    p()
    return '\n'.join(out)


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--date', dest='date', default=None, help='YYYYMMDD, 默认今天')
    parser.add_option('--output', dest='output', default=None, help='报告输出路径')
    opts, _ = parser.parse_args()

    date_str = opts.date or datetime.datetime.now().strftime('%Y%m%d')
    rows, notes, axes, have = build(date_str)
    text = render(date_str, rows, notes, axes, have)

    out_file = opts.output
    if out_file is None and os.path.isdir(RESULT_DIR):
        out_file = f'{RESULT_DIR}/us_resonance_{date_str}.txt'
    if out_file:
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(text + '\n')
        logging.info(f'Resonance report → {out_file}')


if __name__ == '__main__':
    main()
