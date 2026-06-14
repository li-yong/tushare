# coding: utf-8
"""
US Watchlist Suggester — 用实时数据刷新 watchlist 候选 + 清理 select.yml 死字段

方法论 §0:第1层"产业主线"只能人工圈定,脚本【不应】悄悄改 watchlist。
本工具的定位:用 Futu 板块拉【当前】半导体候选池,逐票验证(揪出退市/改名的死票),
再用已有的 premium(贵气)+ delivery(兑现) 评分给候选排序,产出"建议加/删"清单。

  - 默认:只打印建议 + 写 result/us_watchlist_suggest_<date>.csv(不改 select.yml)。
  - --apply:备份 select.yml 后写回(刷新 US_SWING_SEMIS + 删除 CN 时代死字段),
            US_SWING 段的注释原样保留。

候选池 = Futu 半导体板块成分(us_chip);MAG7 视为固定的大盘龙头集合(仅校验存活)。
Futu 不可达 → 无候选池, 仅对现有列表做存活校验, 不做扩充(优雅降级)。

Usage:
  python t_us_watchlist_suggest.py            # 只建议, 不改文件
  python t_us_watchlist_suggest.py --apply    # 备份后写回 select.yml
"""

import os
import sys
import shutil
import logging
import datetime

import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _fetch_daily, _history
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
SEMIS_PLATE_FALLBACK = 'US.LIST20077'   # 半导体 (实测 2026-06)
# 自动纳入 SEMIS 的质量门槛(候选既要兑现达标, 也要贵气不至于太弱)
ADD_DELIVERY_MIN = DELIVERY_MIN          # 兑现 ≥ 60
ADD_PREMIUM_MIN  = 50                    # 贵气 ≥ 50 (略低于共振阈, 给"潜力"留口)
# 删尾部 CN 时代死字段(扫描器/live系统不读;持仓走 Futu 实时)
DEAD_KEYS = ['US_HOLD', 'HK_HOLD', 'CN_HOLD', 'US', 'HK', 'CN',
             'CN_INDEX', 'US_INDEX', 'FUTU_CN_ETF']


def _load_lists():
    import yaml
    cfg = yaml.safe_load(open(WATCHLIST_FILE)) or {}

    def _t(key):
        out = []
        for row in (cfg.get(key) or []):
            out.append(next(iter(row)) if isinstance(row, dict) else row)
        return [str(x).upper() for x in out]
    return _t('US_SWING_MAG7'), _t('US_SWING_SEMIS')


def futu_semis_pool() -> list:
    """当前半导体板块成分 (ticker 去前缀). Futu 不可达 → []。"""
    ctx = us_fundamentals._futu()
    if ctx is None:
        return []
    try:
        from futu import RET_OK
        # 用 curated 的"半导体龙头"板块(us_chip = US.LIST20077, 实测 15 只大盘),
        # 不用宽口径的行业板块——后者含大量小票, 会把优质标的列表冲淡。
        plate_code = SEMIS_PLATE_FALLBACK
        ret, df = ctx.get_plate_stock(plate_code)
        if ret != RET_OK or df is None or 'code' not in df.columns:
            return []
        return [str(c).split('.')[-1].upper() for c in df['code'].tolist()]
    except Exception as e:
        logging.warning(f'futu semis plate failed ({e})')
        return []


def _alive(ticker: str) -> bool:
    try:
        df = _fetch_daily(ticker)
        return df is not None and not df.empty
    except Exception:
        return False


def build(use_futu=True):
    mag7, semis = _load_lists()
    pool = futu_semis_pool() if use_futu else []
    if not pool:
        logging.warning('无 Futu 候选池(离线或板块取数失败)— 仅做存活校验, 不扩充')

    bench = _history('QQQ', period='2y', interval='1d')

    # 候选 = 现有SEMIS ∪ 板块成分, 去掉已在 MAG7 的(避免重复, 如 NVDA)
    candidates = [t for t in dict.fromkeys(semis + pool) if t not in mag7]

    rows = []
    for t in candidates:
        alive = _alive(t)
        prem = scan_premium(t, bench, use_futu=use_futu) if alive else None
        deliv = scan_delivery(t, use_futu=use_futu) if alive else None
        p = prem['premium_score'] if prem else None
        d = deliv['delivery_score'] if deliv else None
        in_list = t in semis

        if not alive:
            action = 'DROP(死票)'
        elif in_list:
            action = 'KEEP'
        elif (d is not None and d >= ADD_DELIVERY_MIN
              and p is not None and p >= ADD_PREMIUM_MIN):
            action = 'ADD'
        else:
            action = '—(未达标)'
        rows.append({'ticker': t, 'in_list': in_list, 'alive': alive,
                     'premium': p, 'delivery': d, 'action': action})

    # MAG7 存活校验(固定集合, 只揪死票)
    mag7_rows = []
    for t in mag7:
        alive = _alive(t)
        mag7_rows.append({'ticker': t, 'alive': alive,
                          'action': 'KEEP' if alive else 'DROP(死票)'})

    # 新 SEMIS = 现有里仍存活的 + 新增达标的
    new_semis = [r['ticker'] for r in rows
                 if r['action'] in ('KEEP', 'ADD')]
    new_mag7 = [r['ticker'] for r in mag7_rows if r['alive']]
    return mag7, semis, rows, mag7_rows, new_mag7, new_semis


def render(semis, rows, mag7_rows, new_semis):
    rows_sorted = sorted(rows, key=lambda r: (
        {'ADD': 0, 'KEEP': 1, '—(未达标)': 2, 'DROP(死票)': 3}.get(r['action'], 4),
        -((r['delivery'] or 0) + (r['premium'] or 0))))
    tbl = [[r['ticker'], 'Y' if r['in_list'] else '', 'Y' if r['alive'] else 'DEAD',
            f"{r['premium']:.0f}" if r['premium'] is not None else '—',
            f"{r['delivery']:.0f}" if r['delivery'] is not None else '—',
            r['action']] for r in rows_sorted]
    print('\n[ SEMIS 候选刷新 ]  门槛: 加入需 兑现≥{} 且 贵气≥{}'.format(ADD_DELIVERY_MIN, ADD_PREMIUM_MIN))
    print(tab_mod.tabulate(tbl, headers=['Ticker', '在列', '存活', 'Prem', 'Deliv', '建议'],
                           tablefmt='simple'))
    adds = [r['ticker'] for r in rows if r['action'] == 'ADD']
    drops = [r['ticker'] for r in rows if r['action'].startswith('DROP')]
    dead_mag7 = [r['ticker'] for r in mag7_rows if not r['alive']]
    print(f"\n  ➕ 建议加入 SEMIS: {', '.join(adds) or '(无)'}")
    print(f"  ➖ 建议删除(死票): {', '.join(drops + dead_mag7) or '(无)'}")
    print(f"  新 SEMIS({len(new_semis)}): {', '.join(new_semis)}")
    return rows_sorted


def apply_to_yaml(new_mag7, new_semis):
    """备份后写回:刷新 MAG7/SEMIS 列表 + 删除 CN 死字段, 保留 US_SWING 注释。"""
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = f'{WATCHLIST_FILE}.bak.{ts}'
    shutil.copy2(WATCHLIST_FILE, backup)
    logging.info(f'已备份 → {backup}')

    lines = open(WATCHLIST_FILE, encoding='utf-8').read().splitlines()

    # 1) 截掉 CN 死段: 从第一行匹配任一 DEAD_KEY 的顶层键开始, 全部丢弃
    cut = len(lines)
    for i, ln in enumerate(lines):
        key = ln.split(':')[0].strip()
        if key in DEAD_KEYS:
            cut = i
            break
    head = lines[:cut]
    # 去掉尾部空行
    while head and head[-1].strip() == '':
        head.pop()

    # 2) 替换 US_SWING_MAG7 / US_SWING_SEMIS 的条目块(保留键行及其注释)
    def _replace_block(buf, key, items):
        out, i = [], 0
        while i < len(buf):
            ln = buf[i]
            if ln.split(':')[0].strip() == key:
                out.append(ln)                       # 键行(含行内注释)原样
                i += 1
                while i < len(buf) and buf[i].lstrip().startswith('- '):
                    i += 1                           # 跳过旧条目
                for it in items:
                    out.append(f'    - {it}')
            else:
                out.append(ln)
                i += 1
        return out

    head = _replace_block(head, 'US_SWING_MAG7', new_mag7)
    head = _replace_block(head, 'US_SWING_SEMIS', new_semis)

    open(WATCHLIST_FILE, 'w', encoding='utf-8').write('\n'.join(head) + '\n')
    logging.info(f'已写回 select.yml(删 {len(DEAD_KEYS)} 个CN死字段, '
                 f'MAG7={len(new_mag7)} SEMIS={len(new_semis)})')


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--apply', dest='apply', action='store_true', default=False,
                      help='备份后写回 select.yml(默认只建议)')
    parser.add_option('--no-futu', dest='no_futu', action='store_true', default=False)
    parser.add_option('--dry-run-to', dest='dry_to', default=None,
                      help='把写回结果输出到指定文件而非 select.yml(自检用)')
    opts, _ = parser.parse_args()

    logging.info('US watchlist suggester starting')
    mag7, semis, rows, mag7_rows, new_mag7, new_semis = build(use_futu=not opts.no_futu)
    render(semis, rows, mag7_rows, new_semis)

    if os.path.isdir(RESULT_DIR):
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        out = f'{RESULT_DIR}/us_watchlist_suggest_{date_str}.csv'
        pd.DataFrame(rows).to_csv(out, index=False)
        logging.info(f'建议 CSV → {out}')

    if opts.dry_to:
        global WATCHLIST_FILE
        shutil.copy2(WATCHLIST_FILE, opts.dry_to)
        _real = WATCHLIST_FILE
        WATCHLIST_FILE = opts.dry_to
        apply_to_yaml(new_mag7, new_semis)
        WATCHLIST_FILE = _real
        logging.info(f'[dry-run] 写回结果在 {opts.dry_to}(未动真文件)')
    elif opts.apply:
        apply_to_yaml(new_mag7, new_semis)
    else:
        print('\n  (仅建议, 未改 select.yml — 加 --apply 写回)')

    us_fundamentals.close_futu()


if __name__ == '__main__':
    main()
