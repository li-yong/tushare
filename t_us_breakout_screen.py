# coding: utf-8
"""
US Breakout Screener — 全市场"横盘后突破"筛选(广域, 只排除垃圾股)

与 t_us_tech_swing 的窄 watchlist 不同, 本脚本放宽搜索域到【整个美股】, 只用
流动性/规模门槛排除垃圾股, 然后套用严格的【横盘突破】判定选股。

横盘突破 = 方法论 §3"长期横盘后放量第一阳冲破区间顶"。四道闸(都做成常量, 可调):
  1. 窄基底:  前 N 周区间高低差 (range_high-range_low)/range_low ≤ BASE_MAX_WIDTH
  2. 平基底:  基底期间净漂移 |close_last/close_first-1| ≤ BASE_MAX_DRIFT(排除"趋势新高")
  3. 首次突破: 本周收盘 > 区间顶, 且上周收盘 ≤ 区间顶(避免连标已突破多周的票)
  4. 放量:    本周量 ≥ VOL_MULT × 5周均量
(这正是 t_us_tech_swing 现有 _breakout_signal 缺的三条——见该函数诊断。)

漏斗:
  ① Futu get_stock_filter:US 全市场, 价≥MIN_PRICE · 市值≥MIN_MKTCAP · 量≥MIN_VOLUME
     → 排除垃圾股, 得候选宇宙(按市值降序, 上限 MAX_UNIVERSE)。
  ② 逐只 yfinance 周线(复用 t_us_tech_swing 缓存, 守 ADR-0001)→ 四道闸 → 命中。

输出: 命中表(按量比降序)+ result/us_breakout_screen_<date>.csv。纯选股, 不改 watchlist。

Usage:
  python t_us_breakout_screen.py                  # 默认域 (市值≥$2B, 上限400)
  python t_us_breakout_screen.py --limit 800      # 放更宽
  python t_us_breakout_screen.py --min-mktcap 1e9 # 更激进(纳入更小盘)
"""

import os
import sys
import logging
import datetime
import traceback

import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _history, _key_support_below, MIN_RR
import us_fundamentals

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR = '/home/ryan/DATA/result'

# ── 垃圾股排除门槛(第一道漏斗) ─────────────────────────────────────────────────
MIN_PRICE    = 10.0       # 排除低价/仙股
MIN_MKTCAP   = 2e9        # 排除微盘($2B)
MIN_VOLUME   = 500_000    # 排除低流动性(日均量)
MAX_UNIVERSE = 400        # 候选上限(按市值降序取前 N), 控制 yfinance 取数耗时

# ── 横盘突破判定(第二道漏斗) ───────────────────────────────────────────────────
CONSOL_W       = 10       # 基底周数
BASE_MAX_WIDTH = 0.25     # 基底高低差上限(越小越"横")
BASE_MAX_DRIFT = 0.15     # 基底净漂移上限(排除趋势新高)
VOL_MULT       = 1.5      # 放量倍数(对 5 周均量)


def futu_us_universe(min_price, min_mktcap, min_volume, max_n) -> list:
    """Futu 全美选股, 排除垃圾股, 返回 [(ticker, mktcap, price)] 按市值降序。"""
    ctx = us_fundamentals._futu()
    if ctx is None:
        logging.error('Futu OpenD 不可达 — 无法取广域宇宙(本脚本依赖 Futu 选股)')
        return []
    try:
        from futu import (SimpleFilter, AccumulateFilter, StockField,
                          SortDir, Market, RET_OK)
        fl = []
        f = SimpleFilter(); f.stock_field = StockField.CUR_PRICE
        f.filter_min = min_price; f.is_no_filter = False; fl.append(f)
        f = SimpleFilter(); f.stock_field = StockField.MARKET_VAL
        f.filter_min = min_mktcap; f.is_no_filter = False
        f.sort = SortDir.DESCEND; fl.append(f)               # 按市值降序
        f = AccumulateFilter(); f.stock_field = StockField.VOLUME
        f.filter_min = min_volume; f.is_no_filter = False; f.days = 1; fl.append(f)

        out, begin = [], 0
        while len(out) < max_n:
            ret, data = ctx.get_stock_filter(Market.US, fl, begin=begin, num=200)
            if ret != RET_OK:
                logging.warning(f'get_stock_filter 失败 @begin={begin}: {data}')
                break
            last_page, all_count, stock_list = data
            if not stock_list:
                break
            for it in stock_list:
                code = getattr(it, 'stock_code', None) or getattr(it, 'code', '')
                tk = str(code).split('.')[-1].upper()
                if tk:
                    out.append((tk, getattr(it, 'market_val', None),
                                getattr(it, 'cur_price', None)))
            begin += len(stock_list)
            if last_page or len(stock_list) < 200:
                break
        logging.info(f'广域宇宙: {len(out)} 只(全美 价≥{min_price} 市值≥{min_mktcap:.0e} 量≥{min_volume})')
        return out[:max_n]
    except Exception as e:
        logging.error(f'futu universe failed: {e}')
        logging.debug(traceback.format_exc())
        return []


def consolidation_breakout(weekly: pd.DataFrame, daily: pd.DataFrame,
                           vol_mult: float = VOL_MULT) -> dict | None:
    """横盘突破判定。返回 setup dict(含 tier), 否则 None。

    tier='HIT'   : 四道闸全过(窄基底+平+首破+放量)——真·放量横盘突破。
    tier='WATCH' : 窄基底+平+首破都过, 唯独差放量——"蓄势待发", 下周放量即确认。
    """
    if weekly is None or len(weekly) < CONSOL_W + 3:
        return None
    cur_close = float(weekly['close'].iloc[-1])
    prev_close = float(weekly['close'].iloc[-2])
    cur_vol = float(weekly['volume'].iloc[-1])

    base = weekly.iloc[-CONSOL_W - 1:-1]               # 前 CONSOL_W 周(不含本周)
    range_high = float(base['close'].max())
    range_low = float(base['low'].min())
    if range_low <= 0:
        return None
    base_first = float(base['close'].iloc[0])
    base_last = float(base['close'].iloc[-1])
    avg_vol_5w = float(weekly['volume'].iloc[-6:-1].mean())

    base_width = (range_high - range_low) / range_low
    base_drift = abs(base_last / base_first - 1)
    vol_ratio = cur_vol / avg_vol_5w if avg_vol_5w > 0 else 0.0

    # ── 形态四闸(放量单列, 决定 tier) ──
    if cur_close <= range_high:               return None   # 没突破
    if prev_close > range_high:               return None   # 非首破(已突破多周)
    if base_width > BASE_MAX_WIDTH:           return None   # 基底不够窄
    if base_drift > BASE_MAX_DRIFT:           return None   # 基底在趋势上行
    tier = 'HIT' if vol_ratio >= vol_mult else 'WATCH'      # 唯一区别 = 放量

    entry = round(cur_close, 2)
    key_stop = _key_support_below(daily, entry)
    stop = key_stop if (key_stop and entry - key_stop > 0) else round(range_low, 2)
    risk = entry - stop
    if risk <= 0:
        return None
    target = round(entry + (range_high - range_low), 2)
    rr = round((target - entry) / risk, 2)
    return {
        'tier': tier,
        'entry': entry, 'stop': round(stop, 2), 'target': target, 'rr': rr,
        'rr_ok': rr >= MIN_RR, 'base_width': round(base_width * 100, 1),
        'base_drift': round(base_drift * 100, 1), 'vol_ratio': round(vol_ratio, 2),
        'range_high': round(range_high, 2), 'range_low': round(range_low, 2),
    }


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--limit', dest='limit', type='int', default=MAX_UNIVERSE)
    parser.add_option('--min-price', dest='min_price', type='float', default=MIN_PRICE)
    parser.add_option('--min-mktcap', dest='min_mktcap', type='float', default=MIN_MKTCAP)
    parser.add_option('--min-volume', dest='min_volume', type='float', default=MIN_VOLUME)
    parser.add_option('--vol-mult', dest='vol_mult', type='float', default=VOL_MULT,
                      help=f'放量倍数门槛, 决定 HIT vs WATCH(默认 {VOL_MULT})')
    parser.add_option('--watch-top', dest='watch_top', type='int', default=20,
                      help='WATCH(待放量)档展示数(默认20)')
    parser.add_option('--output', dest='output', default=None)
    opts, _ = parser.parse_args()

    logging.info('US Breakout Screener — 拉广域宇宙 …')
    universe = futu_us_universe(opts.min_price, opts.min_mktcap, opts.min_volume, opts.limit)
    if not universe:
        logging.error('宇宙为空 — 退出')
        return

    hits = []
    for i, (tk, mv, px) in enumerate(universe, 1):
        if i % 25 == 0:
            logging.info(f'  …{i}/{len(universe)} 已扫, 命中 {len(hits)}')
        try:
            weekly = _history(tk, period='2y', interval='1wk')
            if weekly is None or weekly.empty:
                continue
            daily = _history(tk, period='1y', interval='1d')
            s = consolidation_breakout(weekly, daily, vol_mult=opts.vol_mult)
            if s:
                s['ticker'] = tk
                s['close'] = round(float(weekly['close'].iloc[-1]), 2)
                s['mktcap_b'] = round(mv / 1e9, 1) if mv else None
                hits.append(s)
        except Exception as e:
            logging.debug(f'{tk}: {e}')

    us_fundamentals.close_futu()

    hits.sort(key=lambda h: -h['vol_ratio'])
    fired = [h for h in hits if h['tier'] == 'HIT']
    watch = [h for h in hits if h['tier'] == 'WATCH']
    logging.info(f'扫描完成: {len(universe)} 只候选 → 放量突破 {len(fired)} · 待放量 {len(watch)}')

    def _tbl(items):
        return tab_mod.tabulate(
            [[h['ticker'], h['close'], h.get('mktcap_b', '—'),
              f"{h['base_width']}%", f"{h['base_drift']}%", f"{h['vol_ratio']}×",
              h['entry'], h['stop'], h['target'],
              f"{h['rr']}{'✓' if h['rr_ok'] else ''}"] for h in items],
            headers=['Ticker', 'Close', 'Cap$B', '基底宽', '漂移', '量比',
                     'Entry', 'Stop', 'Target', 'R:R'], tablefmt='simple')

    print(f'\n[ 横盘突破 ]  域: 全美 价≥{opts.min_price} 市值≥{opts.min_mktcap:.0e} '
          f'(扫{len(universe)}只)  闸: 基底宽≤{BASE_MAX_WIDTH:.0%} 漂移≤{BASE_MAX_DRIFT:.0%} 放量≥{opts.vol_mult}×')
    print(f'\n── HIT 放量突破({len(fired)}) ── 窄基底+平+首破+放量, 可直接做')
    print(_tbl(fired) if fired else '  本周无放量横盘突破。')
    print(f'\n── WATCH 待放量({len(watch)}, 显示前{opts.watch_top}) ── 已窄基底首破, 只差放量, 下周放量即确认')
    print(_tbl(watch[:opts.watch_top]) if watch else '  无。')

    out_dir = os.path.join(RESULT_DIR, 'us_breakout_screen')
    if os.path.isdir(RESULT_DIR):
        os.makedirs(out_dir, exist_ok=True)
    out_file = opts.output or (f'{out_dir}/us_breakout_screen_{datetime.datetime.now():%Y%m%d}.csv'
                               if os.path.isdir(RESULT_DIR) else None)
    if out_file and hits:
        pd.DataFrame(hits).to_csv(out_file, index=False)
        logging.info(f'结果 CSV → {out_file}(含 tier 列)')
    print('\n  (纯选股, 未改 watchlist)')


if __name__ == '__main__':
    main()
