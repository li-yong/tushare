# coding: utf-8
"""
US TR-Surge Scanner — 近 N 日真实波幅(True Range)持续放大

度量: 每日 TR% = TR / 前收 × 100, 其中 TR = max(高-低, |高-前收|, |低-前收|)。
  注意这是【波幅】不是收盘涨跌幅 —— 验证案例 MU 2026-04-02 当天收盘 -0.44%,
  但三日 TR% = 8.19/11.85/7.52, 合计 27.6%, 正是本脚本要抓的"连续大波幅"状态。

条件(全部可调):
  1. 最近 --days 个交易日, 每日 TR% ≥ --daily (默认 3%)
  2. 这 --days 日 TR% 合计 > --total (默认 15%)
  3. 当前收盘距 --hi-days (默认 45) 日最高收盘 ≤ --near-high (默认 15%) —
     只要"仍在高位区"的放大, 排除深跌途中的下坡波动。
     注意: 该过滤会滤掉原验证案例 MU 2026-04-02 (当时距45日高 -20.6%,
     处于 3 月急跌后的修复段) — 要复现旧行为用 --near-high 100。

两个模式(对齐 t_us_pullback_shock / t_us_gap_scan):
  --scan [--universe both|ndx|all] [--top N]   全市场扫, 按合计 TR% 排序, 存 CSV
  --ticker SYM                             单票诊断: 逐日 TR% + 是否命中
  --asof YYYY-MM-DD                        回测开关(两模式通用): 只用到那天为止的数据

数据: 复用 t_us_tech_swing 的 yfinance 缓存(_fetch_daily, ADR-0001)。

Usage:
  python t_us_tr_surge.py --ticker MU --asof 2026-04-02
  python t_us_tr_surge.py --scan --universe ndx
  python t_us_tr_surge.py --scan --days 5 --total 20 --daily 2.5 --asof 2026-04-02
"""

import os
import sys
import json
import logging
import datetime

import numpy as np
import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _fetch_daily

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR   = '/home/ryan/DATA/result'
UNIVERSE_DIR = '/home/ryan/DATA/DAY_Global/US_universe'

# ── 默认门槛(命令行可调) ─────────────────────────────────────────────────────
DAYS          = 3      # 最近 N 个交易日
TOTAL_MIN     = 15.0   # N 日 TR% 合计 > 此值(%)
DAILY_MIN     = 3.0    # 每日 TR% ≥ 此值(%)
HI_DAYS       = 45     # 高位区参照窗口(交易日)
NEAR_HIGH_PCT = 15.0   # 收盘距 HI_DAYS 日最高收盘 ≤ 此值(%)
MIN_BARS      = 30     # 数据下限(交易日)


def annotate(df: pd.DataFrame, hi_days: int = None) -> pd.DataFrame:
    d = df.copy()
    prev = d['close'].shift(1)
    tr = pd.concat([(d['high'] - d['low']),
                    (d['high'] - prev).abs(),
                    (d['low'] - prev).abs()], axis=1).max(axis=1)
    d['tr_pct'] = tr / prev * 100.0
    d['r1d']    = (d['close'] / prev - 1.0) * 100.0
    hi = d['close'].rolling(hi_days or HI_DAYS).max()
    d['dfh'] = (d['close'] / hi - 1.0) * 100.0      # 距N日最高收盘(≤0)
    return d


def check_window(d: pd.DataFrame, days: int, total_min: float, daily_min: float,
                 near_high: float = None):
    """取最近 days 日, 返回 (是否命中, 窗口df, 合计TR%, 最小日TR%, 距高%)。
    命中 = TR合计 > total_min 且 每日 ≥ daily_min 且 收盘距N日高 ≤ near_high。"""
    if near_high is None:
        near_high = NEAR_HIGH_PCT
    w = d.tail(days)
    if len(w) < days or w['tr_pct'].isna().any():
        return False, w, np.nan, np.nan, np.nan
    tot = float(w['tr_pct'].sum())
    lo  = float(w['tr_pct'].min())
    dfh = float(w['dfh'].iloc[-1]) if pd.notna(w['dfh'].iloc[-1]) else np.nan
    hit = (tot > total_min) and (lo >= daily_min) \
          and pd.notna(dfh) and (dfh >= -near_high)
    return hit, w, tot, lo, dfh


# ── 模式一: 全市场扫 ─────────────────────────────────────────────────────────
def _load_universe(name: str) -> list:
    path = os.path.join(UNIVERSE_DIR, f'{name}.json')
    if not os.path.exists(path):                       # 缓存缺失时现抓 (含当日缓存)
        from t_us_undervalue import load_universe
        return load_universe(name, force=False)
    with open(path) as f:
        return json.load(f)


def run_scan(universe, days, total_min, daily_min, top, asof=None,
             hi_days=None, near_high=None):
    hi_days = hi_days or HI_DAYS
    near_high = NEAR_HIGH_PCT if near_high is None else near_high
    tickers = _load_universe(universe)
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    logging.info(f'scan universe={universe} ({len(tickers)} 只), days={days}, '
                 f'合计>{total_min}%, 每日≥{daily_min}%, '
                 f'距{hi_days}日高≤{near_high}%{asof_txt}')
    rows = []
    for i, t in enumerate(tickers):
        try:
            df = _fetch_daily(t)
            if df.empty:
                continue
            d = annotate(df, hi_days)
            if asof is not None:
                d = d[d.index <= asof]
            if len(d) < MIN_BARS:
                continue
            hit, w, tot, lo, dfh = check_window(d, days, total_min, daily_min, near_high)
            if not hit:
                continue
            net = (w['close'].iloc[-1] / d['close'].iloc[-days - 1] - 1.0) * 100.0 \
                  if len(d) > days else np.nan
            rows.append({
                'ticker':   t,
                'last_dt':  w.index[-1].date(),
                'close':    round(float(w['close'].iloc[-1]), 2),
                'tr_sum%':  round(tot, 2),
                'tr_min%':  round(lo, 2),
                'daily_tr%': '/'.join(f'{v:.1f}' for v in w['tr_pct']),
                'net_chg%': round(float(net), 2) if pd.notna(net) else np.nan,
                'dfh%':     round(dfh, 1),
            })
        except Exception as e:
            logging.warning(f'{t}: {e}')
        if (i + 1) % 100 == 0:
            logging.info(f'  ...{i + 1}/{len(tickers)}')

    if not rows:
        print(f'无命中: 近 {days} 日无"每日TR%≥{daily_min} 且合计>{total_min} '
              f'且距{hi_days}日高≤{near_high}%"的名。')
        return
    out = pd.DataFrame(rows).sort_values('tr_sum%', ascending=False).reset_index(drop=True)
    show = out.head(top)
    print(f"\nTR连续放大·高位区  (universe={universe}{asof_txt}, days={days}, "
          f"合计>{total_min}% & 每日≥{daily_min}% & 距{hi_days}日高≤{near_high}%, "
          f"命中 {len(out)})")
    print(tab_mod.tabulate(show, headers='keys', tablefmt='github', showindex=False))
    print(f"\n列义: tr_sum%=近{days}日TR%合计(排序键, 越大=连续波幅越猛) · "
          f"tr_min%=窗口内最小单日TR%(每日都≥{daily_min}才入选, 排除一日脉冲) · "
          "daily_tr%=逐日TR%(旧→新, 看波幅是在放大还是收敛) · "
          "net_chg%=同窗口收盘净涨跌 —— TR 只量波幅不辨方向, 方向看这列 · "
          f"dfh%=收盘距{hi_days}日最高收盘(高位过滤门槛, 越接近0越贴着高点)。")

    # ── 简要解读 ──
    up   = out[out['net_chg%'] >= 3.0]
    down = out[out['net_chg%'] <= -3.0]
    chop = out[(out['net_chg%'] > -3.0) & (out['net_chg%'] < 3.0)]
    med  = float(out['tr_sum%'].median())
    print(f"\n解读: 命中 {len(out)} 只(中位合计TR {med:.1f}%) — "
          f"上行 {len(up)} · 下行 {len(down)} · 拉锯 {len(chop)}(净涨跌±3%内)。")
    if len(up):
        print(f"  上行放大(净涨跌≥+3%): {', '.join(up['ticker'].head(8))}"
              f"{' …' if len(up) > 8 else ''} —— 重定价/抢筹态, 顺势但追高风险大, "
              f"配合 gap_scan/key_kline 找回踩入场。")
    if len(down):
        print(f"  下行放大(净涨跌≤-3%): {', '.join(down['ticker'].head(8))}"
              f"{' …' if len(down) > 8 else ''} —— 派发/恐慌态, 强趋势名可对照 "
              f"pullback_shock 看是否构成急跌买点。")
    if len(chop):
        print(f"  拉锯(波幅大但没走出方向): {', '.join(chop['ticker'].head(8))}"
              f"{' …' if len(chop) > 8 else ''} —— 多空激烈换手、方向未决, "
              f"一旦选择方向往往走得远, 是最值得盯确认K线的一档。")
    print("  注: 这是波动状态读数, 非买卖信号 —— 命中面越广=池子整体波动扩散越强, "
          "常对应事件密集期(财报/宏观)。")

    tag = asof.strftime('%Y%m%d') if asof is not None else datetime.date.today().strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_tr_surge')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_tr_surge_{universe}_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


# ── 模式二: 单票诊断 ─────────────────────────────────────────────────────────
def run_ticker(ticker, days, total_min, daily_min, asof=None,
               hi_days=None, near_high=None):
    hi_days = hi_days or HI_DAYS
    near_high = NEAR_HIGH_PCT if near_high is None else near_high
    df = _fetch_daily(ticker)
    if df.empty:
        logging.error(f'{ticker}: 无数据'); return
    d = annotate(df, hi_days)
    if asof is not None:
        d = d[d.index <= asof]
    if len(d) < days + 1:
        logging.error(f'{ticker}: 数据不足'); return
    hit, w, tot, lo, dfh = check_window(d, days, total_min, daily_min, near_high)
    asof_txt = f' (as-of {d.index[-1].date()})' if asof is not None else ''
    print(f"\n{ticker} 近 {days} 日 TR 诊断{asof_txt}  收 ${d['close'].iloc[-1]:.2f}")
    rows = [{'date': ts.date(),
             'close': round(float(r['close']), 2),
             'r1d%':  round(float(r['r1d']), 2),
             'tr%':   round(float(r['tr_pct']), 2),
             'ok':    '✓' if r['tr_pct'] >= daily_min else '✗'}
            for ts, r in w.iterrows()]
    print(tab_mod.tabulate(pd.DataFrame(rows), headers='keys', tablefmt='github', showindex=False))
    print(f"  合计 TR% = {tot:.2f} ({'>' if tot > total_min else '≤'} {total_min}) · "
          f"最小单日 = {lo:.2f} ({'≥' if lo >= daily_min else '<'} {daily_min}) · "
          f"距{hi_days}日高 = {dfh:+.1f}% ({'≥' if pd.notna(dfh) and dfh >= -near_high else '<'} -{near_high})")
    print(f"  → {'★ 命中: 高位区 TR 连续放大' if hit else '未命中'}")


#### MAIN ####
def main():
    parser = OptionParser()
    parser.add_option('--scan', action='store_true', default=False, dest='scan',
                      help='全市场扫 近N日TR%持续放大')
    parser.add_option('--ticker', dest='ticker', help='单票诊断, 如 MU')
    parser.add_option('--universe', dest='universe', default='both',
                      help='both|ndx (配 --scan, 默认 both)')
    parser.add_option('--days', dest='days', type='int', default=DAYS,
                      help=f'最近 N 个交易日 (默认 {DAYS})')
    parser.add_option('--total', dest='total', type='float', default=TOTAL_MIN,
                      help=f'N 日 TR%% 合计门槛, 需 > 此值 (默认 {TOTAL_MIN})')
    parser.add_option('--daily', dest='daily', type='float', default=DAILY_MIN,
                      help=f'每日 TR%% 门槛, 需 ≥ 此值 (默认 {DAILY_MIN})')
    parser.add_option('--hi-days', dest='hi_days', type='int', default=HI_DAYS,
                      help=f'高位区参照窗口(交易日, 默认 {HI_DAYS})')
    parser.add_option('--near-high', dest='near_high', type='float', default=NEAR_HIGH_PCT,
                      help=f'收盘距 hi-days 日最高收盘 ≤ 此值%% (默认 {NEAR_HIGH_PCT}; '
                           f'设 100 等于关掉该过滤)')
    parser.add_option('--top', dest='top', type='int', default=40,
                      help='显示前 N (配 --scan, 默认 40)')
    parser.add_option('--asof', dest='asof', default=None,
                      help='回测某历史日 YYYY-MM-DD: 只用到那天为止的数据(两模式通用)')
    (opt, _) = parser.parse_args()

    asof = None
    if opt.asof:
        try:
            asof = pd.Timestamp(opt.asof).normalize()
        except ValueError:
            print(f'--asof 日期无法解析: {opt.asof}'); sys.exit(1)

    if opt.ticker:
        run_ticker(opt.ticker.upper(), opt.days, opt.total, opt.daily, asof,
                   opt.hi_days, opt.near_high)
    elif opt.scan:
        if opt.universe not in ('both', 'ndx', 'sp500', 'all', 'r2000ht'):
            print('--universe 仅支持 both|ndx|sp500|all|r2000ht'); sys.exit(1)
        run_scan(opt.universe, opt.days, opt.total, opt.daily, opt.top, asof,
                 opt.hi_days, opt.near_high)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
