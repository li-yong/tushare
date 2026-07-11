# coding: utf-8
"""
US Earnings-React Scanner — 财报强反应·守住 (PEAD 确认入场候选)

形态: 财报刚公布(反应 bar 距今 ≤ --er-days 个交易日), 财报日或次日单日收盘涨幅
≥ --chg%, 且最新收盘仍未跌破 close(财报日-1) — 即市场对财报的强定价没有被回吐。
这正是 t_us_tech_swing 静默期规则里说的"财报后跳空确认是更好的入场"的那半句,
之前一直没有独立的扫描把它做出来(key_kline 的 EARNINGS_GAP 只认开盘跳空≥2%,
会漏掉不跳空但盘中拉出大阳的财报反应日; 新鲜窗口也是 15 bar 不是 5 日)。

三条件 (与 key_kline EARNINGS_GAP 的差异见上):
  1. 最近一次财报已公布, 其反应首日(财报日 bar) 距今 ≤ ER_WITHIN 交易日;
  2. 财报日 或 财报日+1 的收盘涨幅 ≥ CHG_MIN% (盘前公布→涨在当日,
     盘后公布→涨在次日, 两根都查即两种口径全覆盖);
  3. 最新收盘 ≥ close(财报日-1) — 参考止损位: 跌破 = 财报强反应被完全回吐,
     形态证伪 (ADR-0002 日收盘判)。

⚠ 定位: 候选发现层, NOT a proven signal source — huice 可回放此源
(--source earnings_react; 过去的财报公告日是历史事实无前视, caveat = 退市票
缺日历的幸存者偏差 + limit=12 日历只覆盖 ~2020+), live 前向样本靠每日 CSV
积累, 两者互相印证。PEAD 文献上有正漂移, 但 return_concentration 实证也提醒
收益集中在少数爆发日, 追高第 2-5 天的入场质量需积累样本判断。放量只作注记
不作门槛(bottom-entry 实证)。

数据: 复用 t_us_tech_swing 的 yfinance 缓存 (_fetch_daily, ADR-0001)。
财报日历: t_us_tech_swing.fetch_earnings_calendar (yfinance get_earnings_dates
的共享磁盘缓存, 3日 TTL, 失败服务陈旧缓存; 与 key_kline 共用) — 且只对价格
漏斗幸存者调用(近窗口内有 ≥CHG_MIN% 单日大阳的名, 通常个位数~几十只), 全池
扫描不会打爆接口; 失败/无数据 → 该票跳过并 warning (优雅降级)。

模式 (对齐 t_us_pullback_shock / t_us_low_bounce):
  --scan [--universe both|ndx|sp500|all|r2000ht] 扫描池子, 命中按新鲜度+涨幅排
  --ticker SYM    单票诊断: 三条件逐条 ✓/✗ + 历史财报反应表(react%/守住/21日后)
  --asof YYYY-MM-DD (两模式通用) 只用 ≤该日的 bar 回放。⚠ 财报日历是"今天抓的
      历史日历"—— 过去的财报日期本身不重写, 但 limit 窗口有限, 太久远的 asof
      可能取不到当时的财报日; 结果标注 asof 即可, 别当严格点位回测。

Usage:
  python t_us_earnings_react.py --scan --universe all
  python t_us_earnings_react.py --ticker NVDA
  python t_us_earnings_react.py --scan --asof 2026-06-26
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

import t_us_tech_swing as _sw
from t_us_tech_swing import _fetch_daily

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR   = '/home/ryan/DATA/result'
UNIVERSE_DIR = '/home/ryan/DATA/DAY_Global/US_universe'

# ── 默认门槛 (命令行可调) ────────────────────────────────────────────────────
CHG_MIN    = 7.0    # 财报日/次日 单日收盘涨幅 ≥ 此值(%)
ER_WITHIN  = 5      # 财报日 bar 距今 ≤ N 个交易日
EARN_LIMIT = 12     # yfinance 拉多少个财报日期 (同 key_kline)
VOL_WIN    = 20     # 放量倍数均量窗口 (仅注记, 不做门槛)
MIN_BARS   = 40     # 数据下限(交易日)


def fetch_er_dates(ticker: str):
    """已公布+未来的财报日期列表(naive normalize, 升序); 失败 → None (跳过该票)。

    走 t_us_tech_swing.fetch_earnings_calendar 的共享磁盘缓存(3日 TTL, 失败服务
    陈旧缓存) — 与 key_kline 共用同一份日历, 重跑/跨脚本不重复打 yfinance。"""
    cal = _sw.fetch_earnings_calendar(ticker, limit=EARN_LIMIT)
    if not cal:
        return None
    return [r['date'] for r in cal]


def evaluate(d: pd.DataFrame, er_dates: list) -> 'dict | None':
    """对最近一次已公布财报评估三条件; 无可评估财报 → None。

    返回 dict 含逐条件真值 + 反应明细, --scan 只收全过的, --ticker 打印全部。"""
    idx = d.index
    close = d['close'].values
    vol = d['volume'].values
    last_i = len(d) - 1

    g = None
    er = None
    for ed in reversed(er_dates):                 # 最近的、已有反应 bar 的财报
        i = idx.searchsorted(ed, side='left')
        if i >= len(idx):
            continue                              # 未来财报 / 财报日尚未开盘
        g, er = i, ed
        break
    if g is None or g <= 0:
        return None

    er_age = last_i - g                           # 财报日 bar 距今交易日数
    # 反应涨幅: 财报日 (close[g]/close[g-1]) 与 次日 (close[g+1]/close[g]) 取大——
    # 盘前公布涨在当日, 盘后公布(多数)涨在次日, 两根都查即全覆盖。
    chg_e = close[g] / close[g - 1] - 1.0
    chg_e1 = close[g + 1] / close[g] - 1.0 if g + 1 <= last_i else np.nan
    if not np.isnan(chg_e1) and chg_e1 > chg_e:
        react, react_i, react_day = chg_e1, g + 1, 'E+1'
    else:
        react, react_i, react_day = chg_e, g, 'E'

    ref = float(close[g - 1])                     # close(财报日-1) = 参考止损
    entry = float(close[last_i])

    v_avg = np.nanmean(vol[max(0, react_i - VOL_WIN):react_i]) if react_i > 0 else np.nan
    vol_x = vol[react_i] / v_avg if (v_avg and not np.isnan(v_avg) and v_avg > 0) else np.nan

    return {
        'er_date': er, 'g': g, 'er_age': er_age,
        'react': react, 'react_i': react_i, 'react_day': react_day,
        'react_dt': idx[react_i], 'react_age': last_i - react_i,
        'ref': ref, 'entry': entry, 'vol_x': vol_x,
        'giveback': entry / close[react_i] - 1.0,   # 现价较反应日收盘的回吐
        'c1_fresh': er_age <= ER_WITHIN,
        'c2_react': react >= CHG_MIN / 100.0,
        'c3_hold': entry >= ref,
    }


# ── 模式一: 全池扫描 ─────────────────────────────────────────────────────────
def _load_universe(name: str) -> list:
    path = os.path.join(UNIVERSE_DIR, f'{name}.json')
    if not os.path.exists(path):                  # 缓存缺失时现抓 (含当日缓存)
        from t_us_undervalue import load_universe
        return load_universe(name, force=False)
    with open(path) as f:
        return json.load(f)


def _market_state(asof):
    """大盘 regime via t_us_tech_swing; asof 时临时锚定保证一致。"""
    prev = _sw._ASOF
    try:
        if asof is not None:
            _sw._ASOF = asof
        return _sw.get_market_state()[0]
    except Exception as e:
        logging.warning(f'market_state 获取失败({e}) — 视为 ERROR, 不门控')
        return 'ERROR'
    finally:
        _sw._ASOF = prev


def run_scan(universe, top, asof=None):
    tickers = _load_universe(universe)
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    mstate = _market_state(asof)
    weak = (mstate == 'WEAK')
    logging.info(f'scan universe={universe} ({len(tickers)} 只), 财报窗口≤{ER_WITHIN}交易日, '
                 f'单日≥{CHG_MIN:.0f}%{asof_txt}, 市场={mstate}')
    rows = []
    n_funnel = 0
    for i, t in enumerate(tickers):
        try:
            df = _fetch_daily(t)
            if asof is not None:
                df = df[df.index <= asof]
            if df.empty or len(df) < MIN_BARS:
                continue
            # 价格漏斗: 近窗口内没有 ≥CHG_MIN% 的单日大阳 → 三条件必不成立,
            # 不必花一次财报日历请求。反应 bar 最晚 = 财报bar(≤5d前)+1 → 近 7 bar 全覆盖。
            r1d = df['close'].pct_change().tail(ER_WITHIN + 2)
            if not (r1d >= CHG_MIN / 100.0).any():
                continue
            n_funnel += 1
            er_dates = fetch_er_dates(t)
            if not er_dates:
                continue
            ev = evaluate(df, er_dates)
            if ev is None or not (ev['c1_fresh'] and ev['c2_react'] and ev['c3_hold']):
                continue
            flags = ['今日' if ev['react_age'] == 0 else f"{ev['react_age']}d前"]
            if not np.isnan(ev['vol_x']) and ev['vol_x'] >= 1.5:
                flags.append(f"放量{ev['vol_x']:.1f}×")
            if weak:
                flags.append('⚠弱市')
            rows.append({
                'ticker':   t,
                'er_date':  ev['er_date'].date(),
                'react_dt': ev['react_dt'].date(),
                'day':      ev['react_day'],
                'react%':   round(ev['react'] * 100.0, 2),
                'age':      ev['react_age'],
                'entry':    round(ev['entry'], 2),
                'ref_stop': round(ev['ref'], 2),      # close(财报日-1)
                'buff%':    round((ev['entry'] / ev['ref'] - 1.0) * 100.0, 1),
                'giveback%': round(ev['giveback'] * 100.0, 1),
                'vol_x':    round(float(ev['vol_x']), 2) if not np.isnan(ev['vol_x']) else np.nan,
                'reason':   '·'.join(flags),
            })
        except Exception as e:
            logging.warning(f'{t}: {e}')
        if (i + 1) % 200 == 0:
            logging.info(f'  ...{i + 1}/{len(tickers)}')
    logging.info(f'价格漏斗幸存 {n_funnel} 只 → 命中 {len(rows)} 只')

    if not rows:
        print(f'无命中: 近 {ER_WITHIN} 交易日内无"财报日/次日 ≥{CHG_MIN:.0f}% 且守住前收"的名。')
        return
    out = pd.DataFrame(rows).sort_values(['age', 'react%'], ascending=[True, False]) \
                            .reset_index(drop=True)
    show = out.head(top)
    print(f"\n财报强反应·守住  (universe={universe}{asof_txt}, 市场={mstate}"
          f"{' · ⚠弱市' if weak else ''}, 命中 {len(out)})")
    print(tab_mod.tabulate(show, headers='keys', tablefmt='github', showindex=False))
    print(f"\n条件: 财报bar距今≤{ER_WITHIN}交易日 · 财报日(E)或次日(E+1)收盘涨幅≥{CHG_MIN:.0f}% · "
          f"最新收盘≥close(财报日-1)。")
    print("列义: day=大阳在哪根(盘前公布→E/盘后公布→E+1) · age=距反应日交易日数(0=今日) · "
          "ref_stop=close(财报日-1),参考止损——收盘跌破=强反应被完全回吐,形态证伪 · "
          "buff%=现价距参考止损 · giveback%=现价较反应日收盘(负=已回吐部分) · "
          "vol_x=反应日量/20日均量(仅注记)。")
    print("⚠ 候选发现层; 回测走 huice --source earnings_react (日历带幸存者偏差 caveat), "
          "入场决策路由 gap_scan/key_kline/tech_swing, live 前向样本靠每日 CSV 积累。")

    tag = asof.strftime('%Y%m%d') if asof is not None else datetime.date.today().strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_earnings_react')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_earnings_react_{universe}_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


# ── 模式二: 单票诊断 ─────────────────────────────────────────────────────────
def run_ticker(ticker, asof=None):
    df = _fetch_daily(ticker)
    if asof is not None:
        df = df[df.index <= asof]
    if df.empty or len(df) < MIN_BARS:
        logging.error(f'{ticker}: 数据不足'); return
    er_dates = fetch_er_dates(ticker)
    if not er_dates:
        logging.error(f'{ticker}: 拿不到财报日历'); return
    asof_txt = f' (as-of {df.index[-1].date()})' if asof is not None else ''
    ev = evaluate(df, er_dates)
    print(f"\n{ticker} 财报强反应·守住 诊断{asof_txt}  收 ${df['close'].iloc[-1]:.2f}")
    if ev is None:
        print('  数据窗口内无已公布财报可评估。')
    else:
        c1, c2, c3 = ev['c1_fresh'], ev['c2_react'], ev['c3_hold']
        print(f"  最近财报 {ev['er_date'].date()} · 反应 bar {ev['react_dt'].date()}({ev['react_day']})")
        print(f"  ① 新鲜度: 财报bar距今 {ev['er_age']} 交易日 (需≤{ER_WITHIN}) {'✓' if c1 else '✗'}")
        print(f"  ② 强反应: E/E+1 最大单日 {ev['react']*100:+.2f}% (需≥+{CHG_MIN:.0f}%) {'✓' if c2 else '✗'}")
        print(f"  ③ 守住:   现价 ${ev['entry']:.2f} vs close(财报日-1) ${ev['ref']:.2f} "
              f"({(ev['entry']/ev['ref']-1)*100:+.1f}%) {'✓' if c3 else '✗'}")
        if c1 and c2 and c3:
            print(f"  ★ 三条件全过 — 参考止损 ${ev['ref']:.2f} (收盘跌破=证伪)")
        else:
            print('  未满足三条件。')

    # 历史财报反应表: 每次已公布财报的 E/E+1 反应 + 是否守住 + 21日后表现
    idx = df.index
    close = df['close']
    rows = []
    for ed in er_dates:
        g = idx.searchsorted(ed, side='left')
        if g >= len(idx) or g <= 0:
            continue
        chg_e = close.iloc[g] / close.iloc[g - 1] - 1.0
        chg_e1 = close.iloc[g + 1] / close.iloc[g] - 1.0 if g + 1 < len(idx) else np.nan
        if not np.isnan(chg_e1) and chg_e1 > chg_e:
            react, ri, day = chg_e1, g + 1, 'E+1'
        else:
            react, ri, day = chg_e, g, 'E'
        ref = close.iloc[g - 1]
        f21 = close.iloc[ri + 21] / close.iloc[ri] - 1.0 if ri + 21 < len(idx) else np.nan
        rows.append({
            'er_date': ed.date(), 'day': day,
            'react%': round(react * 100.0, 2),
            'hit≥7%': '★' if react >= CHG_MIN / 100.0 else '',
            'ref': round(float(ref), 2),
            'now_vs_ref%': round((close.iloc[-1] / ref - 1.0) * 100.0, 1),
            'fwd21d%': round(f21 * 100.0, 2) if not np.isnan(f21) else np.nan,
        })
    print(f"\n历史财报反应 ({len(rows)} 次, 数据窗口内):")
    if rows:
        print(tab_mod.tabulate(pd.DataFrame(rows), headers='keys',
                               tablefmt='github', showindex=False))
        print('  fwd21d% = 反应 bar 收盘起 21 交易日收益 (前向样本参考, 非回测)。')
    else:
        print('  (无)')


#### MAIN ####
def main():
    global CHG_MIN, ER_WITHIN
    parser = OptionParser()
    parser.add_option('--scan', action='store_true', default=False, dest='scan',
                      help='全池扫 财报强反应·守住 候选')
    parser.add_option('--ticker', dest='ticker', help='单票诊断, 如 NVDA')
    parser.add_option('--universe', dest='universe', default='all',
                      help='both|ndx|sp500|all|r2000ht|midht (配 --scan, 默认 all)')
    parser.add_option('--chg', dest='chg', type='float', default=CHG_MIN,
                      help=f'E/E+1 单日收盘涨幅门槛%% (默认 {CHG_MIN})')
    parser.add_option('--er-days', dest='er_days', type='int', default=ER_WITHIN,
                      help=f'财报bar距今 ≤ N 交易日 (默认 {ER_WITHIN})')
    parser.add_option('--top', dest='top', type='int', default=40,
                      help='显示前 N (配 --scan, 默认 40)')
    parser.add_option('--asof', dest='asof', default=None,
                      help='回放某历史日 YYYY-MM-DD (财报日历仍是今天抓的, 见文件头)')
    (opt, _) = parser.parse_args()

    CHG_MIN, ER_WITHIN = opt.chg, opt.er_days
    asof = None
    if opt.asof:
        try:
            asof = pd.Timestamp(opt.asof).normalize()
        except ValueError:
            print(f'--asof 日期无法解析: {opt.asof}'); sys.exit(1)

    if opt.ticker:
        run_ticker(opt.ticker.upper(), asof)
    elif opt.scan:
        if opt.universe not in ('both', 'ndx', 'sp500', 'all', 'r2000ht', 'midht'):
            print('--universe 仅支持 both|ndx|sp500|all|r2000ht|midht'); sys.exit(1)
        run_scan(opt.universe, opt.top, asof)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
