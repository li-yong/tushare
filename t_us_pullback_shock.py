# coding: utf-8
"""
US Pullback-Shock Scanner — 强势股·单日急跌 回调买点

实证动机(事件研究, 15y SP500∪NDX, 对 QQQ 超额, 见 scratchpad/event_study 与本轮对话):
  A. 长期上涨中的单日大跌 = 好买点。趋势(站上200日线 & 半年≥+20% & 距252日高≤15%)
     里出现单日急跌后, 收盘买入的前向表现(对 QQQ 超额):
        单日≤-5%(n=1854): 5日+0.96% · 21日+3.43%(超额+1.69) · 63日+10.16%(超额+4.14)
        单日≤-7%(n=564 , ≈"MU式"): 5日+1.06% · 21日+4.40%(超额+2.54) · 63日+12.50%(超额+6.07)
     短期(5日)即翻正——几乎不用再挨跌; 跌得越狠、拿得越久 edge 越大, 赢 QQQ 概率 52~56%。
  对照 B(长期下跌后单日大涨, "NOW式")头一周为负、赢 QQQ 概率<50% 且被幸存者偏差高估——
     故本脚本只做 A 侧(强势股急跌), 不做追涨弱势股。

与既有筛子的分工: gap_scan 找【向上缺口】(重定价强度), breakout 找【横盘突破】,
  key_kline 找【择时关键K线】。本脚本专职【强势股回调的加仓择时】—— 短期就翻正、
  edge 稳, 适合给已在上升趋势的优质名做逢跌进场/加仓的时点。

指标测当下状态, 不赌方向(docs/indicator_design_state_vs_debt): 只标注"现在处于
  强趋势急跌态", 不承诺反弹。退出策略归 scanner(ADR-0002): 止损 = 急跌日最低(跌破
  即回调失败), 目标 = 252日高(收复前高)。放量/收盘强度仅作状态注记, 不做硬门槛
  (bottom-entry 实证: 机械放量过滤不贡献 EV)。

数据: 复用 t_us_tech_swing 的 yfinance 缓存(_fetch_daily, ADR-0001)。yfinance-only,
  不依赖 Futu/OpenD。

两个模式(对齐 t_us_gap_scan / t_us_key_kline):
  --scan [--universe both|ndx|all] [--lookback N] [--top N]
      全市场扫【当前处于强趋势 + 近 N 交易日内单日急跌】的名, 两档 + 原因标注:
        Tier A = 单日≤-7% (急跌更深, 前向 edge 更大);
        Tier B = 单日 -7%~-5%。A 在前, 各按 severity(跌幅)× 新鲜度排。
  --ticker SYM [--asof ...]
      单票诊断: 当前趋势/信号状态 + 近 120 日历史急跌事件及 21 日后表现。

  --asof YYYY-MM-DD  (两模式通用)
      回测开关: 只用到那天为止的数据, 把"今天"锚到该日; CSV 以 asof 日命名。

Usage:
  python t_us_pullback_shock.py --scan --universe both
  python t_us_pullback_shock.py --scan --universe ndx --top 30
  python t_us_pullback_shock.py --ticker MU
  python t_us_pullback_shock.py --scan --universe ndx --asof 2025-05-01
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

# ── 门槛(都做成常量, 可调) ────────────────────────────────────────────────────
MA_LONG        = 200     # 长期趋势脊梁(交易日)
HI_LOOKBACK    = 252     # 52周高窗口
VOL_WIN        = 20      # 放量倍数均量窗口(状态注记, 不做门槛)
TREND_6M_MIN   = 20.0    # 趋势: 半年(126日)涨幅 ≥ 此值(%)
NEAR_HIGH_MAX  = -15.0   # 趋势: 收盘距252日高 ≥ 此值(%)(即在高点15%以内)
SHOCK_1D       = -5.0    # 触发: 单日跌幅 ≤ 此值(%) 入选
STRONG_SHOCK   = -7.0    # Tier A: 单日跌幅 ≤ 此值(%)(≈"MU式"急跌)
SCAN_LOOKBACK  = 3       # 只看最近 N 个交易日内新生的急跌(越新越可操作)


# ── 核心: 在日线 df 上标注趋势/急跌状态 ────────────────────────────────────────
def annotate(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d['ma_long'] = d['close'].rolling(MA_LONG).mean()
    d['hi252']   = d['close'].rolling(HI_LOOKBACK).max()
    d['r1d']     = d['close'] / d['close'].shift(1) - 1.0
    d['r6m']     = d['close'] / d['close'].shift(126) - 1.0
    d['dfh']     = d['close'] / d['hi252'] - 1.0                    # 距252日高(≤0)
    vol_avg      = d['volume'].rolling(VOL_WIN).mean().shift(1)     # 不含当日均量
    d['vol_mult'] = d['volume'] / vol_avg
    tr = pd.concat([(d['high'] - d['low']),
                    (d['high'] - d['close'].shift(1)).abs(),
                    (d['low'] - d['close'].shift(1)).abs()], axis=1).max(axis=1)
    d['atr']     = tr.rolling(14).mean()                           # 止损缓冲用
    # 趋势态(不含当日急跌): 站上200线 & 半年强 & 距高不远
    d['in_trend'] = (d['close'] > d['ma_long']) & (d['r6m'] >= TREND_6M_MIN / 100.0) \
                    & (d['dfh'] >= NEAR_HIGH_MAX / 100.0)
    return d


def _rr(entry, stop, target):
    """盈亏比 = (目标-入场)/(入场-止损); 止损无效则 nan。"""
    risk = entry - stop
    if risk <= 0:
        return np.nan
    return round((target - entry) / risk, 2)


# ── 模式一: 全市场扫强势股急跌 ────────────────────────────────────────────────
def _load_universe(name: str) -> list:
    path = os.path.join(UNIVERSE_DIR, f'{name}.json')
    if not os.path.exists(path):                       # 缓存缺失时现抓 (含当日缓存)
        from t_us_undervalue import load_universe
        return load_universe(name, force=False)
    with open(path) as f:
        return json.load(f)


def _market_state(asof):
    """大盘 regime via t_us_tech_swing (QQQ+SOXX 20周线); asof 时临时锚定保证一致。"""
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


def run_scan(universe, lookback, top, asof=None):
    tickers = _load_universe(universe)
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    mstate = _market_state(asof)
    weak = (mstate == 'WEAK')
    logging.info(f'scan universe={universe} ({len(tickers)} 只), lookback={lookback} 交易日'
                 f'{asof_txt}, 市场={mstate}')
    rows = []
    for i, t in enumerate(tickers):
        try:
            df = _fetch_daily(t)
            if df.empty or len(df) < HI_LOOKBACK + 2:
                continue
            d = annotate(df)
            if asof is not None:                     # 站在 asof 当天, 丢弃其后所有 bar
                d = d[d.index <= asof]
            if len(d) < HI_LOOKBACK + 2:
                continue
            recent = d.tail(lookback)
            # 触发日: 近 lookback 日内, 当日急跌 且 急跌【前一天】处于强趋势态。
            cand = recent[(recent['r1d'] <= SHOCK_1D / 100.0)
                          & (recent['in_trend'].shift(1, fill_value=False))]
            if cand.empty:
                continue
            # 取最近(最新)那根急跌
            ts = cand.index[-1]
            r = cand.loc[ts]
            pos = d.index.get_loc(ts)
            age = len(d) - 1 - pos                     # 距今交易日数(0=今天)
            entry = d['close'].iloc[-1]                # 入场 = 最新收盘
            # 止损 = 急跌日最低 下方留 0.5×ATR 缓冲(跌破=回调失败; 缓冲避免贴脸止损)
            atr = float(r['atr']) if pd.notna(r['atr']) else 0.0
            stop  = float(d['low'].iloc[pos]) - 0.5 * atr
            target = float(r['hi252'])                 # 目标 = 收复252日高
            if entry <= stop:                          # 现价已破急跌日低 = 回调已失败, 不再是买点
                continue
            rr = _rr(entry, stop, target)
            drop = float(r['r1d']) * 100.0
            tier = 'A' if drop <= STRONG_SHOCK else 'B'
            vol = r['vol_mult']
            flags = []
            if pd.notna(vol) and vol >= 1.5:
                flags.append(f'放量{vol:.2f}×')
            if age == 0:
                flags.append('今日')
            else:
                flags.append(f'{age}d前')
            if weak:
                flags.append('⚠弱市')
            stop_pct = (entry - stop) / entry * 100.0
            rows.append({
                'ticker':  t,
                'tier':    tier,
                'shock_dt': ts.date(),
                'age':     age,
                'drop%':   round(drop, 2),           # 触发日单日跌幅
                'r6m%':    round(float(r['r6m']) * 100.0, 1),   # 半年趋势强度
                'dfh%':    round(float(r['dfh']) * 100.0, 1),   # 距252日高
                'vol_x':   round(float(vol), 2) if pd.notna(vol) else np.nan,
                'entry':   round(float(entry), 2),
                'stop':    round(stop, 2),
                'stop%':   round(stop_pct, 1),        # 到止损(急跌日低)缓冲
                'target':  round(target, 2),
                'R:R':     rr,
                'reason':  '·'.join(flags) if flags else '—',
            })
        except Exception as e:
            logging.warning(f'{t}: {e}')
        if (i + 1) % 100 == 0:
            logging.info(f'  ...{i + 1}/{len(tickers)}')

    if not rows:
        print('无命中: 近期无"强趋势 + 单日急跌"的回调买点。')
        return
    out = pd.DataFrame(rows)
    out['_tr'] = out['tier'].map({'A': 0, 'B': 1})    # A 在前(急跌更深, edge 更大)
    out = out.sort_values(['_tr', 'drop%', 'age'], ascending=[True, True, True]) \
             .drop(columns='_tr').reset_index(drop=True)
    show = out.head(top)
    n_a = int((out['tier'] == 'A').sum()); n_b = int((out['tier'] == 'B').sum())
    print(f"\n强势股急跌·回调买点  (universe={universe}{asof_txt}, 市场={mstate}"
          f"{' · ⚠弱市' if weak else ''}, 命中 {len(out)}: A {n_a} · B {n_b})")
    print(tab_mod.tabulate(show, headers='keys', tablefmt='github', showindex=False))
    print(f"\nTier A=单日≤{STRONG_SHOCK:.0f}%(急跌更深, 前向 edge 更大) · "
          f"Tier B=单日{SHOCK_1D:.0f}%~{STRONG_SHOCK:.0f}%; 均要求急跌前处于强趋势"
          f"(>200日线 & 半年≥+{TREND_6M_MIN:.0f}% & 距高≤{-NEAR_HIGH_MAX:.0f}%)。")
    print("列义: age=距今交易日(0=今日) · drop%=触发日单日跌幅 · r6m%=半年涨幅 · "
          "dfh%=距252日高 · vol_x=对20日均量(仅注记) · stop=急跌日最低(天然止损) · "
          "target=252日高 · R:R=盈亏比。")
    print("实证基率(15y, 对QQQ超额): ≤-7% 21日+4.4%(超额+2.5)/63日+12.5%(超额+6.1); "
          "≤-5% 21日+3.4%(超额+1.7)/63日+10.2%(超额+4.1)。短期即翻正, 拿得越久 edge 越大。")

    tag = asof.strftime('%Y%m%d') if asof is not None else datetime.date.today().strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_pullback_shock')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_pullback_shock_{universe}_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


# ── 模式二: 单票诊断 ──────────────────────────────────────────────────────────
def run_ticker(ticker, asof=None):
    df = _fetch_daily(ticker)
    if df.empty:
        logging.error(f'{ticker}: 无数据'); return
    d = annotate(df)
    if asof is not None:
        d = d[d.index <= asof]
    if len(d) < HI_LOOKBACK + 2:
        logging.error(f'{ticker}: 数据不足'); return
    last = d.iloc[-1]
    asof_txt = f' (as-of {d.index[-1].date()})' if asof is not None else ''
    print(f"\n{ticker} 现状{asof_txt}  收 ${last['close']:.2f}")
    trend_prev = bool(d['in_trend'].iloc[-2])         # 急跌前(昨日)是否强趋势
    print(f"  趋势: >200线 {'✓' if last['close']>last['ma_long'] else '✗'} · "
          f"半年 {last['r6m']*100:+.1f}%(需≥+{TREND_6M_MIN:.0f}) · "
          f"距252高 {last['dfh']*100:+.1f}%(需≥{NEAR_HIGH_MAX:.0f}) → "
          f"{'强趋势' if trend_prev else '不满足趋势'}")
    is_signal = (last['r1d'] <= SHOCK_1D / 100.0) and trend_prev
    if is_signal:
        atr = float(last['atr']) if pd.notna(last['atr']) else 0.0
        entry = last['close']; stop = float(last['low']) - 0.5 * atr
        target = float(last['hi252'])
        tier = 'A' if last['r1d']*100 <= STRONG_SHOCK else 'B'
        print(f"  ★ 今日信号 Tier {tier}: 单日 {last['r1d']*100:+.2f}% 急跌")
        print(f"    买 ${entry:.2f} / 止损 ${stop:.2f}(急跌日低, -{(entry-stop)/entry*100:.1f}%) / "
              f"目标 ${target:.2f}(252日高) / R:R {_rr(entry,stop,target)}")
    else:
        print("  今日无信号(非强趋势急跌态)。")

    # 历史急跌事件(近 120 日) + 21 日后表现
    win = d.tail(120)
    ev = win[(win['r1d'] <= SHOCK_1D / 100.0) & (win['in_trend'].shift(1, fill_value=False))]
    print(f"\n近120日 强趋势急跌事件({len(ev)} 次) 及 21 日后表现:")
    if ev.empty:
        print("  (无)")
        return
    rows = []
    cl = d['close']
    for ts in ev.index:
        p = d.index.get_loc(ts)
        f21 = cl.iloc[p+21]/cl.iloc[p]-1.0 if p+21 < len(cl) else np.nan
        rows.append({'date': ts.date(), 'drop%': round(d['r1d'].iloc[p]*100,2),
                     'fwd21d%': round(f21*100,2) if pd.notna(f21) else np.nan})
    print(tab_mod.tabulate(pd.DataFrame(rows), headers='keys', tablefmt='github', showindex=False))


#### MAIN ####
def main():
    parser = OptionParser()
    parser.add_option('--scan', action='store_true', default=False, dest='scan',
                      help='全市场扫强趋势+近期单日急跌的回调买点')
    parser.add_option('--ticker', dest='ticker', help='单票诊断, 如 MU')
    parser.add_option('--universe', dest='universe', default='both',
                      help='both|ndx (配 --scan, 默认 both)')
    parser.add_option('--lookback', dest='lookback', type='int', default=SCAN_LOOKBACK,
                      help=f'近 N 交易日内的急跌(配 --scan, 默认 {SCAN_LOOKBACK})')
    parser.add_option('--top', dest='top', type='int', default=40,
                      help='显示前 N(配 --scan, 默认 40)')
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
        run_ticker(opt.ticker.upper(), asof)
    elif opt.scan:
        if opt.universe not in ('both', 'ndx', 'sp500', 'all', 'r2000ht'):
            print('--universe 仅支持 both|ndx|sp500|all|r2000ht'); sys.exit(1)
        run_scan(opt.universe, opt.lookback, opt.top, asof)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
