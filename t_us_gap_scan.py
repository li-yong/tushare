# coding: utf-8
"""
US Gap Scanner — 跳空缺口扫描(借鉴 attic/quekou.py 的思想, 重写为美股 yfinance 版)

老脚本(attic/quekou.py, CN A股 + AKShare, 已退役)的好思想: 把【隔夜跳空】和
【日内走势】拆成两个独立维度做交叉。它的毛病: 把"缺口"和"强动量"硬绑死、缺口
上下沿算反、依赖已停更的 AKShare US 数据、且缺最关键的一环——缺口回补追踪。

本脚本只保留思想, 方法重做:

缺口 = 一段"未成交的价格真空", 它在后续 K 线里充当支撑/阻力。一个【向上真缺口】
(low > 昨日最高, 图上留下真空 [昨高, 今低])值不值钱, 取决于它有没有被【回补】——
后续是否有哪天的 low 跌回缺口下沿(昨高)。未回补的向上缺口 = 买盘掌控、下沿即天然
止损; 一旦回补, setup 失效(守 ADR-0002: scanner 拥有退出策略, 缺口下沿做 stop)。

不硬筛、给缺口分级(守 docs/indicator_design_state_vs_debt: 指标测当下状态, 不赌方向):
  · 跳空幅度 gap%        — 隔夜重定价强度(分档呼应 return_concentration 研究)
  · 放量倍数 vol×        — 真突破要放量(对 20 日均量)
  · 收盘强度 close_pos   — 收在当日区间的位置 (close-low)/(high-low), 高=买盘掌控
  · 新鲜度 age           — 距今交易日数, 越近越可操作
  · 仍未回补 open        — 缺口下沿未被跌破(scan 模式硬要求)

数据: 复用 t_us_tech_swing 的 yfinance 缓存(_fetch_daily, 守 ADR-0001)。

三个模式(对齐 t_us_key_kline 的 --ticker / --scan):

  --ticker SYM [--from YYYY-MM-DD --to YYYY-MM-DD]
      单票完整【缺口台账】: 区间内每个向上真缺口 + 回补状态/持续天数, 末尾给汇总。

  --scan [--universe both|ndx|all] [--lookback N] [--top N]
      全市场扫向上缺口入场标的, 两档 + 命中原因(reason)标注:
        Tier A = 放量+收强 且 lookback 日内新生 (缺口日进, 止损紧) —— 原有逻辑;
        Tier B = 存活 ≥SCAN_SURVIVE 日未回补 (市场亲自确认需求, 入场偏晚·止损较宽,
                 即便未过放量/收强)。全样本统计: 快速回补才是反向信号, 故"未回补"
                 入选、"放量收强"仅作 reason 注记。A 在前, 各按 score 排。

  --activity [--universe both|ndx|all] [--days N] [--top N]
      全市场【缺口活跃度排行】: 近 N 日有效缺口次数(上/下分列, 标注向上未回补数),
      按次数排序。回答"哪些股票最活跃/最常跳空", 半导体等高波动名常居前列。

  --asof YYYY-MM-DD  (三模式通用)
      回测开关: 只用到那天为止的数据, 把"今天"锚到该日。回补追踪不偷看未来(站在
      asof 当天能看到的状态)。CSV 以 asof 日命名, 不覆盖当日 live 文件。可复盘
      "当时的 report 长什么样"。

Usage:
  python t_us_gap_scan.py --ticker MU --from 2025-03-01 --to 2025-05-01
  python t_us_gap_scan.py --scan --universe both --lookback 5
  python t_us_gap_scan.py --scan --universe ndx --top 30
  python t_us_gap_scan.py --activity --universe both --days 30
  python t_us_gap_scan.py --scan --universe ndx --asof 2025-05-01   # 历史复盘
"""

import os
import sys
import json
import logging
import datetime
import traceback

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

# ── 缺口判定门槛(都做成常量, 可调) ────────────────────────────────────────────
MIN_GAP_PCT  = 1.0   # 最小跳空幅度%(开盘相对昨收), 滤掉无意义的零星缺口
VOL_WIN      = 20    # 放量倍数的均量窗口(交易日)
# scan 模式"可操作"门槛
SCAN_LOOKBACK = 5    # Tier A: 只看最近 N 个交易日内新生的缺口
SCAN_MIN_VOL  = 1.2  # 缺口当日量 ≥ 1.2× 20日均量
SCAN_MIN_CPOS = 0.4  # 收盘至少在当日区间中下沿之上(排除高开大幅回落)
# Tier B: 缺口跳空后存活(下沿未被跌破)≥ N 个交易日 → 标为入场标的, 即便没过放量/收强。
# (市场用"不回补"亲自确认了需求; 全样本统计显示快速回补 X 才是反向信号。)
SCAN_SURVIVE  = 5    # Tier B 存活门槛: 缺口 age ≥ 该值且仍未回补
SCAN_MAX_AGE  = 25   # Tier B 时效上限: age 超过即视为旧闻, 不再当作可操作入场


# ── 核心: 纯函数, 在日线 df 上标注缺口属性 ─────────────────────────────────────
def annotate_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    输入: DatetimeIndex 日线, 列 open/high/low/close/volume。
    输出: 追加缺口属性列。向上真缺口 up_gap = 今低 > 昨高(图上留真空)。
    """
    d = df.copy()
    d['prev_high']  = d['high'].shift(1)
    d['prev_low']   = d['low'].shift(1)
    d['prev_close'] = d['close'].shift(1)
    d['gap_pct']    = (d['open'] / d['prev_close'] - 1.0) * 100.0
    d['up_gap']     = d['low'] > d['prev_high']          # 向上真缺口(未在开盘即回补)
    d['dn_gap']     = d['high'] < d['prev_low']          # 向下真缺口
    rng = (d['high'] - d['low']).replace(0, np.nan)
    d['close_pos']  = ((d['close'] - d['low']) / rng).clip(0, 1)   # 收盘在当日区间的位置
    vol_avg = d['volume'].rolling(VOL_WIN).mean().shift(1)         # 不含当日的均量
    d['vol_mult']   = d['volume'] / vol_avg
    return d


def upgap_fill(d: pd.DataFrame, pos: int, floor: float):
    """
    向上缺口回补追踪。pos = 缺口当日整数位置, floor = 缺口下沿(= 昨日最高)。
    后续首个 low ≤ floor 即回补。返回 (filled, fill_date|None, bars_open)。
    bars_open: 已存续的交易日数(回补则到回补日, 未回补则到最新一根)。
    """
    fut = d.iloc[pos + 1:]
    if len(fut) == 0:
        return False, None, 0
    hit = fut.index[fut['low'] <= floor]
    if len(hit) == 0:
        return False, None, len(fut)
    fill_date = hit[0]
    bars_open = d.index.get_loc(fill_date) - pos
    return True, fill_date, bars_open


def upgap_ledger(d: pd.DataFrame) -> pd.DataFrame:
    """区间内每个向上真缺口一行, 带回补状态。d 已 annotate_gaps 过。"""
    rows = []
    arr = d.reset_index()
    date_col = arr.columns[0]
    for pos in np.where(d['up_gap'].values & (d['gap_pct'].values >= MIN_GAP_PCT))[0]:
        floor = d['prev_high'].iloc[pos]      # 缺口下沿
        ceil  = d['low'].iloc[pos]            # 缺口上沿(当日最低, 真空区上界)
        filled, fill_date, bars_open = upgap_fill(d, int(pos), floor)
        rows.append({
            'date':      arr[date_col].iloc[pos].date(),
            'gap_pct':   round(d['gap_pct'].iloc[pos], 2),
            'gap_lo':    round(floor, 2),               # 缺口下沿(止损参考)
            'gap_hi':    round(ceil, 2),                # 缺口上沿
            'vol_x':     round(d['vol_mult'].iloc[pos], 2) if pd.notna(d['vol_mult'].iloc[pos]) else np.nan,
            'cpos':      round(d['close_pos'].iloc[pos], 2),
            'close':     round(d['close'].iloc[pos], 2),
            'status':    'FILLED' if filled else 'OPEN',
            'fill_date': fill_date.date() if fill_date is not None else '',
            'bars':      bars_open,                      # 回补用时 / 已存续天数
        })
    return pd.DataFrame(rows)


def _quality(gap_pct, vol_x, cpos) -> float:
    """透明的可操作性打分 0~1: 跳空幅度0.4 + 收盘强度0.3 + 放量0.3。原始列照样打印。"""
    g = min(max(gap_pct, 0), 12) / 12.0
    v = min(max((vol_x - 1.0) / 2.0, 0), 1) if pd.notna(vol_x) else 0
    c = min(max(cpos, 0), 1)
    return round(0.4 * g + 0.3 * c + 0.3 * v, 3)


# ── 模式一: 单票缺口台账 ──────────────────────────────────────────────────────
def run_ticker(ticker: str, date_from: str | None, date_to: str | None,
               asof: pd.Timestamp | None = None):
    df = _fetch_daily(ticker)
    if df.empty:
        logging.error(f'{ticker}: 无数据')
        return
    full = annotate_gaps(df)
    if asof is not None:                       # point-in-time: 回补追踪不许偷看未来
        full = full[full.index <= asof]

    view = full
    if date_from:
        view = view[view.index >= pd.Timestamp(date_from)]
    if date_to:
        view = view[view.index <= pd.Timestamp(date_to)]
    # 回补要往未来看, 所以台账在 full 上算, 再按区间筛缺口当日
    led = upgap_ledger(full)
    if not led.empty:
        lo = pd.Timestamp(date_from).date() if date_from else led['date'].min()
        hi = pd.Timestamp(date_to).date() if date_to else led['date'].max()
        led = led[(led['date'] >= lo) & (led['date'] <= hi)].reset_index(drop=True)

    rng_txt = f"{date_from or full.index[0].date()} ~ {date_to or full.index[-1].date()}"
    n_bars = len(view)
    n_dn = int(view['dn_gap'].sum())
    print(f"\n{ticker}  {rng_txt}  交易日 {n_bars}, 向下真缺口 {n_dn} 次")
    print("向上真缺口(今低>昨高)台账:")
    if led.empty:
        print("  (无)")
    else:
        print(tab_mod.tabulate(led, headers='keys', tablefmt='github', showindex=False))
        n_open = int((led['status'] == 'OPEN').sum())
        n_fill = int((led['status'] == 'FILLED').sum())
        filled = led[led['status'] == 'FILLED']
        avg_fill = round(filled['bars'].mean(), 1) if len(filled) else float('nan')
        print(f"\n汇总: 向上真缺口 {len(led)} 次 | 仍未回补 {n_open} | 已回补 {n_fill}"
              f" | 平均回补用时 {avg_fill} 个交易日")
        print("注: gap_lo = 缺口下沿(昨高), 跌破即回补, 可作未回补缺口的止损线。")


# ── 模式二: 全市场扫近期放量未回补向上缺口 ────────────────────────────────────
def _load_universe(name: str) -> list:
    path = os.path.join(UNIVERSE_DIR, f'{name}.json')
    if not os.path.exists(path):                       # 缓存缺失时现抓 (含当日缓存)
        from t_us_undervalue import load_universe
        return load_universe(name, force=False)
    with open(path) as f:
        return json.load(f)


def _market_state(asof: pd.Timestamp | None) -> str:
    """大盘 regime via t_us_tech_swing.get_market_state() (QQQ+SOXX 20周线):
    STRONG/MIXED/WEAK/ERROR。回测时临时把 tech_swing 锚到 asof, 保证 as-of 一致。"""
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


def run_scan(universe: str, lookback: int, top: int, asof: pd.Timestamp | None = None,
             weak_gate: bool = True):
    tickers = _load_universe(universe)
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    # 大盘 regime 门控: 2022 熊市回测显示 Tier B 在 WEAK 急跌段会亏钱(绝对胜率仅
    # 34%, alpha 倒挂), 故 WEAK 时默认抑制 Tier B。--no-weak-gate 可关(改为标⚠弱市)。
    mstate = _market_state(asof)
    weak = (mstate == 'WEAK')
    logging.info(f'scan universe={universe} ({len(tickers)} 只), lookback={lookback} 交易日'
                 f'{asof_txt}, 市场={mstate}'
                 + (' → Tier B 抑制' if (weak and weak_gate) else ''))
    rows = []
    for i, t in enumerate(tickers):
        try:
            df = _fetch_daily(t)
            if df.empty or len(df) < VOL_WIN + 2:
                continue
            d = annotate_gaps(df)
            if asof is not None:                 # 站在 asof 当天, 丢弃其后所有 bar
                d = d[d.index <= asof]
            if len(d) < VOL_WIN + 2:
                continue
            # 考察窗放宽到能覆盖"存活"的老缺口(Tier B 需要 age ≥ SCAN_SURVIVE)。
            window = max(lookback, SCAN_MAX_AGE)
            recent = d.tail(window)
            cand = recent[(recent['up_gap']) & (recent['gap_pct'] >= MIN_GAP_PCT)]
            if cand.empty:
                continue
            last_close = d['close'].iloc[-1]
            for ts, r in cand.iterrows():
                pos = d.index.get_loc(ts)
                floor = r['prev_high']
                filled, fill_date, bars_open = upgap_fill(d, pos, floor)
                if filled:                                  # 已回补 → setup 失效, 跳过
                    continue
                age = len(d) - 1 - pos                       # 距今交易日数(=存活天数)
                vol = r['vol_mult']
                hi_vol  = bool(pd.notna(vol) and vol >= SCAN_MIN_VOL)
                hi_cpos = bool(r['close_pos'] >= SCAN_MIN_CPOS)
                fresh   = age < lookback                    # Tier A 新鲜窗
                survived = SCAN_SURVIVE <= age <= SCAN_MAX_AGE   # Tier B 存活窗
                tier_a = fresh and hi_vol and hi_cpos       # 现行逻辑(原样保留)
                tier_b = survived
                if not (tier_a or tier_b):
                    continue
                tier = 'A' if tier_a else 'B'
                # 弱市门控: WEAK 时 Tier B 默认抑制(2022 实证亏钱); 关闸则保留并标⚠弱市。
                if tier == 'B' and weak:
                    if weak_gate:
                        continue
                flags = []                                  # 命中原因标注
                if hi_vol:   flags.append(f'放量{vol:.2f}×')
                if hi_cpos:  flags.append(f'收强{r["close_pos"]:.2f}')
                if survived: flags.append(f'存活{age}d')
                if tier == 'B' and weak and not weak_gate:
                    flags.append('⚠弱市')
                stop_dist = (last_close - floor) / last_close * 100.0  # 到缺口下沿的缓冲%
                rows.append({
                    'ticker':   t,
                    'tier':     tier,
                    'gap_date': ts.date(),
                    'age':      age,
                    'gap_pct':  round(r['gap_pct'], 2),
                    'vol_x':    round(vol, 2) if pd.notna(vol) else np.nan,
                    'cpos':     round(r['close_pos'], 2),
                    'gap_lo':   round(floor, 2),
                    'cur':      round(last_close, 2),
                    'stop%':    round(stop_dist, 1),         # 现价距缺口下沿(止损)缓冲
                    'score':    _quality(r['gap_pct'], vol, r['close_pos']),
                    'reason':   '·'.join(flags) if flags else '—',
                })
        except Exception as e:
            logging.warning(f'{t}: {e}')
        if (i + 1) % 100 == 0:
            logging.info(f'  ...{i + 1}/{len(tickers)}')

    if not rows:
        print('无命中: 近期无可入场的向上缺口。')
        return
    out = pd.DataFrame(rows)
    out['_tr'] = out['tier'].map({'A': 0, 'B': 1})          # A 在前(更新鲜·止损更紧)
    out = out.sort_values(['_tr', 'score', 'age'], ascending=[True, False, True]) \
             .drop(columns='_tr').reset_index(drop=True)
    show = out.head(top)
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    n_a = int((out['tier'] == 'A').sum()); n_b = int((out['tier'] == 'B').sum())
    gate_txt = ' · Tier B 已抑制(弱市)' if (weak and weak_gate) else (' · ⚠弱市' if weak else '')
    print(f"\n向上缺口入场标的  (universe={universe}{asof_txt}, 市场={mstate}{gate_txt}, "
          f"命中 {len(out)}: A {n_a} · B {n_b})")
    print(tab_mod.tabulate(show, headers='keys', tablefmt='github', showindex=False))
    print(f"\nTier A=放量({SCAN_MIN_VOL}×)+收强({SCAN_MIN_CPOS})且 {lookback} 日内新生 (缺口日进, 止损紧);"
          f" Tier B=存活≥{SCAN_SURVIVE}日未回补 (市场确认, 入场偏晚·止损较宽, 即便未过放量/收强)。")
    print("列义: age=距今交易日(=存活天数) · vol_x=对20日均量 · cpos=收盘在当日区间位置 ·"
          " gap_lo=缺口下沿(天然止损) · stop%=现价距下沿缓冲 · reason=命中原因")

    tag = asof.strftime('%Y%m%d') if asof is not None else datetime.date.today().strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_gap_scan')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_gap_scan_{universe}_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


# ── 模式三: 全市场缺口活跃度排行 ──────────────────────────────────────────────
def run_activity(universe: str, days: int, top: int, asof: pd.Timestamp | None = None):
    """
    近 days 个【日历日】内每只票的有效缺口次数, 按活跃度排序。
    有效缺口 = 真缺口(向上 今低>昨高 / 向下 今高<昨低)且 |gap%| ≥ MIN_GAP_PCT。
    用来回答"哪些股票最活跃/最常跳空"。
    """
    tickers = _load_universe(universe)
    anchor = asof.normalize() if asof is not None else pd.Timestamp.today().normalize()
    cutoff = anchor - pd.Timedelta(days=days)
    asof_txt = f', as-of {anchor.date()}' if asof is not None else ''
    logging.info(f'activity universe={universe} ({len(tickers)} 只), 近 {days} 日'
                 f' ({cutoff.date()} ~ {anchor.date()}){asof_txt}')
    rows = []
    for i, t in enumerate(tickers):
        try:
            df = _fetch_daily(t)
            if df.empty or len(df) < VOL_WIN + 2:
                continue
            d = annotate_gaps(df)
            if asof is not None:                 # 站在 anchor 当天, 丢弃其后所有 bar
                d = d[d.index <= anchor]
            w = d[(d.index >= cutoff) & (d.index <= anchor)]
            if w.empty:
                continue
            big = w['gap_pct'].abs() >= MIN_GAP_PCT
            up = w[w['up_gap'] & big]
            dn = w[w['dn_gap'] & big]
            n_up, n_dn = len(up), len(dn)
            if n_up + n_dn == 0:
                continue
            # 未回补的向上缺口(到最新一根仍未跌破下沿)
            n_open = 0
            for ts in up.index:
                pos = d.index.get_loc(ts)
                filled, _, _ = upgap_fill(d, pos, d['prev_high'].iloc[pos])
                if not filled:
                    n_open += 1
            allgap = pd.concat([up, dn])
            rows.append({
                'ticker':   t,
                'gaps':     n_up + n_dn,        # 有效缺口总次数
                'up':       n_up,
                'dn':       n_dn,
                'up_open':  n_open,             # 向上缺口里仍未回补的
                'avg_gap%': round(allgap['gap_pct'].abs().mean(), 2),
                'max_gap%': round(allgap['gap_pct'].abs().max(), 2),
                'last':     allgap.index.max().date(),
            })
        except Exception as e:
            logging.warning(f'{t}: {e}')
        if (i + 1) % 100 == 0:
            logging.info(f'  ...{i + 1}/{len(tickers)}')

    if not rows:
        print('无命中。')
        return
    out = pd.DataFrame(rows).sort_values(['gaps', 'avg_gap%'], ascending=[False, False]).reset_index(drop=True)
    asof_txt = f', as-of {anchor.date()}' if asof is not None else ''
    print(f"\n缺口活跃度排行  (universe={universe}, 近 {days} 日{asof_txt}, {len(out)} 只有缺口)")
    print(tab_mod.tabulate(out.head(top), headers='keys', tablefmt='github', showindex=False))
    print("\n列义: gaps=有效缺口总数 · up/dn=向上/向下 · up_open=向上缺口里仍未回补 ·"
          " avg/max_gap%=跳空幅度 · last=最近一次缺口日")

    tag = anchor.strftime('%Y%m%d') if asof is not None else datetime.date.today().strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_gap_scan')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_gap_activity_{universe}_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


#### MAIN ####
def main():
    parser = OptionParser()
    parser.add_option('--ticker', dest='ticker', help='单票缺口台账, 如 MU')
    parser.add_option('--from', dest='date_from', help='起始日 YYYY-MM-DD(配 --ticker)')
    parser.add_option('--to', dest='date_to', help='结束日 YYYY-MM-DD(配 --ticker)')
    parser.add_option('--scan', action='store_true', default=False, dest='scan',
                      help='全市场扫近期放量未回补向上缺口')
    parser.add_option('--activity', action='store_true', default=False, dest='activity',
                      help='全市场缺口活跃度排行(近 --days 日有效缺口次数)')
    parser.add_option('--days', dest='days', type='int', default=30,
                      help='活跃度统计窗口日历日(配 --activity, 默认 30)')
    parser.add_option('--universe', dest='universe', default='both',
                      help='both|ndx (配 --scan, 默认 both)')
    parser.add_option('--lookback', dest='lookback', type='int', default=SCAN_LOOKBACK,
                      help=f'近 N 交易日(配 --scan, 默认 {SCAN_LOOKBACK})')
    parser.add_option('--top', dest='top', type='int', default=40,
                      help='显示前 N(配 --scan, 默认 40)')
    parser.add_option('--asof', dest='asof', default=None,
                      help='回测某历史日 YYYY-MM-DD: 只用到那天为止的数据, 把"今天"锚到该日'
                           '(三模式通用; 回补追踪不偷看未来)')
    parser.add_option('--no-weak-gate', action='store_true', default=False, dest='no_weak_gate',
                      help='关闭弱市门控(配 --scan): 默认 WEAK 时抑制 Tier B, 加此项改为保留并标⚠弱市')
    (opt, _) = parser.parse_args()

    asof = None
    if opt.asof:
        try:
            asof = pd.Timestamp(opt.asof).normalize()
        except ValueError:
            print(f'--asof 日期无法解析: {opt.asof}'); sys.exit(1)

    if opt.ticker:
        run_ticker(opt.ticker.upper(), opt.date_from, opt.date_to, asof)
    elif opt.scan:
        if opt.universe not in ('both', 'ndx', 'sp500', 'all', 'r2000ht'):
            print('--universe 仅支持 both|ndx|sp500|all|r2000ht'); sys.exit(1)
        run_scan(opt.universe, opt.lookback, opt.top, asof, weak_gate=not opt.no_weak_gate)
    elif opt.activity:
        if opt.universe not in ('both', 'ndx', 'sp500', 'all', 'r2000ht'):
            print('--universe 仅支持 both|ndx|sp500|all|r2000ht'); sys.exit(1)
        run_activity(opt.universe, opt.days, opt.top, asof)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
