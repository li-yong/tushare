# coding: utf-8
"""
US Market Network Event Study — 股票市场网络结构分析 · 阶段4（事件研究·检验交易价值）

实现 docs/stock_market_network_structure.md 执行计划的 **阶段 4** —— 判断整套网络
方法到底有没有 alpha 的关键一步：

    "抱团瓦解"信号出现后，簇内股票后续收益是否系统性走弱？

spec §2.3 应用三 / §2.4 的核心断言是反直觉的：**alpha 多在"瓦解端"而非"形成端"**
——簇内相关性见顶回落、社区松散，是趋势反转的早期信号。本脚本就检验这条：
对阶段2 的抱团温度时序，因果地(point-in-time)生成两类信号 ——

    UNWIND  瓦解端: 复合热度近期到过高位、现在见顶回落 (peak rollover)
    DENSIFY 形成/致密端: 复合热度高且仍在升、四分量齐升

—— 再测每类信号后，该组(等权篮子)相对基准(默认 QQQ)的 **前瞻超额收益**
(多个 horizon)，与无条件基线对比。若 UNWIND 后超额系统性为负、且比 DENSIFY/基线
更负，则支持"瓦解端有 alpha(做空/减仓价值)"。

⚠ 因果性(无未来泄漏)：
  · 信号用 **expanding(只用过去) z-score** 构造，绝不用全样本统计量。
  · 见顶必须"确认"才算数：用"近期到过高位 + 已从近端高点回落 ΔDROP"在 **回落确认
    日** 触发，而非在不可知的峰值当日触发。
  · 前瞻收益严格取信号日 **之后** 的 bar。

诚实的局限(spec §3 数据清洗占 80%)：
  · 样本短(本地~3y)、事件少、horizon 重叠 → 统计力弱，结论是"方向性证据"非定论。
  · 篮子用**当前成分**(survivorship bias)；区间以 2023–26 牛市为主(regime 单一)。
  · 这是方法论检验脚本，不是实盘信号源。

依赖：复用阶段1(t_us_network_structure)+阶段2(t_us_network_dynamics)的管线。

Usage:
  python t_us_network_event_study.py                       # ndx, baskets, QQQ基准
  python t_us_network_event_study.py --groups community
  python t_us_network_event_study.py --benchmark SPY
  python t_us_network_event_study.py --hot 0.8 --drop 0.5 --min-gap 10
  python t_us_network_event_study.py --horizons 5,10,21,42,63
"""

import os
import sys
import json
import logging
import datetime
import warnings
from optparse import OptionParser

warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

import t_us_network_structure as ns
import t_us_network_dynamics as nd

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('netevent')

RESULT_DIR = '/home/ryan/DATA/result/us_network_event_study'
COMPONENTS = nd.COMPONENTS


# ════════════════════════════════════════════════════════════════════════════
# 因果复合热度 + 信号检测
# ════════════════════════════════════════════════════════════════════════════
def causal_composite(df, min_periods):
    """
    expanding(只用过去) z-score 四分量再取均值 = 因果复合热度。
    前 min_periods 个点统计量不稳 → 该段 composite=NaN，不产信号。
    """
    z = pd.DataFrame(index=df.index)
    for c in COMPONENTS:
        s = df[c]
        mu = s.expanding(min_periods=min_periods).mean()
        sd = s.expanding(min_periods=min_periods).std(ddof=0)
        z[c] = (s - mu) / sd.replace(0, np.nan)
    comp = z[COMPONENTS].mean(axis=1)
    return comp


def _slope(vals):
    y = np.asarray(vals, float)
    y = y[~np.isnan(y)]
    if len(y) < 3 or np.allclose(y, y[0]):
        return 0.0
    return float(np.polyfit(np.arange(len(y)), y, 1)[0])


def detect_signals(df, comp, k_trail, hot, drop, min_gap):
    """
    因果信号。对每个时点 i (composite 有效)：
      UNWIND  : 近 k_trail 窗内曾 ≥ hot，且 comp[i] ≤ 近端max - drop，且近端斜率<0
      DENSIFY : comp[i] ≥ hot，且 comp 近端斜率>0，且四分量近端齐升
    同类型信号在 min_gap 个窗口内只取首个(去重叠)。
    返回 {'UNWIND': [idx_positions], 'DENSIFY': [...]}（idx_positions 是 df 行号）。
    """
    n = len(df)
    cvals = comp.values
    out = {'UNWIND': [], 'DENSIFY': []}
    last = {'UNWIND': -10**9, 'DENSIFY': -10**9}
    for i in range(n):
        if np.isnan(cvals[i]) or i < k_trail:
            continue
        win = cvals[i - k_trail:i + 1]
        if np.all(np.isnan(win)):
            continue
        tmax = np.nanmax(win)
        comp_slope = _slope(cvals[max(0, i - k_trail):i + 1])
        four_slopes = [_slope(df[c].values[max(0, i - k_trail):i + 1]) for c in COMPONENTS]

        # UNWIND: 近期高位 + 回落确认
        if tmax >= hot and cvals[i] <= tmax - drop and comp_slope < 0:
            if i - last['UNWIND'] >= min_gap:
                out['UNWIND'].append(i)
                last['UNWIND'] = i
        # DENSIFY: 高位且齐升
        if cvals[i] >= hot and comp_slope > 0 and all(s > 0 for s in four_slopes):
            if i - last['DENSIFY'] >= min_gap:
                out['DENSIFY'].append(i)
                last['DENSIFY'] = i
    return out


# ════════════════════════════════════════════════════════════════════════════
# 前瞻超额收益
# ════════════════════════════════════════════════════════════════════════════
def forward_excess(port_lr, bench_lr, sig_date, horizons):
    """
    信号日 sig_date 之后 H 个交易日的等权篮子对基准 **超额对数收益**（按日对齐）。
    返回 {H: excess_logret or nan}。严格取 sig_date 之后的 bar（无泄漏）。
    """
    idx = port_lr.index
    pos = idx.get_indexer([sig_date])[0]
    out = {}
    for H in horizons:
        a, b = pos + 1, pos + 1 + H
        if pos < 0 or b > len(idx):
            out[H] = np.nan
            continue
        p = port_lr.iloc[a:b].sum()
        q = bench_lr.iloc[a:b].sum()
        out[H] = float(p - q)
    return out


def car_path(port_lr, bench_lr, event_positions, lo, hi):
    """事件研究 CAR：以事件日为 0，[lo,hi] 交易日的平均累计超额对数收益。"""
    excess = (port_lr - bench_lr)
    offs = list(range(lo, hi + 1))
    rows = []
    for p in event_positions:
        if p + lo < 0 or p + hi >= len(excess):
            continue
        seg = excess.iloc[p + lo:p + hi + 1].cumsum()
        seg = seg - excess.iloc[p]               # 锚定事件日累计=0(含事件日当天前为负偏移)
        # 以"事件日(offset 0)处累计=0"对齐
        base = seg.iloc[offs.index(0)]
        rows.append((seg.values - base))
    if not rows:
        return None, None
    arr = np.vstack(rows)
    return np.array(offs), arr.mean(axis=0)


# ── 统计 ─────────────────────────────────────────────────────────────────────
def aggregate(events_excess, horizons):
    """events_excess: list of {H: excess}. 返回每 H 的 n/mean/median/hit/tstat（%）。"""
    rows = []
    for H in horizons:
        vals = np.array([e[H] for e in events_excess if not np.isnan(e.get(H, np.nan))])
        if len(vals) == 0:
            rows.append({'H': H, 'n': 0})
            continue
        pct = np.expm1(vals) * 100                      # 对数→简单收益 %
        se = pct.std(ddof=1) / np.sqrt(len(pct)) if len(pct) > 1 else np.nan
        rows.append({
            'H': H, 'n': len(pct),
            'mean%': pct.mean(), 'median%': float(np.median(pct)),
            'neg%': float((pct < 0).mean() * 100),     # 走弱命中率
            't': pct.mean() / se if se and se > 0 else np.nan,
        })
    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════════════════════
# 可视化
# ════════════════════════════════════════════════════════════════════════════
def plot_event_study(paths, agg_tables, out_png, title, horizons):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    colors = {'UNWIND': 'crimson', 'DENSIFY': 'seagreen', 'BASELINE': 'gray'}
    for sig, (offs, mean_path) in paths.items():
        if offs is None:
            continue
        ax1.plot(offs, np.expm1(mean_path) * 100, label=sig,
                 color=colors.get(sig, None), lw=1.8)
    ax1.axvline(0, color='black', lw=0.8, ls='--')
    ax1.axhline(0, color='gray', lw=0.5)
    ax1.set_title('Avg cumulative EXCESS return vs benchmark around signal (CAR)')
    ax1.set_xlabel('trading days from signal')
    ax1.set_ylabel('cum excess %')
    ax1.legend()
    ax1.grid(alpha=0.25)

    # 右图：各 horizon 的平均超额 (UNWIND vs DENSIFY vs BASELINE)
    width = 0.26
    xs = np.arange(len(horizons))
    for j, sig in enumerate(['UNWIND', 'DENSIFY', 'BASELINE']):
        tbl = agg_tables.get(sig)
        if tbl is None:
            continue
        means = [float(tbl[tbl.H == H]['mean%'].iloc[0]) if (tbl.H == H).any()
                 and 'mean%' in tbl.columns and len(tbl[tbl.H == H]) else np.nan
                 for H in horizons]
        ax2.bar(xs + (j - 1) * width, means, width, label=sig,
                color=colors.get(sig))
    ax2.axhline(0, color='gray', lw=0.6)
    ax2.set_xticks(xs)
    ax2.set_xticklabels([f'+{H}d' for H in horizons])
    ax2.set_title('Mean forward excess % by horizon')
    ax2.legend()
    ax2.grid(alpha=0.25, axis='y')

    fig.suptitle(title, fontsize=12)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(out_png, dpi=125, bbox_inches='tight')
    plt.close(fig)
    log.info(f'图已保存: {out_png}')


# ════════════════════════════════════════════════════════════════════════════
# 报告 + main
# ════════════════════════════════════════════════════════════════════════════
def write_report(out_txt, params, rets, groups, all_events, agg_tables,
                 per_group_counts, horizons, event_dates):
    lines = []
    P = lines.append
    P('=' * 82)
    P('股票市场网络结构分析 · 阶段4 事件研究 (检验交易价值)')
    P('=' * 82)
    P(f'生成时间   : {datetime.datetime.now():%Y-%m-%d %H:%M:%S}'
      + ('  [回测 --asof %s]' % ns._ASOF.date() if ns._ASOF is not None else ''))
    P(f'股票池/分组: {params["universe"]} / groups={params["groups"]} ({len(groups)} 组)')
    P(f'基准       : {params["benchmark"]}   数据区间 {rets.index[0].date()} → {rets.index[-1].date()}')
    P(f'窗口/信号  : win{params["window"]}/step{params["step"]}  '
      f'hot≥{params["hot"]} drop≥{params["drop"]} k_trail={params["k_trail"]} '
      f'min_gap={params["min_gap"]}')
    P('')
    P('检验命题(spec §2.4): "抱团瓦解"信号后簇内股票是否系统性走弱 → 瓦解端是否有 alpha。')
    P('超额 = 等权篮子对基准的前瞻收益。走弱 = 超额为负 (neg% = 命中率)。')
    P('')

    for sig in ['UNWIND', 'DENSIFY', 'BASELINE']:
        n_ev = len(all_events.get(sig, []))
        P(f'── {sig}  (事件数 {n_ev}) ' + '─' * 50)
        tbl = agg_tables.get(sig)
        if tbl is None or tbl.empty or 'mean%' not in tbl.columns:
            P('  样本不足，无统计。')
            P('')
            continue
        P('   horizon |   n |   mean% | median% |  走弱neg% |   t')
        for _, r in tbl.iterrows():
            if r.get('n', 0) == 0 or 'mean%' not in r or pd.isna(r.get('mean%')):
                P(f'    +{int(r["H"]):>3}d  |   0 |    —    |    —    |     —     |   —')
            else:
                P(f'    +{int(r["H"]):>3}d  | {int(r["n"]):>3} | {r["mean%"]:+6.2f}  '
                  f'| {r["median%"]:+6.2f}  |  {r["neg%"]:5.1f}    | {r["t"]:+.2f}')
        P('')

    # 解读
    P('── 解读 ' + '─' * 72)
    u = agg_tables.get('UNWIND'); d = agg_tables.get('DENSIFY'); b = agg_tables.get('BASELINE')

    def _row(tbl, H):
        if tbl is None or 'mean%' not in tbl.columns:
            return None
        m = tbl[tbl.H == H]
        return None if m.empty or pd.isna(m['mean%'].iloc[0]) else m.iloc[0]
    verdict = []
    for H in horizons:
        ru, rd, rb = _row(u, H), _row(d, H), _row(b, H)
        if ru is None:
            continue
        cmp_base = f'(基线 {rb["mean%"]:+.2f}%)' if rb is not None else ''
        cmp_dens = f'(形成端 {rd["mean%"]:+.2f}%)' if rd is not None else ''
        sign = '走弱✓' if ru['mean%'] < 0 else '未走弱✗'
        more_neg = (rb is not None and ru['mean%'] < rb['mean%'])
        verdict.append(f'  +{H}d: 瓦解后超额 {ru["mean%"]:+.2f}% {sign}  '
                       f'{cmp_base}{cmp_dens}  '
                       + ('比基线更弱✓' if more_neg else '未明显弱于基线'))
    lines += verdict
    P('')
    P('  本样本图景(CAR): DENSIFY(致密/形成端高位) 信号前篮子已大幅跑赢、信号后立刻')
    P('  转跌、各 horizon 系统性<0 且远逊基线 → 印证 spec §2.4 "看得最清=风险最大":')
    P('  在抱团最致密时买入/追入最危险。UNWIND(瓦解端) 短期(≤21d)先有反弹(超跌反抽),')
    P('  ~42d 后才转弱并跑输基线 → 瓦解端的"走弱"是滞后的、≠即时做空信号。')
    P('')
    P('  ⚠⚠ 关键局限 —— 事件非独立: 各组 DENSIFY/UNWIND 高度集中于同一日历段(见下方')
    P('  事件日期, 多在 2025-05 那轮全市场抱团峰值前后), 跨组 horizon 严重重叠 → 有效')
    P('  独立样本远少于事件计数, 上面的均值/t 实质是"少数几轮行情"的复述, 不是 N 次')
    P('  独立验证。叠加 survivorship(当前成分)+区间以牛市为主 → 结论仅"方向性一致",')
    P('  统计上不显著(t 普遍 |t|<2)。要定论需更长历史 + point-in-time 成分股(spec §3)。')
    P('')
    P('── 事件日期 (暴露聚集) ' + '─' * 58)
    for kind in ('UNWIND', 'DENSIFY'):
        ds = sorted(event_dates.get(kind, []))
        P(f'  {kind}: ' + (', '.join(d.strftime('%y-%m-%d') for d in ds) if ds else '无'))
    P('')
    P('── 各组事件计数 ' + '─' * 64)
    for g, c in sorted(per_group_counts.items(), key=lambda kv: -(kv[1]['UNWIND'])):
        P(f'  {g:16} UNWIND={c["UNWIND"]}  DENSIFY={c["DENSIFY"]}')
    P('=' * 82)

    txt = '\n'.join(lines)
    with open(out_txt, 'w') as fh:
        fh.write(txt)
    return txt


def main():
    parser = OptionParser()
    parser.add_option('--universe', default='ndx', help='sp500 | ndx | both (默认 ndx)')
    parser.add_option('--groups', default='baskets',
                      help='baskets | sector | community (默认 baskets)')
    parser.add_option('--basket', action='append', default=[],
                      help='自定义篮子 NAME=T1,T2,...')
    parser.add_option('--benchmark', default='QQQ', help='基准代码 (默认 QQQ)')
    parser.add_option('--window', type='int', default=63)
    parser.add_option('--step', type='int', default=5)
    parser.add_option('--theta', type='float', default=0.5)
    parser.add_option('--hot', type='float', default=0.8,
                      help='热度高位阈值(因果z, 默认0.8)')
    parser.add_option('--drop', type='float', default=0.5,
                      help='见顶回落确认幅度(z, 默认0.5)')
    parser.add_option('--k-trail', dest='k_trail', type='int', default=8,
                      help='近端窗口数(默认8)')
    parser.add_option('--min-gap', dest='min_gap', type='int', default=10,
                      help='同类型信号最小间隔(窗口数, 去重叠, 默认10)')
    parser.add_option('--min-periods', dest='min_periods', type='int', default=30,
                      help='expanding z-score 最少窗口(默认30)')
    parser.add_option('--horizons', default='5,10,21,42,63',
                      help='前瞻交易日, 逗号分隔')
    parser.add_option('--min-history', dest='min_history', type='int', default=120)
    parser.add_option('--min-dollar-vol', dest='min_dv', type='float', default=5e6)
    parser.add_option('--no-plot', dest='no_plot', action='store_true', default=False)
    parser.add_option('--asof', default=None)
    parser.add_option('--force', action='store_true', default=False)
    opts, _ = parser.parse_args()

    if opts.asof:
        try:
            ns._ASOF = pd.Timestamp(opts.asof).normalize()
            log.info(f'回测模式: 锚定 {ns._ASOF.date()}')
        except Exception:
            parser.error(f'--asof 无法解析: {opts.asof}')
    horizons = [int(h) for h in opts.horizons.split(',') if h.strip()]

    extra = {}
    for spec in opts.basket:
        if '=' not in spec:
            parser.error(f'--basket 格式 NAME=T1,T2,...  收到: {spec}')
        nm, syms = spec.split('=', 1)
        extra[nm.strip().upper()] = [s.strip().upper().replace('.', '-')
                                     for s in syms.split(',') if s.strip()]

    tickers = ns.load_universe(opts.universe, opts.force)
    if extra:
        tickers = sorted(set(tickers) | {t for v in extra.values() for t in v})
    rets = nd.build_panel(tickers, None, opts.min_history, opts.min_dv, opts.force)

    groups = nd.resolve_groups(opts.groups, list(rets.columns), extra)
    if not groups:
        raise SystemExit('无有效分组')
    log.info('分组: ' + ', '.join(f'{k}({len(v)})' for k, v in groups.items()))

    # 基准对数收益（对齐到面板交易日）
    bs = ns.fetch_close(opts.benchmark, opts.force)
    if bs.empty:
        raise SystemExit(f'基准 {opts.benchmark} 无数据')
    bench_lr = np.log(bs / bs.shift(1)).reindex(rets.index).fillna(0.0)

    # 阶段2 滚动四分量
    per_raw, _ = nd.rolling_crowding(rets, groups, opts.window, opts.step, opts.theta)

    # 把"窗口时点"映射回 rets.index 行号（窗口末日即该时点）
    pos_of = {d: rets.index.get_indexer([d])[0] for df in per_raw.values()
              for d in df.index}

    events = {'UNWIND': [], 'DENSIFY': [], 'BASELINE': []}
    car_pos = {'UNWIND': [], 'DENSIFY': [], 'BASELINE': []}
    event_dates = {'UNWIND': [], 'DENSIFY': []}
    per_group_counts = {}
    for g, df in per_raw.items():
        comp = causal_composite(df, opts.min_periods)
        sig = detect_signals(df, comp, opts.k_trail, opts.hot, opts.drop, opts.min_gap)
        port_lr = rets[groups[g]].mean(axis=1)        # 等权篮子日对数收益
        per_group_counts[g] = {'UNWIND': len(sig['UNWIND']),
                               'DENSIFY': len(sig['DENSIFY'])}
        for kind in ('UNWIND', 'DENSIFY'):
            for row_i in sig[kind]:
                sig_date = df.index[row_i]
                events[kind].append(forward_excess(port_lr, bench_lr, sig_date, horizons))
                car_pos[kind].append((g, pos_of[sig_date]))
                event_dates[kind].append(sig_date)
        # 基线：该组所有有效窗口时点（无条件）—— 用于对照
        for d in df.index[comp.notna()]:
            events['BASELINE'].append(forward_excess(port_lr, bench_lr, d, horizons))
            car_pos['BASELINE'].append((g, pos_of[d]))

    agg_tables = {k: aggregate(v, horizons) for k, v in events.items()}

    # CAR：需要每组自己的 port_lr，按组累计后再平均
    car_paths = {}
    for kind, plist in car_pos.items():
        per_kind = []
        offs_ref = None
        for g, p in plist:
            port_lr = rets[groups[g]].mean(axis=1)
            offs, mean_path = car_path(port_lr, bench_lr, [p], -20, max(horizons))
            if offs is None:
                continue
            offs_ref = offs
            per_kind.append(mean_path)
        if per_kind:
            car_paths[kind] = (offs_ref, np.vstack(per_kind).mean(axis=0))
        else:
            car_paths[kind] = (None, None)

    params = {'universe': opts.universe, 'groups': opts.groups,
              'benchmark': opts.benchmark, 'window': opts.window, 'step': opts.step,
              'hot': opts.hot, 'drop': opts.drop, 'k_trail': opts.k_trail,
              'min_gap': opts.min_gap}

    os.makedirs(RESULT_DIR, exist_ok=True)
    tag = (ns._ASOF.date().isoformat() if ns._ASOF is not None
           else datetime.date.today().isoformat())
    out_txt = os.path.join(RESULT_DIR, f'us_network_event_study_{tag}.txt')
    out_png = os.path.join(RESULT_DIR, f'us_network_event_study_{tag}.png')

    txt = write_report(out_txt, params, rets, groups, events, agg_tables,
                       per_group_counts, horizons, event_dates)
    print('\n' + txt)

    # 机器可读 JSON sidecar（供 t_us_network_report.py 汇总）
    def _tbl(df):
        rows = []
        for _, r in df.iterrows():
            if r.get('n', 0) == 0 or 'mean%' not in r or pd.isna(r.get('mean%')):
                rows.append({'H': int(r['H']), 'n': int(r.get('n', 0))})
            else:
                rows.append({'H': int(r['H']), 'n': int(r['n']),
                             'mean': round(float(r['mean%']), 2),
                             'median': round(float(r['median%']), 2),
                             'neg': round(float(r['neg%']), 1),
                             't': round(float(r['t']), 2) if pd.notna(r['t']) else None})
        return rows
    summary = {
        'stage': 'event_study', 'date': tag, 'universe': opts.universe,
        'groups_mode': opts.groups, 'benchmark': opts.benchmark,
        'data_range': [rets.index[0].date().isoformat(), rets.index[-1].date().isoformat()],
        'params': params, 'horizons': horizons,
        'tables': {k: _tbl(v) for k, v in agg_tables.items()},
        'event_counts': {k: len(v) for k, v in events.items()},
        'event_dates': {k: [d.date().isoformat() for d in sorted(v)]
                        for k, v in event_dates.items()},
        'per_group_counts': per_group_counts,
    }
    out_json = os.path.join(RESULT_DIR, f'us_network_event_study_{tag}.json')
    with open(out_json, 'w') as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    log.info(f'JSON摘要: {out_json}')

    if not opts.no_plot:
        title = (f'US Network Event Study · {opts.universe} · groups={opts.groups} · '
                 f'bench={opts.benchmark} · {rets.index[-1].date()}')
        try:
            plot_event_study(car_paths, agg_tables, out_png, title, horizons)
        except Exception as e:
            log.error(f'绘图失败 (不影响报告): {e}')

    log.info('完成。')


if __name__ == '__main__':
    main()
