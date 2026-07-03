# coding: utf-8
"""
US Market Network Dynamics — 股票市场网络结构分析 · 阶段2（动态网络 + 抱团温度）

实现 docs/stock_market_network_structure.md 执行计划的 **阶段 2（核心验证）**：
在滚动窗口上构建动态网络，对每个分组(community/sector/主题篮子)计算"抱团温度"
时间序列，识别 抱团生命周期：形成 → 致密 → 见顶 → 瓦解。

抱团温度四分量（spec §2.3 应用一 + 附录公式）——四者**同时单调上升 = 真抱团**：
  1. intra     簇内平均相关性 ρ̄_in           （"温度"本身）
  2. density   簇内致密度 = ρ>θ 的簇内对占比   （致密化）
  3. ratio     簇内/簇间相关比 ρ̄_in / ρ̄_out  （与大盘/他簇**解耦**）
  4. gravity   引力占比 = 簇相关质量 / 全市场相关质量（资金虹吸指纹）

把四分量各自对自身历史 z-score 后取均值 = 复合热度 composite。再据此与近端
斜率/回撤判生命周期阶段。

⚠ 重要立场（spec §2.4 / §3）——本脚本的**交易含义全在相变，不在水平值**：
  · 相关网络是同步/滞后指标，非领先指标。结构看得最清(四分量齐升、复合热度见顶)
    的时刻，往往拥挤已极、最接近反转 —— **看得最清=风险最大**。
  · 反直觉但实证支持：**alpha 多在"瓦解端"**（簇内相关见顶回落、解耦比收敛）
    而非"形成端"。故本脚本重点标注 PEAK-ROLLOVER 与 UNWINDING，而非追涨升温。
  · 仍是环境判断工具，不是买卖信号发生器。

去噪说明：滚动短窗口下 T<N，谱分解/求逆不可靠（spec 维度灾难），**但本脚本的
四分量全部是 pairwise 相关性的均值/计数**，单个 ρ_ij 只需 T 个观测即稳健，不涉
矩阵求逆，故滚动段不做 RMT/收缩。阶段1 的谱方法才需要去噪。

数据/股票池/缓存沿用阶段1（t_us_network_structure 的 loader，yfinance only）。

分组来源（--groups）：
  baskets   (默认) 内置主题篮子 MAG7 / SEMI / SOFTWARE / ... ∩ 股票池
  sector    yfinance GICS sector（需 sectors.json 缓存，见阶段1 --sectors）
  community 复用阶段1 输出的 Louvain 社区（result/us_network_structure 最新 node csv）

历史窗口提醒：本地缓存约 3y（~2023-06 起），覆盖 **2023–24 AI/半导体抱团** 这段
著名行情，正好做"对得上号吗"的校验；够不到 2020–21（茅/宁组合那一代）。

Usage:
  python t_us_network_dynamics.py                         # ndx, baskets, 全历史
  python t_us_network_dynamics.py --universe both         # 全市场作背景
  python t_us_network_dynamics.py --groups sector
  python t_us_network_dynamics.py --groups community
  python t_us_network_dynamics.py --basket AI=NVDA,AVGO,AMD,ARM,ALAB
  python t_us_network_dynamics.py --window 63 --step 5 --theta 0.5
  python t_us_network_dynamics.py --asof 2024-07-01       # 只用 ≤该日 bar 复盘
"""

import os
import sys
import glob
import json
import logging
import datetime
import warnings
from optparse import OptionParser

warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

# 复用阶段1 的数据管线（loader / 复权缓存 / 股票池 / sector）
import t_us_network_structure as ns

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('netdyn')

RESULT_DIR = '/home/ryan/DATA/result/us_network_dynamics'
PHASE1_DIR = '/home/ryan/DATA/result/us_network_structure'

# 内置主题篮子（与股票池取交集后使用）。覆盖 2023–24 抱团主线。
BASKETS = {
    'MAG7':     ['AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'NVDA', 'TSLA'],
    'SEMI':     ['NVDA', 'AVGO', 'AMD', 'MU', 'LRCX', 'AMAT', 'KLAC', 'ASML',
                 'ADI', 'MCHP', 'MPWR', 'TXN', 'INTC', 'QCOM', 'NXPI', 'ON', 'MRVL', 'ARM'],
    'AI_INFRA': ['NVDA', 'AVGO', 'AMD', 'ARM', 'ALAB', 'MU', 'AMAT', 'LRCX', 'KLAC'],
    'SOFTWARE': ['MSFT', 'CRM', 'ADBE', 'NOW', 'ORCL', 'INTU', 'PANW', 'CRWD',
                 'SNPS', 'CDNS', 'ADSK', 'WDAY', 'FTNT', 'DDOG'],
    'INTERNET': ['GOOGL', 'GOOG', 'META', 'AMZN', 'NFLX', 'BKNG', 'ABNB', 'SHOP'],
}


# ════════════════════════════════════════════════════════════════════════════
# 数据：全历史对齐收益率面板（阶段0 管线的"动态版"）
# ════════════════════════════════════════════════════════════════════════════
def build_panel(tickers, start, min_history, min_dv, refetch):
    """返回对齐后的对数收益率矩阵 R（行=交易日, 列=ticker），尽量长的历史。"""
    closes = {}
    n = len(tickers)
    for i, t in enumerate(tickers, 1):
        if i % 100 == 0:
            log.info(f'  载入收盘 {i}/{n} ...')
        s = ns.fetch_close(t, refetch)          # 复用阶段1 缓存/兜底；遵从 ns._ASOF
        if s.empty:
            continue
        if start is not None:
            s = s[s.index >= start]
        if len(s) < min_history:
            continue
        if (s.attrs.get('dollar_vol') or 0) < min_dv:
            continue
        closes[t] = s
    if len(closes) < 5:
        raise SystemExit(f'有效票太少 ({len(closes)})')
    panel = pd.DataFrame(closes).sort_index()
    panel = panel.dropna(axis=1, thresh=int(0.95 * len(panel)))   # 历史覆盖<95%剔除
    panel = panel.ffill().dropna(how='any')
    rets = np.log(panel / panel.shift(1)).iloc[1:].dropna(axis=1)
    log.info(f'收益率面板: T={rets.shape[0]} 日 × N={rets.shape[1]} 票  '
             f'({rets.index[0].date()} → {rets.index[-1].date()})')
    return rets


# ── 分组定义 ─────────────────────────────────────────────────────────────────
def resolve_groups(mode, cols, extra_baskets):
    """返回 {group_name: [tickers ∩ cols]}（≥2 成员才保留）。"""
    colset = set(cols)
    groups = {}
    if mode == 'baskets':
        src = dict(BASKETS)
        src.update(extra_baskets)
        for name, members in src.items():
            keep = [t for t in members if t in colset]
            if len(keep) >= 2:
                groups[name] = keep
    elif mode == 'sector':
        sectors = ns.load_sectors(list(cols), do_fetch=False)
        by = {}
        for t, sec in sectors.items():
            if sec and sec != 'Unknown' and t in colset:
                by.setdefault(sec, []).append(t)
        groups = {k: v for k, v in by.items() if len(v) >= 2}
        if not groups:
            raise SystemExit('无 sector 缓存；先跑 t_us_network_structure.py --sectors')
    elif mode == 'community':
        paths = sorted(glob.glob(os.path.join(PHASE1_DIR, 'us_network_nodes_*.csv')))
        if not paths:
            raise SystemExit('无阶段1 社区输出；先跑 t_us_network_structure.py')
        df = pd.read_csv(paths[-1], index_col=0)
        log.info(f'社区来源: {os.path.basename(paths[-1])}')
        for c, sub in df.groupby('community'):
            keep = [t for t in sub.index if t in colset]
            if len(keep) >= 2:
                # 用度最高成员命名社区，便于辨识
                head = sub.sort_values('degree', ascending=False).index[0]
                groups[f'C{c}~{head}'] = keep
    else:
        raise ValueError(mode)
    return groups


# ════════════════════════════════════════════════════════════════════════════
# 滚动抱团温度（核心）
# ════════════════════════════════════════════════════════════════════════════
def rolling_crowding(rets, groups, window, step, theta):
    """
    对每个 group 计算四分量时间序列 + 全市场 mean_corr（温度计背景）。
    返回 (per_group: {name: DataFrame[intra,density,ratio,gravity,n]}, market: Series)。
    """
    cols = list(rets.columns)
    idx = {t: i for i, t in enumerate(cols)}
    R = rets.values                       # T×N
    T, N = R.shape
    ends = list(range(window, T + 1, step))
    if ends and ends[-1] != T:
        ends.append(T)                    # 总是包含最新窗口
    gmask = {name: np.array([idx[t] for t in mem]) for name, mem in groups.items()}

    per = {name: [] for name in groups}
    dates, market = [], []
    for e in ends:
        win = R[e - window:e]                          # window×N
        z = win - win.mean(0)
        sd = z.std(0, ddof=0)
        sd[sd == 0] = 1e-12
        z = z / sd
        C = (z.T @ z) / window                          # N×N 相关矩阵（pairwise，稳健）
        np.clip(C, -1.0, 1.0, out=C)
        d = rets.index[e - 1]
        dates.append(d)
        iu = np.triu_indices(N, k=1)
        market.append(float(C[iu].mean()))
        absC = np.abs(C)
        strength = absC.sum(1) - 1.0                    # 每节点相关质量（去自身）
        total_mass = strength.sum()
        for name, m in gmask.items():
            sub = C[np.ix_(m, m)]
            k = len(m)
            iu2 = np.triu_indices(k, k=1)
            intra_vals = sub[iu2]
            intra = float(intra_vals.mean())
            density = float((intra_vals > theta).mean())
            # 簇间：成员 × 非成员
            others = np.setdiff1d(np.arange(N), m, assume_unique=False)
            inter = float(C[np.ix_(m, others)].mean()) if len(others) else np.nan
            ratio = intra / inter if inter and inter > 1e-6 else np.nan
            gravity = float(strength[m].sum() / total_mass) if total_mass else np.nan
            per[name].append((intra, density, ratio, gravity, k))

    market = pd.Series(market, index=pd.DatetimeIndex(dates), name='market_mean_corr')
    out = {}
    for name in groups:
        out[name] = pd.DataFrame(
            per[name], index=pd.DatetimeIndex(dates),
            columns=['intra', 'density', 'ratio', 'gravity', 'n'])
    log.info(f'滚动窗口 {window}d / 步长 {step}d → {len(dates)} 个时点')
    return out, market


# ── 复合热度 + 生命周期阶段 ───────────────────────────────────────────────────
COMPONENTS = ['intra', 'density', 'ratio', 'gravity']


def composite_and_regime(df, trail):
    """
    给 group 的四分量时间序列加 composite（四分量各自 z-score 后均值）列，
    并返回当前阶段诊断 dict。
    """
    z = pd.DataFrame(index=df.index)
    for c in COMPONENTS:
        s = df[c]
        mu, sd = s.mean(), s.std(ddof=0)
        z[c] = (s - mu) / sd if sd > 1e-9 else 0.0
    comp = z[COMPONENTS].mean(axis=1)
    df = df.copy()
    df['composite'] = comp

    def slope(s):
        y = s.tail(trail).values
        if len(y) < 3 or np.allclose(y, y[0]):
            return 0.0
        x = np.arange(len(y))
        return float(np.polyfit(x, y, 1)[0])

    comp_slope = slope(comp)
    comp_pct = float((comp <= comp.iloc[-1]).mean())          # 当前热度历史分位
    recent_max = float(comp.tail(max(trail * 3, 12)).max())
    drawdown = float(comp.iloc[-1] - recent_max)              # ≤0
    four_slopes = {c: slope(df[c]) for c in COMPONENTS}
    four_rising = all(v > 0 for v in four_slopes.values())
    four_falling = all(v < 0 for v in four_slopes.values())

    # 生命周期：形成→致密→见顶→瓦解（spec §2.3 应用三）
    if comp_pct >= 0.80 and (four_falling or (comp_slope < 0 and drawdown < -0.15)):
        stage = '见顶/瓦解 PEAK→UNWIND ⚠'
        note = 'alpha端: 簇内相关见顶回落=趋势反转早期信号(spec §2.3/§2.4)'
    elif comp_pct >= 0.75 and comp_slope <= 0.01:
        stage = '见顶 PEAK ⚠'
        note = '看得最清=风险最大: 拥挤已极、最接近反转，勿追'
    elif four_rising and comp_pct >= 0.6:
        stage = '致密化 DENSIFYING'
        note = '四分量齐升+高位: 真抱团特征(资金虹吸进行中)，享受但备好减仓纪律'
    elif comp_slope > 0:
        stage = '升温 BUILDING'
        note = '形成端: 升温中但未极致'
    elif drawdown < -0.20:
        stage = '瓦解 UNWINDING'
        note = '已从高位回落: 抱团松散，趋势反转中后段'
    else:
        stage = '平静/松散 QUIET'
        note = '无抱团信号'

    return df, {
        'stage': stage, 'note': note, 'comp_now': float(comp.iloc[-1]),
        'comp_pct': comp_pct, 'comp_slope': comp_slope, 'drawdown': drawdown,
        'four_rising': four_rising, 'four_slopes': four_slopes,
        'intra_now': float(df['intra'].iloc[-1]),
        'ratio_now': float(df['ratio'].iloc[-1]),
        'gravity_now': float(df['gravity'].iloc[-1]),
        'n': int(df['n'].iloc[-1]),
    }


# ════════════════════════════════════════════════════════════════════════════
# 可视化
# ════════════════════════════════════════════════════════════════════════════
def plot_dynamics(per_comp, market, diag, out_png, title, detail_names):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    n_detail = len(detail_names)
    fig, axes = plt.subplots(n_detail + 1, 1, figsize=(15, 3.2 * (n_detail + 1)),
                             sharex=True)
    if n_detail == 0:
        axes = [axes]

    # 顶panel：各组复合热度 + 全市场温度  (plot text ASCII: matplotlib 无 CJK 字体)
    ax0 = axes[0]
    for name, df in per_comp.items():
        ax0.plot(df.index, df['composite'], label=name, lw=1.3)
    ax0b = ax0.twinx()
    ax0b.plot(market.index, market.values, color='black', lw=1.0, ls='--',
              alpha=0.5, label='market mean_corr (R)')
    ax0.set_title('Composite crowding heat (L, z-score) + market mean_corr (R, dashed)',
                  fontsize=11)
    ax0.axhline(0, color='gray', lw=0.5)
    ax0.legend(fontsize=7, ncol=4, loc='upper left')
    ax0b.legend(fontsize=7, loc='upper right')
    ax0.grid(alpha=0.25)

    # 每个 detail 组：四分量（各自 min-max 归一便于同图看齐升）
    for ax, name in zip(axes[1:], detail_names):
        df = per_comp[name]
        for c in COMPONENTS:
            s = df[c]
            rng = s.max() - s.min()
            ax.plot(df.index, (s - s.min()) / rng if rng > 1e-9 else s * 0,
                    label=c, lw=1.2)
        stage_ascii = diag[name]['stage'].encode('ascii', 'ignore').decode().strip()
        ax.set_title(f'{name} four components (min-max norm)  —  {stage_ascii}',
                     fontsize=10)
        ax.legend(fontsize=7, ncol=4, loc='upper left')
        ax.grid(alpha=0.25)

    fig.suptitle(title, fontsize=13, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.99])
    fig.savefig(out_png, dpi=125, bbox_inches='tight')
    plt.close(fig)
    log.info(f'图已保存: {out_png}')


# ════════════════════════════════════════════════════════════════════════════
# 报告 + main
# ════════════════════════════════════════════════════════════════════════════
def write_report(out_txt, params, rets, per_comp, market, diag):
    lines = []
    P = lines.append
    P('=' * 80)
    P('股票市场网络结构分析 · 阶段2 动态网络 + 抱团温度')
    P('=' * 80)
    P(f'生成时间   : {datetime.datetime.now():%Y-%m-%d %H:%M:%S}'
      + ('  [回测 --asof %s]' % ns._ASOF.date() if ns._ASOF is not None else ''))
    P(f'股票池/分组: {params["universe"]} / groups={params["groups"]} '
      f'({len(per_comp)} 组)')
    P(f'数据区间   : {rets.index[0].date()} → {rets.index[-1].date()}  (N={rets.shape[1]})')
    P(f'滚动窗口   : {params["window"]}d / 步长 {params["step"]}d / θ={params["theta"]}')
    P(f'最新时点   : {market.index[-1].date()}  全市场 mean_corr={market.iloc[-1]:+.3f}')
    P('')
    P('抱团温度=四分量同时上升: intra(簇内相关) density(致密) ratio(解耦比) gravity(引力占比)')
    P('⚠ 交易义在相变不在水平: 四齐升+热度见顶=拥挤极致(风险最大); alpha多在瓦解端。')
    P('')

    # 按当前复合热度分位排序
    order = sorted(diag.items(), key=lambda kv: -kv[1]['comp_pct'])
    P('── 各组当前状态 (按复合热度历史分位降序) ' + '─' * 36)
    for name, d in order:
        fr = '↑↑↑↑' if d['four_rising'] else ''.join(
            '↑' if d['four_slopes'][c] > 0 else '↓' for c in COMPONENTS)
        P(f'  {name:14} {d["stage"]}')
        P(f'      热度分位 {d["comp_pct"]:.0%}  斜率 {d["comp_slope"]:+.3f}  '
          f'回撤 {d["drawdown"]:+.2f}  四分量 {fr}  (n={d["n"]})')
        P(f'      intra={d["intra_now"]:+.2f} ratio={d["ratio_now"]:.2f} '
          f'gravity={d["gravity_now"]:.3f}')
        P(f'      → {d["note"]}')
    P('')

    # 历史峰值校验（"对得上号吗"）：每组复合热度历史最高点的日期
    P('── 历史抱团峰值校验 (各组 composite 历史最高点日期) ' + '─' * 22)
    P('  (本地~3y 数据覆盖 2023–24 AI/半导体抱团; 够不到 2020–21 茅/宁组合)')
    for name, df in sorted(per_comp.items(),
                           key=lambda kv: -kv[1]['composite'].max()):
        peak_date = df['composite'].idxmax()
        P(f'  {name:14} 峰值 {df["composite"].max():+.2f} @ {peak_date.date()}  '
          f'(当前 {df["composite"].iloc[-1]:+.2f})')
    P('')
    P('=' * 80)

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
                      help='自定义篮子 NAME=T1,T2,... 可多次')
    parser.add_option('--start', default=None, help='起始日 YYYY-MM-DD (默认全历史)')
    parser.add_option('--window', type='int', default=63, help='滚动窗口交易日 (默认63≈3mo)')
    parser.add_option('--step', type='int', default=5, help='滚动步长交易日 (默认5)')
    parser.add_option('--theta', type='float', default=0.5, help='致密度阈值 (默认0.5)')
    parser.add_option('--trail', type='int', default=8,
                      help='斜率/阶段判定的近端窗口数 (默认8)')
    parser.add_option('--min-history', dest='min_history', type='int', default=120)
    parser.add_option('--min-dollar-vol', dest='min_dv', type='float', default=5e6)
    parser.add_option('--plot-detail', dest='plot_detail', type='int', default=3,
                      help='画四分量细节的组数 (按当前热度, 默认3)')
    parser.add_option('--no-plot', dest='no_plot', action='store_true', default=False)
    parser.add_option('--asof', default=None, help='point-in-time 锚日 YYYY-MM-DD')
    parser.add_option('--force', action='store_true', default=False)
    opts, _ = parser.parse_args()

    if opts.asof:
        try:
            ns._ASOF = pd.Timestamp(opts.asof).normalize()      # 让阶段1 loader 截断
            log.info(f'回测模式: 锚定 {ns._ASOF.date()}')
        except Exception:
            parser.error(f'--asof 无法解析: {opts.asof}')
    start = pd.Timestamp(opts.start).normalize() if opts.start else None

    extra = {}
    for spec in opts.basket:
        if '=' not in spec:
            parser.error(f'--basket 格式 NAME=T1,T2,...  收到: {spec}')
        nm, syms = spec.split('=', 1)
        extra[nm.strip().upper()] = [s.strip().upper().replace('.', '-')
                                     for s in syms.split(',') if s.strip()]

    tickers = ns.load_universe(opts.universe, opts.force)
    if extra:                                  # 自定义篮子的票也要进面板
        tickers = sorted(set(tickers) | {t for v in extra.values() for t in v})
    rets = build_panel(tickers, start, opts.min_history, opts.min_dv, opts.force)

    groups = resolve_groups(opts.groups, list(rets.columns), extra)
    if not groups:
        raise SystemExit('无有效分组')
    log.info('分组: ' + ', '.join(f'{k}({len(v)})' for k, v in groups.items()))

    per_raw, market = rolling_crowding(rets, groups, opts.window, opts.step, opts.theta)
    if not market.index.size:
        raise SystemExit('窗口数为0: 历史太短或 --window 太大')

    per_comp, diag = {}, {}
    for name, df in per_raw.items():
        dfc, d = composite_and_regime(df, opts.trail)
        per_comp[name], diag[name] = dfc, d

    params = {'universe': opts.universe, 'groups': opts.groups,
              'window': opts.window, 'step': opts.step, 'theta': opts.theta}

    os.makedirs(RESULT_DIR, exist_ok=True)
    tag = (ns._ASOF.date().isoformat() if ns._ASOF is not None
           else datetime.date.today().isoformat())
    out_txt = os.path.join(RESULT_DIR, f'us_network_dynamics_{tag}.txt')
    out_png = os.path.join(RESULT_DIR, f'us_network_dynamics_{tag}.png')
    out_csv = os.path.join(RESULT_DIR, f'us_network_dynamics_{tag}.csv')

    txt = write_report(out_txt, params, rets, per_comp, market, diag)
    print('\n' + txt)

    # 长表 CSV：date,group,intra,density,ratio,gravity,n,composite
    frames = []
    for name, df in per_comp.items():
        f = df.copy()
        f.insert(0, 'group', name)
        f.index.name = 'date'
        frames.append(f.reset_index())
    pd.concat(frames).to_csv(out_csv, index=False)
    log.info(f'时间序列: {out_csv}')

    # 机器可读 JSON sidecar（供 t_us_network_report.py 汇总）
    def _f(x):
        return None if x is None or (isinstance(x, float) and np.isnan(x)) else round(float(x), 4)
    groups_out = []
    for name, d in sorted(diag.items(), key=lambda kv: -kv[1]['comp_pct']):
        comp = per_comp[name]['composite']
        groups_out.append({
            'name': name, 'members': groups[name], 'stage': d['stage'], 'note': d['note'],
            'comp_now': _f(d['comp_now']), 'comp_pct': _f(d['comp_pct']),
            'comp_slope': _f(d['comp_slope']), 'drawdown': _f(d['drawdown']),
            'four_rising': bool(d['four_rising']),
            'four_slopes': {k: _f(v) for k, v in d['four_slopes'].items()},
            'intra': _f(d['intra_now']), 'ratio': _f(d['ratio_now']),
            'gravity': _f(d['gravity_now']), 'n': int(d['n']),
            'peak_composite': _f(comp.max()),
            'peak_date': comp.idxmax().date().isoformat(),
        })
    summary = {
        'stage': 'dynamic_crowding', 'date': tag, 'universe': opts.universe,
        'groups_mode': opts.groups,
        'data_range': [rets.index[0].date().isoformat(), rets.index[-1].date().isoformat()],
        'n': int(rets.shape[1]), 'window': opts.window, 'step': opts.step,
        'theta': opts.theta, 'market_mean_corr_now': _f(market.iloc[-1]),
        'asof': market.index[-1].date().isoformat(), 'groups': groups_out,
    }
    out_json = os.path.join(RESULT_DIR, f'us_network_dynamics_{tag}.json')
    with open(out_json, 'w') as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    log.info(f'JSON摘要: {out_json}')

    if not opts.no_plot:
        detail = [n for n, _ in sorted(diag.items(),
                  key=lambda kv: -kv[1]['comp_pct'])][:max(opts.plot_detail, 0)]
        title = (f'US Network Dynamics · {opts.universe} · groups={opts.groups} · '
                 f'{rets.index[-1].date()} · win{opts.window}/step{opts.step}')
        try:
            plot_dynamics(per_comp, market, diag, out_png, title, detail)
        except Exception as e:
            log.error(f'绘图失败 (不影响报告): {e}')

    log.info('完成。')


if __name__ == '__main__':
    main()
