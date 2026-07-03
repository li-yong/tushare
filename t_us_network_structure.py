# coding: utf-8
"""
US Market Network Structure — 股票市场网络结构分析 · 阶段1（静态网络 sanity check）

实现 docs/stock_market_network_structure.md 执行计划的 **阶段 0（数据管线）+
阶段 1（静态网络）**，即最小可行流程：

    收益率 → 相关矩阵 → (RMT/收缩 去噪) → Mantegna 距离 → MST → Louvain 社区 → 可视化

定位（务必牢记 spec §3 的结论）：
  · 网络法**不是选股 alpha 引擎**，而是风险/择时的环境判断工具。本阶段只做
    "看得清结构"的 sanity check —— 校验检测出的社区与官方行业分类(GICS sector)
    的吻合度，确认管线/相关结构没跑偏。真正的交易价值在阶段 2(抱团温度相变)
    与阶段 4(瓦解端事件研究)，本脚本刻意不给买卖信号。
  · 这是**静态快照**。spec §2.4/§3 反复强调相关结构强非平稳、必须滚动窗口，
    单张快照会误导 —— 阶段 2 才上动态网络。本脚本的 MST/社区只代表 lookback
    窗口内的平均结构。

数据源（与 ADR-0001 一致）：yfinance only。
  · 行情：本地 US_yf/<ticker>.csv 复权日线缓存（split/div adjusted），缺失或
    过期时回退 yfinance 拉取，再不行用陈旧缓存兜底。
  · 股票池：S&P 500 ∪ Nasdaq-100（Wikipedia 当日缓存于 US_universe/）。
  · sector 标签（社区校验用）：yfinance .info 懒抓 + 缓存到 US_universe/sectors.json，
    默认有缓存才做校验；--sectors 触发首次抓取（慢，500 名 ~数分钟，之后复用）。

维度警告：本地缓存约 3y(~750 bar)，N≈500 → T/N≈1.5 < 2，相关矩阵估计不可靠，
故 **默认 --denoise rmt**（Marchenko–Pastur 特征值裁剪去噪）。

Usage:
  python t_us_network_structure.py                       # both, 默认 RMT 去噪
  python t_us_network_structure.py --universe ndx        # 仅 Nasdaq-100
  python t_us_network_structure.py --lookback 504        # 自定义窗口(交易日)
  python t_us_network_structure.py --denoise lw          # Ledoit-Wolf 收缩
  python t_us_network_structure.py --denoise none        # 原始相关(不推荐)
  python t_us_network_structure.py --sectors             # 首次抓 sector 做社区校验
  python t_us_network_structure.py --asof 2025-03-01     # point-in-time 回测快照
  python t_us_network_structure.py --no-plot             # 跳过 PNG
  python t_us_network_structure.py --tickers AAPL,MSFT,NVDA,...   # 只跑指定票
"""

import os
import sys
import json
import time
import glob
import logging
import datetime
import warnings
from io import StringIO
from optparse import OptionParser

warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('network')

# ── 路径常量（与其它 US 脚本一致）─────────────────────────────────────────────
BAR_CACHE_DIR = '/home/ryan/DATA/DAY_Global/US_yf'           # 复权日线缓存
UNIV_DIR      = '/home/ryan/DATA/DAY_Global/US_universe'     # 股票池 / sector 缓存
RESULT_DIR    = '/home/ryan/DATA/result/us_network_structure'
SECTOR_CACHE  = os.path.join(UNIV_DIR, 'sectors.json')
UA = {'User-Agent': 'Mozilla/5.0'}

BAR_FETCH_PERIOD = '3y'

# point-in-time 锚点；None=live。设置后所有 bar 截断到 ≤ _ASOF。
_ASOF: 'pd.Timestamp | None' = None


def _now() -> 'pd.Timestamp':
    return _ASOF if _ASOF is not None else pd.Timestamp.today().normalize()


# ════════════════════════════════════════════════════════════════════════════
# data/loader.py  —  股票池 + 复权日线（阶段 0）
# ════════════════════════════════════════════════════════════════════════════
def _wiki_table(url: str, symbol_col_candidates) -> list:
    import urllib.request
    req = urllib.request.Request(url, headers=UA)
    html = urllib.request.urlopen(req, timeout=25).read().decode('utf-8', 'ignore')
    for tbl in pd.read_html(StringIO(html)):
        col = next((c for c in symbol_col_candidates if c in tbl.columns), None)
        if col is not None:
            return (tbl[col].astype(str).str.upper()
                    .str.replace('.', '-', regex=False).str.strip().tolist())
    raise ValueError(f'no symbol column in {url}')


def load_universe(which: str, force: bool) -> list:
    """which ∈ {sp500, ndx, both}. 当日缓存到 UNIV_DIR/<which>.json（同 t_us_undervalue）。"""
    os.makedirs(UNIV_DIR, exist_ok=True)
    path = os.path.join(UNIV_DIR, f'{which}.json')
    if not force and os.path.exists(path):
        mtime = datetime.date.fromtimestamp(os.path.getmtime(path))
        if mtime == datetime.date.today():
            try:
                with open(path) as fh:
                    syms = json.load(fh)
                log.info(f'股票池 {which}: {len(syms)} 只 (当日缓存)')
                return syms
            except Exception:
                pass
    parts = []
    try:
        if which in ('sp500', 'both'):
            parts += _wiki_table(
                'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
                ['Symbol', 'Ticker symbol', 'Ticker'])
        if which in ('ndx', 'both'):
            parts += _wiki_table('https://en.wikipedia.org/wiki/Nasdaq-100',
                                 ['Ticker', 'Symbol'])
    except Exception as e:
        log.error(f'股票池抓取失败 ({e})')
        if os.path.exists(path):
            with open(path) as fh:
                syms = json.load(fh)
            log.warning(f'降级使用陈旧缓存 {which}: {len(syms)} 只')
            return syms
        return []
    syms = sorted(set(s for s in parts if s and s.isascii() and 1 <= len(s) <= 6))
    try:
        with open(path, 'w') as fh:
            json.dump(syms, fh)
    except Exception:
        pass
    log.info(f'股票池 {which}: {len(syms)} 只 (新抓取)')
    return syms


def _cache_path(ticker: str) -> str:
    return os.path.join(BAR_CACHE_DIR, f'{ticker}.csv')


def _cache_is_fresh(path: str) -> bool:
    if not os.path.exists(path):
        return False
    return datetime.date.fromtimestamp(os.path.getmtime(path)) == datetime.date.today()


def _read_cache(ticker: str) -> pd.DataFrame:
    path = _cache_path(ticker)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, parse_dates=['date']).set_index('date')


def fetch_close(ticker: str, refetch: bool) -> 'pd.Series':
    """
    单票复权收盘价 Series。本地缓存优先；--force 或缓存过期时回退 yfinance，失败
    则用陈旧缓存兜底（ADR-0001 stale-cache）。--asof 下截断到 ≤ _ASOF。
    """
    path = _cache_path(ticker)
    use_net = refetch or not _cache_is_fresh(path)
    df = pd.DataFrame()
    if use_net:
        try:
            import yfinance as yf
            raw = yf.Ticker(ticker).history(period=BAR_FETCH_PERIOD, auto_adjust=True)
            if raw.empty:
                raise ValueError('empty frame')
            df = raw.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low',
                                     'Close': 'close', 'Volume': 'volume'})[
                ['open', 'high', 'low', 'close', 'volume']].copy()
            df.index = pd.to_datetime(df.index).tz_localize(None)
            df.index.name = 'date'
            df = df.dropna(subset=['close'])
            os.makedirs(BAR_CACHE_DIR, exist_ok=True)
            df.reset_index().to_csv(path, index=False)
        except Exception as e:
            df = _read_cache(ticker)
            if df.empty:
                log.debug(f'{ticker}: yfinance 失败 ({e}) 且无缓存 — 跳过')
                return pd.Series(dtype=float)
            log.debug(f'{ticker}: yfinance 失败 ({e}) — 用陈旧缓存')
    else:
        df = _read_cache(ticker)
    if df.empty:
        return pd.Series(dtype=float)
    if _ASOF is not None:
        df = df[df.index <= _ASOF]
    # 流动性：留作上层过滤所需，附在 Series.attrs
    s = df['close'].astype(float)
    try:
        s.attrs['dollar_vol'] = float((df['close'] * df['volume']).tail(63).median())
    except Exception:
        s.attrs['dollar_vol'] = np.nan
    return s


# ════════════════════════════════════════════════════════════════════════════
# data/cleaner.py  —  对齐 / 流动性过滤 → 对数收益率矩阵（阶段 0 产出）
# ════════════════════════════════════════════════════════════════════════════
def build_returns(tickers: list, lookback: int, min_history: int,
                  min_dollar_vol: float, refetch: bool) -> 'pd.DataFrame':
    """
    返回对齐后的对数收益率矩阵 R (行=交易日, 列=ticker)。

    清洗：取每票最近 lookback+1 个 ≤_now 的收盘 → 价格面板 → 内连接对齐交易日 →
    剔除历史不足 / 低流动性 / 收益缺失过多的票 → log return。
    """
    closes = {}
    skipped_liq = skipped_hist = 0
    n = len(tickers)
    for i, t in enumerate(tickers, 1):
        if i % 100 == 0:
            log.info(f'  载入收盘 {i}/{n} ...')
        s = fetch_close(t, refetch)
        if s.empty:
            continue
        s = s[s.index <= _now()].tail(lookback + 5)
        if len(s) < min_history:
            skipped_hist += 1
            continue
        if (s.attrs.get('dollar_vol') or 0) < min_dollar_vol:
            skipped_liq += 1
            continue
        closes[t] = s
    if len(closes) < 5:
        raise SystemExit(f'有效票太少 ({len(closes)})，无法建网络')

    panel = pd.DataFrame(closes).sort_index()
    # 内连接对齐：只保留所有票都有报价的交易日（停牌/上市晚的自然被裁）
    panel = panel.dropna(how='any')
    if len(panel) > lookback + 1:
        panel = panel.tail(lookback + 1)
    # 对齐后历史仍不足的窗口直接报错（避免维度灾难）
    if len(panel) < min_history:
        # 内连接太狠时退一步：放宽到 80% 覆盖的交易日 + 列级 dropna
        log.warning(f'内连接后仅 {len(panel)} 日，放宽对齐到 80% 覆盖')
        full = pd.DataFrame(closes).sort_index()
        full = full[full.isna().mean(axis=1) <= 0.20]
        full = full.dropna(axis=1, thresh=int(0.95 * len(full)))
        panel = full.ffill().dropna(how='any').tail(lookback + 1)

    rets = np.log(panel / panel.shift(1)).dropna(how='all').iloc[1:]
    rets = rets.dropna(axis=1)               # 任一缺口的票直接剔除，保证矩阵干净
    log.info(f'收益率矩阵: T={rets.shape[0]} 日 × N={rets.shape[1]} 票 '
             f'(剔除 低流动性{skipped_liq} / 历史不足{skipped_hist}); '
             f'T/N={rets.shape[0]/max(rets.shape[1],1):.2f}')
    if rets.shape[1] < 5:
        raise SystemExit('对齐后有效票太少，无法建网络')
    return rets


# ════════════════════════════════════════════════════════════════════════════
# network/correlation.py  —  相关矩阵 + RMT / Ledoit-Wolf 去噪
# ════════════════════════════════════════════════════════════════════════════
def correlation(rets: 'pd.DataFrame', method: str) -> 'pd.DataFrame':
    """method ∈ {none, rmt, lw}. 返回（去噪后）相关矩阵 DataFrame。"""
    C = rets.corr().values
    cols = rets.columns
    T, N = rets.shape
    if method == 'none':
        out = C
    elif method == 'lw':
        from sklearn.covariance import LedoitWolf
        # 在标准化收益上估协方差 ≈ 收缩相关矩阵
        Z = (rets - rets.mean()) / rets.std(ddof=0)
        cov = LedoitWolf().fit(Z.values).covariance_
        d = np.sqrt(np.clip(np.diag(cov), 1e-12, None))
        out = cov / np.outer(d, d)
        log.info('相关矩阵: Ledoit-Wolf 收缩')
    elif method == 'rmt':
        out = _rmt_denoise(C, T, N)
    else:
        raise ValueError(f'unknown denoise method {method}')
    np.fill_diagonal(out, 1.0)
    out = np.clip(out, -1.0, 1.0)
    return pd.DataFrame(out, index=cols, columns=cols)


def _rmt_denoise(C: 'np.ndarray', T: int, N: int) -> 'np.ndarray':
    """
    Marchenko–Pastur 特征值裁剪（Laloux/Plerou）：
      · q = N/T；MP 上沿 λ+ = (1 + sqrt(q))^2
      · 保留 > λ+ 的特征值（含最大的"市场模式"），bulk（噪声）特征值用其均值替换
        以保迹（trace），重构后把对角线归一回 1。
    """
    q = N / T
    lam_plus = (1.0 + np.sqrt(q)) ** 2
    vals, vecs = np.linalg.eigh(C)            # 升序
    signal = vals > lam_plus
    n_signal = int(signal.sum())
    bulk = ~signal
    if bulk.any():
        avg_bulk = vals[bulk].mean()
        vals_clean = np.where(bulk, avg_bulk, vals)
    else:
        vals_clean = vals
    Cc = (vecs * vals_clean) @ vecs.T
    d = np.sqrt(np.clip(np.diag(Cc), 1e-12, None))
    Cc = Cc / np.outer(d, d)                   # 归一对角线 → 真·相关矩阵
    log.info(f'相关矩阵: RMT 去噪 (q={q:.3f}, λ+={lam_plus:.3f}, '
             f'保留 {n_signal}/{N} 个信号特征值，其余 {int(bulk.sum())} 个判为噪声)')
    return Cc


# ════════════════════════════════════════════════════════════════════════════
# network/distance.py + filter.py  —  Mantegna 距离 + MST
# ════════════════════════════════════════════════════════════════════════════
def mantegna_distance(corr: 'pd.DataFrame') -> 'pd.DataFrame':
    """d_ij = sqrt(2 (1 - ρ_ij))；ρ=1→0(最近)，ρ=-1→2(最远)。"""
    D = np.sqrt(np.clip(2.0 * (1.0 - corr.values), 0.0, 4.0))
    return pd.DataFrame(D, index=corr.index, columns=corr.columns)


def build_mst(dist: 'pd.DataFrame', corr: 'pd.DataFrame'):
    """从距离矩阵建全连接图取 MST。边带 distance 与 corr 两个属性。"""
    import networkx as nx
    G = nx.Graph()
    names = list(dist.index)
    G.add_nodes_from(names)
    D = dist.values
    R = corr.values
    n = len(names)
    for i in range(n):
        for j in range(i + 1, n):
            G.add_edge(names[i], names[j], distance=float(D[i, j]), corr=float(R[i, j]))
    mst = nx.minimum_spanning_tree(G, weight='distance')
    log.info(f'MST: {mst.number_of_nodes()} 节点 / {mst.number_of_edges()} 边')
    return mst


# ════════════════════════════════════════════════════════════════════════════
# analysis/community.py + centrality.py + topology.py
# ════════════════════════════════════════════════════════════════════════════
def detect_communities(mst, corr: 'pd.DataFrame', seed: int = 42) -> dict:
    """
    Louvain 社区检测（spec 最小流程 MST + Louvain）。在 MST 上跑，边权用相关性
    的正部 ρ⁺ 作相似度（Louvain 最大化模块度需相似度而非距离）。
    返回 {node: community_id}。
    """
    import networkx as nx
    import community as community_louvain   # python-louvain
    H = nx.Graph()
    H.add_nodes_from(mst.nodes())
    for u, v, d in mst.edges(data=True):
        H.add_edge(u, v, weight=max(d['corr'], 1e-4))
    part = community_louvain.best_partition(H, weight='weight', random_state=seed)
    n_comm = len(set(part.values()))
    log.info(f'Louvain 社区: {n_comm} 个')
    return part


def centralities(mst) -> 'pd.DataFrame':
    """MST 上的结构中心性（spec §2.2A：找结构枢纽，非引领者）。"""
    import networkx as nx
    deg = dict(mst.degree())
    btw = nx.betweenness_centrality(mst, weight='distance')
    # eigenvector 用相似度权重；MST 是树，一般可收敛，失败则退回 degree
    try:
        H = mst.copy()
        for u, v, d in H.edges(data=True):
            H[u][v]['w'] = max(d['corr'], 1e-4)
        eig = nx.eigenvector_centrality_numpy(H, weight='w')
    except Exception:
        eig = {n: deg[n] for n in mst.nodes()}
    df = pd.DataFrame({'degree': deg, 'betweenness': btw, 'eigenvector': eig})
    return df


def topology(corr: 'pd.DataFrame', mst) -> dict:
    """整体拓扑温度计（spec §2.2C）。静态快照值，趋势义见阶段 2。"""
    import networkx as nx
    R = corr.values
    iu = np.triu_indices_from(R, k=1)
    off = R[iu]
    metric = {
        'mean_corr':   float(off.mean()),
        'median_corr': float(np.median(off)),
        'pct_corr>0.5': float((off > 0.5).mean()) * 100,
        'mst_total_len': float(sum(d['distance'] for *_, d in mst.edges(data=True))),
    }
    metric['mst_avg_edge_len'] = metric['mst_total_len'] / max(mst.number_of_edges(), 1)
    try:
        metric['mst_diameter'] = nx.diameter(mst)               # 跳数
    except Exception:
        metric['mst_diameter'] = None
    degs = [d for _, d in mst.degree()]
    metric['mst_max_degree'] = max(degs) if degs else 0
    return metric


# ── sector 标签（社区校验，可选）─────────────────────────────────────────────
def load_sectors(tickers: list, do_fetch: bool) -> dict:
    """读 SECTOR_CACHE；--sectors 时为缺失票懒抓 yfinance .info（throttle）并回写。"""
    cache = {}
    if os.path.exists(SECTOR_CACHE):
        try:
            with open(SECTOR_CACHE) as fh:
                cache = json.load(fh)
        except Exception:
            cache = {}
    missing = [t for t in tickers if t not in cache]
    if missing and do_fetch:
        import yfinance as yf
        log.info(f'抓取 sector 标签 {len(missing)} 只 (yfinance .info, 慢) ...')
        for i, t in enumerate(missing, 1):
            try:
                cache[t] = (yf.Ticker(t).info or {}).get('sector') or 'Unknown'
            except Exception:
                cache[t] = 'Unknown'
            if i % 25 == 0:
                log.info(f'  sector {i}/{len(missing)} ...')
                try:
                    with open(SECTOR_CACHE, 'w') as fh:
                        json.dump(cache, fh)
                except Exception:
                    pass
            time.sleep(0.15)
        try:
            os.makedirs(UNIV_DIR, exist_ok=True)
            with open(SECTOR_CACHE, 'w') as fh:
                json.dump(cache, fh)
        except Exception:
            pass
    return {t: cache.get(t, 'Unknown') for t in tickers}


def community_sector_purity(part: dict, sectors: dict) -> 'tuple[float, pd.DataFrame]':
    """
    每个社区的主导 sector + 纯度（主导 sector 占比），以及整体加权纯度。
    sanity check：纯度高 → 检测社区与 GICS 行业吻合，相关结构/管线正常。
    """
    from collections import Counter, defaultdict
    comm_members = defaultdict(list)
    for node, c in part.items():
        comm_members[c].append(node)
    rows = []
    total = 0
    weighted_hit = 0.0
    for c, members in sorted(comm_members.items(), key=lambda kv: -len(kv[1])):
        labs = [sectors.get(m, 'Unknown') for m in members]
        cnt = Counter(labs)
        dom, dom_n = cnt.most_common(1)[0]
        size = len(members)
        rows.append({'community': c, 'size': size, 'dominant_sector': dom,
                     'purity': dom_n / size,
                     'sectors': ', '.join(f'{k}:{v}' for k, v in cnt.most_common(4))})
        total += size
        weighted_hit += dom_n
    df = pd.DataFrame(rows)
    overall = weighted_hit / total if total else 0.0
    return overall, df


# ════════════════════════════════════════════════════════════════════════════
# viz/plot.py  —  MST 可视化（社区上色 / 度大小）
# ════════════════════════════════════════════════════════════════════════════
def plot_mst(mst, part: dict, cent: 'pd.DataFrame', out_png: str, title: str):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import networkx as nx

    plt.figure(figsize=(20, 16))
    pos = nx.spring_layout(mst, seed=42, k=1.2 / np.sqrt(max(mst.number_of_nodes(), 1)),
                           iterations=120, weight=None)
    comms = sorted(set(part.values()))
    cmap = plt.get_cmap('tab20')
    node_color = [cmap(part[n] % 20) for n in mst.nodes()]
    deg = cent['degree']
    sizes = [80 + 90 * deg.get(n, 1) for n in mst.nodes()]

    nx.draw_networkx_edges(mst, pos, alpha=0.30, width=0.7)
    nx.draw_networkx_nodes(mst, pos, node_color=node_color, node_size=sizes,
                           linewidths=0.3, edgecolors='white')
    # 只给枢纽（度 top）打标签，避免糊成一团
    hubs = deg.sort_values(ascending=False).head(40).index
    nx.draw_networkx_labels(mst, pos, labels={n: n for n in hubs}, font_size=8)
    plt.title(title, fontsize=15)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(out_png, dpi=130, bbox_inches='tight')
    plt.close()
    log.info(f'图已保存: {out_png}')


# ════════════════════════════════════════════════════════════════════════════
# 报告 + main
# ════════════════════════════════════════════════════════════════════════════
def write_report(out_txt, params, rets, corr, mst, part, cent, topo,
                 sector_overall, sector_df):
    from collections import defaultdict
    lines = []
    P = lines.append
    P('=' * 78)
    P('股票市场网络结构分析 · 阶段1 静态网络 (sanity check)')
    P('=' * 78)
    P(f'生成时间      : {datetime.datetime.now():%Y-%m-%d %H:%M:%S}'
      + ('  [回测 --asof %s]' % _ASOF.date() if _ASOF is not None else ''))
    P(f'股票池        : {params["universe_desc"]}')
    P(f'窗口          : 最近 {params["lookback"]} 交易日  '
      f'(T={rets.shape[0]} × N={rets.shape[1]}, T/N={rets.shape[0]/rets.shape[1]:.2f})')
    P(f'数据区间      : {rets.index[0].date()} → {rets.index[-1].date()}')
    P(f'去噪          : {params["denoise"]}')
    P('')
    P('⚠ 这是静态快照，只代表窗口内平均结构；非选股信号。趋势/相变见阶段2，')
    P('  方向性龙头见阶段3(需有向信息流)，交易价值检验见阶段4。')
    P('')

    P('── 拓扑温度计 (整体市场状态) ' + '─' * 46)
    P(f'  平均相关性 mean_corr      : {topo["mean_corr"]:+.3f}   '
      f'(中位 {topo["median_corr"]:+.3f}; |ρ|>0.5 占 {topo["pct_corr>0.5"]:.1f}%)')
    P(f'  MST 总长 / 平均边长       : {topo["mst_total_len"]:.2f} / {topo["mst_avg_edge_len"]:.3f}')
    P(f'  MST 直径(跳) / 最大度     : {topo["mst_diameter"]} / {topo["mst_max_degree"]}')
    P('  解读: mean_corr 越高、MST 越短越星形 → 市场越"everything moves together"')
    P('        (拥挤/风险升高)。绝对值意义有限，价值在阶段2 的时间序列拐点。')
    P('')

    P('── 结构枢纽 (MST 中心性 · spec §2.2A) ' + '─' * 38)
    P('  注意: 对称相关网络衡量"最同步/最有代表性"的票(常是大盘股/板块代理)，')
    P('        是"结构核心"≠"趋势引领者"。找龙头须用阶段3 有向信息流。')
    P('  [度中心性 top — 板块内枢纽]')
    for n, r in cent.sort_values('degree', ascending=False).head(15).iterrows():
        P(f'    {n:6} deg={int(r["degree"]):2d}  btw={r["betweenness"]:.3f}  '
          f'eig={r["eigenvector"]:.3f}')
    P('  [介数中心性 top — 跨板块枢纽 / 风险传染关键]')
    for n, r in cent.sort_values('betweenness', ascending=False).head(15).iterrows():
        P(f'    {n:6} btw={r["betweenness"]:.3f}  deg={int(r["degree"]):2d}')
    P('')

    P('── 社区 (Louvain on MST · spec §2.2B) ' + '─' * 38)
    comm_members = defaultdict(list)
    for node, c in part.items():
        comm_members[c].append(node)
    P(f'  共 {len(comm_members)} 个社区')
    for c, members in sorted(comm_members.items(), key=lambda kv: -len(kv[1])):
        # 社区代表 = 度最高的几个
        reps = cent.loc[members, 'degree'].sort_values(ascending=False).head(8).index.tolist()
        P(f'  · C{c} (n={len(members)}): {", ".join(reps)}'
          + (' ...' if len(members) > 8 else ''))
    P('')

    if sector_df is not None:
        P('── 社区 vs GICS sector 校验 (阶段1 sanity check) ' + '─' * 27)
        P(f'  整体加权纯度: {sector_overall:.1%}   '
          '(越高=检测社区越贴合官方行业 → 管线/相关结构正常)')
        for _, r in sector_df.iterrows():
            P(f'    C{r["community"]} (n={r["size"]:>3}): {r["dominant_sector"]:<24} '
              f'纯度 {r["purity"]:.0%}   [{r["sectors"]}]')
    else:
        P('── 社区 vs sector 校验: 跳过 (无 sector 缓存; 用 --sectors 首次抓取) ──')
    P('')
    P('=' * 78)

    txt = '\n'.join(lines)
    with open(out_txt, 'w') as fh:
        fh.write(txt)
    return txt


def main():
    global _ASOF
    parser = OptionParser()
    parser.add_option('--universe', default='both', help='sp500 | ndx | both (默认 both)')
    parser.add_option('--tickers', default=None, help='逗号分隔，只跑指定票 (跳过宽源)')
    parser.add_option('--lookback', type='int', default=504,
                      help='相关窗口交易日 (默认 504≈2y; 本地缓存约750)')
    parser.add_option('--min-history', dest='min_history', type='int', default=250,
                      help='对齐后最少交易日 (默认 250)')
    parser.add_option('--min-dollar-vol', dest='min_dv', type='float', default=5e6,
                      help='近63日中位成交额下限美元 (默认 5e6)')
    parser.add_option('--denoise', default='rmt', help='rmt | lw | none (默认 rmt)')
    parser.add_option('--sectors', action='store_true', default=False,
                      help='为缺失票抓取 sector 做社区校验 (慢，首次)')
    parser.add_option('--asof', default=None, help='point-in-time 回测锚日 YYYY-MM-DD')
    parser.add_option('--no-plot', dest='no_plot', action='store_true', default=False)
    parser.add_option('--force', action='store_true', default=False,
                      help='忽略当日缓存，重拉股票池 + 重新下载行情')
    opts, _ = parser.parse_args()

    if opts.asof:
        try:
            _ASOF = pd.Timestamp(opts.asof).normalize()
            log.info(f'回测模式: 锚定 {_ASOF.date()}')
        except Exception:
            parser.error(f'--asof 无法解析: {opts.asof}')
    if opts.denoise not in ('rmt', 'lw', 'none'):
        parser.error('--denoise 必须是 rmt|lw|none')

    if opts.tickers:
        tickers = sorted(set(t.strip().upper().replace('.', '-')
                             for t in opts.tickers.split(',') if t.strip()))
        universe_desc = f'指定 {len(tickers)} 只'
    else:
        tickers = load_universe(opts.universe, opts.force)
        universe_desc = f'{opts.universe} ({len(tickers)} 只)'
    if len(tickers) < 5:
        raise SystemExit('股票池太小')

    params = {'universe_desc': universe_desc, 'lookback': opts.lookback,
              'denoise': opts.denoise}

    # 阶段 0：管线 → 收益率矩阵
    rets = build_returns(tickers, opts.lookback, opts.min_history,
                         opts.min_dv, opts.force)
    # 阶段 1：相关 → 去噪 → 距离 → MST → 社区 → 中心性 → 拓扑
    corr = correlation(rets, opts.denoise)
    dist = mantegna_distance(corr)
    mst  = build_mst(dist, corr)
    part = detect_communities(mst, corr)
    cent = centralities(mst)
    topo = topology(corr, mst)

    sectors = load_sectors(list(rets.columns), opts.sectors)
    if any(v != 'Unknown' for v in sectors.values()):
        sector_overall, sector_df = community_sector_purity(part, sectors)
    else:
        sector_overall, sector_df = None, None

    # 输出
    os.makedirs(RESULT_DIR, exist_ok=True)
    tag = (_ASOF.date().isoformat() if _ASOF is not None
           else datetime.date.today().isoformat())
    out_txt = os.path.join(RESULT_DIR, f'us_network_structure_{tag}.txt')
    out_png = os.path.join(RESULT_DIR, f'us_network_structure_{tag}.png')
    out_csv = os.path.join(RESULT_DIR, f'us_network_nodes_{tag}.csv')

    txt = write_report(out_txt, params, rets, corr, mst, part, cent, topo,
                       sector_overall, sector_df)
    print('\n' + txt)

    # 节点级明细 CSV（社区 + 中心性 + sector），供下游/阶段2 复用
    node_df = cent.copy()
    node_df['community'] = pd.Series(part)
    node_df['sector'] = pd.Series(sectors)
    node_df.sort_values(['community', 'degree'], ascending=[True, False]).to_csv(out_csv)
    log.info(f'节点明细: {out_csv}')

    # 机器可读 JSON sidecar（供阶段汇总脚本 t_us_network_report.py 读取）
    from collections import defaultdict as _dd
    members = _dd(list)
    for nd_, c in part.items():
        members[c].append(nd_)
    sec_purity = {}
    if sector_df is not None:
        sec_purity = {int(r['community']): {'dominant': r['dominant_sector'],
                      'purity': float(r['purity'])} for _, r in sector_df.iterrows()}
    summary = {
        'stage': 'static_network', 'date': tag, 'universe': universe_desc,
        'n': int(rets.shape[1]), 'T': int(rets.shape[0]),
        'TN': round(rets.shape[0] / rets.shape[1], 2),
        'data_range': [rets.index[0].date().isoformat(), rets.index[-1].date().isoformat()],
        'denoise': params['denoise'], 'lookback': params['lookback'],
        'topology': {k: (round(v, 4) if isinstance(v, float) else v)
                     for k, v in topo.items()},
        'sector_purity_overall': (round(sector_overall, 4)
                                  if sector_overall is not None else None),
        'hubs_degree': [{'ticker': n, 'degree': int(r['degree']),
                         'betweenness': round(r['betweenness'], 3),
                         'eigenvector': round(r['eigenvector'], 3)}
                        for n, r in cent.sort_values('degree', ascending=False).head(10).iterrows()],
        'hubs_betweenness': [{'ticker': n, 'betweenness': round(r['betweenness'], 3),
                              'degree': int(r['degree'])}
                             for n, r in cent.sort_values('betweenness', ascending=False).head(10).iterrows()],
        'communities': [
            {'id': int(c), 'size': len(m),
             'members_top': cent.loc[m, 'degree'].sort_values(ascending=False).head(8).index.tolist(),
             'dominant_sector': sec_purity.get(int(c), {}).get('dominant'),
             'purity': sec_purity.get(int(c), {}).get('purity')}
            for c, m in sorted(members.items(), key=lambda kv: -len(kv[1]))],
    }
    out_json = os.path.join(RESULT_DIR, f'us_network_structure_{tag}.json')
    with open(out_json, 'w') as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    log.info(f'JSON摘要: {out_json}')

    if not opts.no_plot:
        title = (f'US Market MST · {universe_desc} · {rets.index[-1].date()} · '
                 f'denoise={opts.denoise} · {len(set(part.values()))} communities')
        try:
            plot_mst(mst, part, cent, out_png, title)
        except Exception as e:
            log.error(f'绘图失败 (不影响报告): {e}')

    log.info('完成。')


if __name__ == '__main__':
    main()
