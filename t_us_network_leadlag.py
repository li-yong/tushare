#!/usr/bin/env python3
"""
US Market Network — 领先性检验 (阶段3 的日线雏形 / directed information flow)

回答阶段1/2 的对称相关网络答不了的问题(spec §2.3 应用二 / §4.2 阶段3):
某只票(focus)到底是"领先指标",还是只是"对称的结构枢纽"?对称相关测不出方向,
本脚本用三个【有方向】的工具检验,并构建有向信息流网络:

  1. 滞后互相关 (lagged cross-correlation)  —— focus 领先 vs 滞后(按显著性判方向)
  2. 转移熵 (transfer entropy)              —— 有方向的信息流(可抓非线性)
  3. Granger 因果                           —— 经典线性领先检验
最后看 focus 的【净信息流出】是否显著为正;三项里 ≥2 项支持才算"有领先迹象"。

实证(2026-06, 半导体簇日线): LRCX / NVDA 当 focus 均 0/3 不支持 —— 日线尺度
没有稳定领先,同涨同跌更像被共同因子(AI/半导体周期、利率、beta)一起推动;
真要找日内领先须上分钟级数据(完整阶段3, spec §4.1 第三层)。

数据: 本地缓存优先(CACHE_DIR, 独立于 US_yf 主缓存, 见下), 覆盖不到才联网 yfinance。

用法:
  python t_us_network_leadlag.py
  (改 CONFIG 里的 focus / tickers / 日期 / lag 即可; refresh=True 强制重拉)

依赖: yfinance pandas numpy scipy statsmodels networkx matplotlib
  pip install yfinance pandas numpy scipy statsmodels networkx matplotlib
"""

from __future__ import annotations
import os
import itertools
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from scipy import stats
from statsmodels.tsa.stattools import grangercausalitytests

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
CONFIG = {
    "focus": "LRCX",                       # 要检验的"疑似龙头"
    "tickers": [
        "LRCX", "AMAT", "KLAC", "ASML",    # 设备
        "NVDA", "AMD", "AVGO",             # 设计/算力
        "TSM", "MU", "INTC",               # 制造/IDM
        # "SOXX",                          # 可选:板块 ETF 作为"基准节点"
    ],
    "start": "2022-01-01",
    "end":   "2025-06-01",
    "max_lag": 3,          # 滞后相关 / Granger 检验的最大滞后(交易日)
    "te_lag": 1,           # 转移熵的滞后阶 (k=1)
    "te_bins": 6,          # 转移熵离散化的分箱数
    "granger_lag": 2,      # Granger 用的滞后阶
    "alpha": 0.05,         # 显著性阈值 (滞后相关 + Granger 共用)
    "outdir": ".",         # 图片输出目录
    "refresh": False,      # True=强制联网重拉, False=有本地缓存就用
}

# 本脚本独立缓存目录:不动 US_yf 主缓存(主缓存只存3年且要保持更新到当天,
# 本实验窗口可回溯到 2022,覆盖范围不同,故各管各的)。
CACHE_DIR = "/home/ryan/DATA/DAY_Global/US_yf_leadlag"


# ----------------------------------------------------------------------
# 1. 数据
# ----------------------------------------------------------------------
def _cached_close(ticker, start, end, refresh):
    """
    单票复权收盘 Series:本地缓存优先,缓存覆盖不到所需区间(或 refresh)才联网
    yfinance 拉取并写回 CACHE_DIR。返回 (series, fetched_bool)。
    历史区间固定(end 已是过去),一旦缓存覆盖就永久离线复用。
    """
    path = os.path.join(CACHE_DIR, f"{ticker}.csv")
    s = None
    if os.path.exists(path) and not refresh:
        s = pd.read_csv(path, parse_dates=["date"]).set_index("date")["close"]
        # 容差 7 天:start/end 可能落在周末或假日,首/末个交易日会偏几天
        tol = pd.Timedelta(days=7)
        covers = (s.index.min() <= pd.Timestamp(start) + tol
                  and s.index.max() >= pd.Timestamp(end) - tol)
        if not covers:
            s = None
    if s is not None:
        return s[(s.index >= pd.Timestamp(start)) & (s.index <= pd.Timestamp(end))], False
    import yfinance as yf
    raw = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
    if raw.empty:
        return pd.Series(dtype=float), True
    s = raw["Close"].copy()
    s.index = pd.to_datetime(s.index).tz_localize(None)
    os.makedirs(CACHE_DIR, exist_ok=True)
    s.rename("close").rename_axis("date").reset_index().to_csv(path, index=False)
    return s[(s.index >= pd.Timestamp(start)) & (s.index <= pd.Timestamp(end))], True


def fetch_log_returns(tickers, start, end, refresh=False) -> pd.DataFrame:
    """本地缓存优先取复权收盘 -> 对齐 -> 对数收益率。"""
    cols, cached, fetched = {}, [], []
    for t in tickers:
        s, was_fetched = _cached_close(t, start, end, refresh)
        if s.empty:
            print(f"[data] !! {t} 无数据,跳过")
            continue
        cols[t] = s
        (fetched if was_fetched else cached).append(t)
    raw = pd.DataFrame(cols)
    # 对齐交易日历:任一票缺失的日子整行丢弃(保证同步采样)
    raw = raw.dropna(how="any")
    rets = np.log(raw).diff().dropna(how="any")
    print(f"[data] {rets.shape[0]} 个交易日 x {rets.shape[1]} 只票  "
          f"({rets.index.min().date()} ~ {rets.index.max().date()})  "
          f"[本地缓存 {len(cached)} / 联网 {len(fetched)}]")
    return rets


# ----------------------------------------------------------------------
# 2. 滞后互相关
# ----------------------------------------------------------------------
def lagged_corr(x: np.ndarray, y: np.ndarray, lag: int):
    """corr( x[t], y[t+lag] ),返回 (r, p)。lag>0 => x 领先 y。"""
    if lag > 0:
        a, b = x[:-lag], y[lag:]
    elif lag < 0:
        a, b = x[-lag:], y[:lag]
    else:
        a, b = x, y
    if len(a) < 5:
        return (np.nan, np.nan)
    r, p = stats.pearsonr(a, b)
    return (float(r), float(p))


def lead_lag_table(rets: pd.DataFrame, focus: str, max_lag: int, alpha: float) -> pd.DataFrame:
    """
    比较 focus 领先 vs 滞后,并按【显著性】判定方向(不再只看符号):
      focus领先? ✓  仅当 领先相关显著(p<alpha) 且 |lead|>|lag| 且 滞后相关不显著
                    (单向领先,与 Granger 列同口径);两侧都显著=双向,否则 —
    """
    def _best(x, y, lags):
        cand = [lagged_corr(x, y, l) for l in lags]
        cand = [c for c in cand if not np.isnan(c[0])] or [(np.nan, np.nan)]
        return max(cand, key=lambda rp: abs(rp[0]) if not np.isnan(rp[0]) else -1)

    others = [c for c in rets.columns if c != focus]
    x = rets[focus].values
    rows = []
    for o in others:
        y = rets[o].values
        lead_r, lead_p = _best(x, y, range(1, max_lag + 1))     # focus 领先
        lag_r,  lag_p  = _best(x, y, range(-max_lag, 0))        # focus 滞后
        lead_sig = (not np.isnan(lead_p)) and lead_p < alpha
        lag_sig  = (not np.isnan(lag_p)) and lag_p < alpha
        if lead_sig and abs(lead_r) > abs(lag_r) and not lag_sig:
            verdict = "✓"
        elif lead_sig and lag_sig:
            verdict = "双向"
        else:
            verdict = "—"
        rows.append({
            "pair": f"{focus}->{o}",
            "lead_r(先)": round(lead_r, 3),
            "lead_p":     round(lead_p, 3),
            "lag_r(后)":  round(lag_r, 3),
            "lag_p":      round(lag_p, 3),
            "focus领先?": verdict,
        })
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------
# 3. 转移熵 (离散化 / 直方图法)
# ----------------------------------------------------------------------
def _discretize(series: np.ndarray, bins: int) -> np.ndarray:
    """按分位数分箱成等频符号,适合厚尾的收益率。"""
    ranks = stats.rankdata(series, method="average") / len(series)
    edges = np.linspace(0, 1, bins + 1)
    return np.clip(np.digitize(ranks, edges[1:-1]), 0, bins - 1)


def transfer_entropy(source: np.ndarray, target: np.ndarray,
                     bins: int = 6, k: int = 1) -> float:
    """
    TE(source -> target):source 的过去对 target 未来的信息增益(bits)。
    TE = sum p(yn, yp, xp) * log2[ p(yn|yp,xp) / p(yn|yp) ]
      yn = target[t],  yp = target[t-k],  xp = source[t-k]
    """
    s = _discretize(source, bins)
    t = _discretize(target, bins)
    yn = t[k:]
    yp = t[:-k]
    xp = s[:-k]

    def _entropy_from_counts(*arrs):
        stacked = np.vstack(arrs).T
        _, counts = np.unique(stacked, axis=0, return_counts=True)
        p = counts / counts.sum()
        return -np.sum(p * np.log2(p))

    H_yn_yp     = _entropy_from_counts(yn, yp)
    H_yp        = _entropy_from_counts(yp)
    H_yn_yp_xp  = _entropy_from_counts(yn, yp, xp)
    H_yp_xp     = _entropy_from_counts(yp, xp)
    # TE = H(yn|yp) - H(yn|yp,xp) = [H(yn,yp)-H(yp)] - [H(yn,yp,xp)-H(yp,xp)]
    te = (H_yn_yp - H_yp) - (H_yn_yp_xp - H_yp_xp)
    return max(te, 0.0)   # 数值噪声可能略小于0,截断


def te_matrix(rets: pd.DataFrame, bins: int, k: int) -> pd.DataFrame:
    cols = list(rets.columns)
    M = pd.DataFrame(0.0, index=cols, columns=cols)   # M[i,j] = TE(i -> j)
    for i, j in itertools.permutations(cols, 2):
        M.loc[i, j] = transfer_entropy(rets[i].values, rets[j].values, bins, k)
    return M


def net_te_flow(M: pd.DataFrame) -> pd.DataFrame:
    """净信息流出 = 出 - 入。>0 说明更像信息源(领先者)。"""
    out_flow = M.sum(axis=1)    # 行和:i 流向所有 j
    in_flow  = M.sum(axis=0)    # 列和:所有 j 流向 i
    df = pd.DataFrame({
        "出 (out)": out_flow,
        "入 (in)":  in_flow,
        "净流出 (net)": out_flow - in_flow,
    }).sort_values("净流出 (net)", ascending=False)
    return df.round(4)


# ----------------------------------------------------------------------
# 4. Granger 因果
# ----------------------------------------------------------------------
def granger_p(cause: np.ndarray, effect: np.ndarray, maxlag: int) -> float:
    """检验 cause 是否 Granger-引起 effect。返回最小 p 值。"""
    data = np.column_stack([effect, cause])   # statsmodels: 第2列 -> 第1列
    try:
        res = grangercausalitytests(data, maxlag=maxlag)
        return min(res[l][0]["ssr_ftest"][1] for l in range(1, maxlag + 1))
    except Exception:
        return np.nan


def granger_from_focus(rets: pd.DataFrame, focus: str, maxlag: int, alpha: float):
    others = [c for c in rets.columns if c != focus]
    rows = []
    for o in others:
        p_out = granger_p(rets[focus].values, rets[o].values, maxlag)  # focus -> o
        p_in  = granger_p(rets[o].values, rets[focus].values, maxlag)  # o -> focus
        rows.append({
            "target": o,
            f"{focus}->target p": round(p_out, 4),
            f"target->{focus} p": round(p_in, 4),
            "focus领先?": "✓" if (p_out < alpha and not p_in < alpha) else
                          ("双向" if (p_out < alpha and p_in < alpha) else "—"),
        })
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------
# 5. 有向网络可视化
# ----------------------------------------------------------------------
def plot_te_network(M: pd.DataFrame, focus: str, outpath: str, top_frac=0.6):
    """只画较强的有向边;节点大小=净信息流出,LRCX 高亮。"""
    net = net_te_flow(M)["净流出 (net)"]
    # 边阈值:保留权重前 top_frac 的边,避免全连接糊成一团
    weights = [M.loc[i, j] for i in M.index for j in M.columns if i != j]
    thr = np.quantile(weights, 1 - top_frac)

    G = nx.DiGraph()
    for n in M.index:
        G.add_node(n)
    for i in M.index:
        for j in M.columns:
            if i != j and M.loc[i, j] >= thr:
                G.add_edge(i, j, weight=M.loc[i, j])

    pos = nx.spring_layout(G, seed=42, k=1.2)
    sizes = [2000 + 9000 * (net[n] - net.min()) / (net.max() - net.min() + 1e-9)
             for n in G.nodes]
    colors = ["#d62728" if n == focus else "#1f77b4" for n in G.nodes]
    ews = [G[u][v]["weight"] for u, v in G.edges]
    ew_norm = [0.5 + 4 * (w - min(ews)) / (max(ews) - min(ews) + 1e-9) for w in ews]

    plt.figure(figsize=(11, 8))
    nx.draw_networkx_nodes(G, pos, node_size=sizes, node_color=colors, alpha=0.9)
    nx.draw_networkx_labels(G, pos, font_size=10, font_weight="bold",
                            font_color="white")
    nx.draw_networkx_edges(G, pos, width=ew_norm, edge_color="#888",
                           arrowstyle="-|>", arrowsize=16,
                           connectionstyle="arc3,rad=0.08", alpha=0.6)
    plt.title(f"半导体抱团簇 · 转移熵有向信息流\n"
              f"(节点大小=净信息流出;红色={focus};箭头=信息流向)", fontsize=12)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(outpath, dpi=150, bbox_inches="tight")
    print(f"[plot] saved -> {outpath}")


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main(cfg=CONFIG):
    focus = cfg["focus"]
    rets = fetch_log_returns(cfg["tickers"], cfg["start"], cfg["end"],
                             cfg.get("refresh", False))

    print("\n" + "=" * 64)
    print(f"1) 滞后互相关:{focus} 领先 vs 滞后(按显著性判方向)")
    print("=" * 64)
    ll = lead_lag_table(rets, focus, cfg["max_lag"], cfg["alpha"])
    print(ll.to_string(index=False))
    n_lead_ll = int((ll["focus领先?"] == "✓").sum())
    print(f"\n  -> {focus} 显著单向领先(p<{cfg['alpha']}):{n_lead_ll}/{len(ll)} 个标的")

    print("\n" + "=" * 64)
    print("2) 转移熵:净信息流出排名(>0 = 更像信息源/领先者)")
    print("=" * 64)
    M = te_matrix(rets, cfg["te_bins"], cfg["te_lag"])
    net = net_te_flow(M)
    print(net.to_string())
    rank = list(net.index).index(focus) + 1
    print(f"\n  -> {focus} 净流出排名 第 {rank}/{len(net)} 位,"
          f"净值 = {net.loc[focus, '净流出 (net)']:+.4f}")

    print("\n" + "=" * 64)
    print(f"3) Granger 因果(maxlag={cfg['granger_lag']}, α={cfg['alpha']})")
    print("=" * 64)
    gr = granger_from_focus(rets, focus, cfg["granger_lag"], cfg["alpha"])
    print(gr.to_string(index=False))
    n_lead = (gr["focus领先?"] == "✓").sum()
    print(f"\n  -> {focus} 单向领先的标的数:{n_lead}/{len(gr)}")

    plot_te_network(M, focus, f"{cfg['outdir']}/te_network.png")

    # ---- 综合结论 ----
    print("\n" + "=" * 64)
    print("综合判断")
    print("=" * 64)
    score = 0
    if n_lead_ll >= len(ll) / 2:
        score += 1; print(f"  [+] 滞后相关:{focus} 显著单向领先多数标的 ({n_lead_ll}/{len(ll)})")
    else:
        print(f"  [-] 滞后相关:显著单向领先仅 {n_lead_ll}/{len(ll)},未见系统性领先")
    if rank <= max(2, len(net) // 4):
        score += 1; print(f"  [+] 转移熵:{focus} 净信息流出排名靠前 (第{rank}/{len(net)})")
    else:
        print(f"  [-] 转移熵:{focus} 净信息流出不突出 (第{rank}/{len(net)})")
    if n_lead >= len(gr) / 2:
        score += 1; print(f"  [+] Granger:{focus} 单向领先多数标的")
    else:
        print(f"  [-] Granger:{focus} 单向领先证据不足")
    print(f"\n  三项中 {score}/3 支持'{focus} 是领先指标'。")
    if score >= 2:
        print(f"  => 倾向支持:{focus} 在日线尺度有方向性领先迹象。")
    elif score == 0:
        print(f"  => 不支持:{focus} 更像对称的结构枢纽,或领先发生在日内(需上高频)。")
    else:
        print("  => 模糊:证据混合,建议上高频数据或换检验进一步确认。")


if __name__ == "__main__":
    main()
