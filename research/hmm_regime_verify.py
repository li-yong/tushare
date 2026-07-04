# -*- coding: utf-8 -*-
"""HMM 体制识别 研究+验证 (SPY/QQQ 日线, 高斯HMM on 对数收益率)

验证要点:
1) 前视偏差: 教科书演示 = 全样本 Baum-Welch 训练 + 全样本平滑/Viterbi 解码,
   等于用未来给过去贴标签。这里做严格 walk-forward 对照:
   - 每 63 个交易日(季度)用截至当日的扩展窗重新 EM 拟合;
   - 日常解码只用 forward 滤波概率 P(S_t | O_1..O_t) (自实现, 无 backward=无未来);
   - 当日收盘出信号, 次日生效 (shift 1)。
2) 规则: (a) 预测下一日期望收益 >0 做多: E[r_{t+1}] = (p_t A) · mu
        (b) 教科书规则: 最高方差状态的滤波概率 <0.5 做多
3) 基线: 买入持有 / 200日线, 全部对齐到 walk-forward 可交易的同一评估窗。
4) EM 局部最优敏感性: 全样本 K=2 换 5 个随机种子看参数漂移。

用法: python research/hmm_regime_verify.py  (联网, 全跑约几分钟)
"""
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm
from hmmlearn.hmm import GaussianHMM

MIN_OBS = 1000        # 首次拟合最少观测 (~4年)
REFIT = 63            # 每季度重拟合
SCALE = 100.0         # 收益率×100, EM 数值稳定
BEAR_PEAKS = {
    "SPY": ["2000-03-24", "2007-10-09", "2020-02-19", "2022-01-03"],
    "QQQ": ["2000-03-27", "2018-08-29", "2020-02-19", "2021-12-27", "2025-02-19"],
}

def load(sym):
    df = yf.download(sym, period="max", interval="1d", auto_adjust=True, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].dropna()
    s.index = pd.to_datetime(s.index).tz_localize(None)
    return s

def fit_hmm(x, k, seed=7):
    m = GaussianHMM(n_components=k, covariance_type="diag", n_iter=200,
                    tol=1e-4, random_state=seed)
    m.fit(x.reshape(-1, 1))
    return (m.startprob_.copy(), m.transmat_.copy(),
            m.means_.ravel().copy(), np.sqrt(m.covars_.ravel()).copy())

def forward_filtered(x, pi, A, mu, sd):
    """滤波概率 P(S_t|O_1..O_t), (T,K)。只向前递推, 无未来信息。"""
    B = norm.pdf(x[:, None], mu[None, :], sd[None, :]) + 1e-300
    out = np.empty((len(x), len(mu)))
    a = pi * B[0]; a /= a.sum(); out[0] = a
    for t in range(1, len(x)):
        a = (a @ A) * B[t]; a /= a.sum(); out[t] = a
    return out

def walkforward_signals(x, k, seed=7):
    """返回 (signal_a, signal_b) ∈{0,1}, 长度=len(x), 前 MIN_OBS 天为 nan。"""
    T = len(x)
    sig_a = np.full(T, np.nan); sig_b = np.full(T, np.nan)
    for fit_end in range(MIN_OBS, T, REFIT):
        blk_end = min(fit_end + REFIT, T)
        pi, A, mu, sd = fit_hmm(x[:fit_end], k, seed)
        filt = forward_filtered(x[:blk_end], pi, A, mu, sd)[fit_end:blk_end]
        pred_mu = (filt @ A) @ mu                    # 规则a: 一步预测期望
        sig_a[fit_end:blk_end] = (pred_mu > 0).astype(float)
        hi = int(np.argmax(sd))                      # 规则b: 最高方差=风险态
        sig_b[fit_end:blk_end] = (filt[:, hi] < 0.5).astype(float)
    return sig_a, sig_b

def insample_signal(x, k, seed=7):
    """前视版: 全样本拟合 + 全样本平滑概率 (forward-backward, 用了未来)。"""
    m = GaussianHMM(n_components=k, covariance_type="diag", n_iter=200,
                    tol=1e-4, random_state=seed)
    m.fit(x.reshape(-1, 1))
    post = m.predict_proba(x.reshape(-1, 1))
    hi = int(np.argmax(m.covars_.ravel()))
    return (post[:, hi] < 0.5).astype(float)

def perf(close, signal, eval_mask, cost_bps=0.0):
    """signal: 当日收盘信号(0/1), 次日生效。全部指标只在 eval_mask 内计。"""
    ret = close.pct_change().fillna(0).values
    pos = pd.Series(signal, index=close.index).shift(1).fillna(0).values
    turn = np.abs(np.diff(np.nan_to_num(pos), prepend=0))
    r = (pos * ret - turn * cost_bps / 1e4)[eval_mask]
    idx = close.index[eval_mask]
    eq = np.cumprod(1 + np.nan_to_num(r))
    yrs = (idx[-1] - idx[0]).days / 365.25
    cagr = eq[-1] ** (1 / yrs) - 1
    mdd = (eq / np.maximum.accumulate(eq) - 1).min()
    s = np.nan_to_num(signal)[eval_mask]
    switches = np.abs(np.diff(s)).sum() / yrs
    return cagr * 100, mdd * 100, switches, s.mean() * 100

def bear_exits(close, signal, peaks, eval_start):
    """每次真熊: 峰后信号首次转0时, 距峰值回撤%。峰在评估窗外记 '--'。"""
    out = []
    sig = pd.Series(signal, index=close.index)
    for pk in peaks:
        p = close.index.get_indexer([pd.Timestamp(pk)], method="nearest")[0]
        if close.index[p] < eval_start:
            out.append("   --"); continue
        seg = sig.iloc[p:p + 500]
        flat = (seg == 0).values
        if flat.any():
            i = int(np.argmax(flat))
            out.append(f"{(close.iloc[p+i]/close.iloc[p]-1)*100:5.1f}")
        else:
            out.append("未离场")
    return " ".join(out)

for sym in ("SPY", "QQQ"):
    c = load(sym)
    lr = (np.log(c).diff().dropna() * SCALE)
    x = lr.values
    cx = c.loc[lr.index]                      # 与收益率对齐的收盘价
    eval_start_i = MIN_OBS
    eval_mask = np.zeros(len(cx), bool); eval_mask[eval_start_i:] = True
    eval_start = cx.index[eval_start_i]
    print(f"\n===== {sym} 评估窗 {eval_start.date()} ~ {cx.index[-1].date()} "
          f"(前{MIN_OBS}天只做训练) =====")

    rows = []
    # 基线
    bh = np.ones(len(cx))
    ma = cx.rolling(200).mean(); ma_sig = (cx > ma).astype(float).values
    rows.append(("买入持有", bh))
    rows.append(("MA200", ma_sig))
    # walk-forward HMM
    for k in (2, 3):
        sa, sb = walkforward_signals(x, k)
        rows.append((f"HMM K{k} WF 规则a(预测μ>0)", sa))
        rows.append((f"HMM K{k} WF 规则b(P高波<.5)", sb))
    # 前视版
    rows.append(("HMM K2 全样本平滑[前视!]", insample_signal(x, 2)))

    print(f"{'策略':<26} {'CAGR%':>7} {'@10bp':>7} {'MaxDD%':>7} {'切换/年':>7} {'在场%':>6}  真熊离场回撤%")
    for name, sig in rows:
        cagr0, mdd, sw, tim = perf(cx, sig, eval_mask, 0)
        cagr1, _, _, _ = perf(cx, sig, eval_mask, 10)
        be = bear_exits(cx, sig, BEAR_PEAKS[sym], eval_start)
        pad = "　" * max(0, 15 - sum(1 for ch in name if ord(ch) > 255)) # 粗对齐
        print(f"{name:<26} {cagr0:>7.2f} {cagr1:>7.2f} {mdd:>7.1f} {sw:>7.1f} {tim:>6.1f}  {be}")

    # EM 种子敏感性 (全样本 K=2)
    print(f"-- {sym} K=2 全样本拟合, 5个种子的参数 (按σ升序: 低波态/高波态) --")
    for seed in range(5):
        pi, A, mu, sd = fit_hmm(x, 2, seed)
        o = np.argsort(sd)
        print(f"  seed{seed}: μ=({mu[o[0]]:+.3f},{mu[o[1]]:+.3f})%/日 "
              f"σ=({sd[o[0]]:.2f},{sd[o[1]]:.2f})% "
              f"自持概率=({A[o[0],o[0]]:.3f},{A[o[1],o[1]]:.3f})")
