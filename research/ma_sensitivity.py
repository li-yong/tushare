# -*- coding: utf-8 -*-
"""
MA 长度敏感性: 200日线是魔法数字还是平台上的任意点?
规则: 收盘 > MA 次日持有, 否则空仓 (最简体制开关, 无粘滞带)。
指标: CAGR / 最大回撤 / 交叉(鞭打)次数/年 / 在场时间 / 每次真熊里破线时的回撤深度。
长度网格覆盖 199 vs 200, 以及 31 天 (≈200小时: 200/6.5) 的代理。
"""
import pandas as pd
import numpy as np
import yfinance as yf

def load(sym):
    df = yf.download(sym, period="max", interval="1d", auto_adjust=True, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].dropna()
    s.index = pd.to_datetime(s.index).tz_localize(None)
    return s

BEARS = {  # 峰值日 (前一分析得到的真熊, 收盘口径)
    "SPY": ["2000-03-24", "2007-10-09", "2020-02-19", "2022-01-03"],
    "QQQ": ["2000-03-27", "2018-08-29", "2020-02-19", "2021-12-27", "2025-02-19"],
}

def evaluate(close, n):
    ma = close.rolling(n).mean()
    pos = (close > ma).astype(float).shift(1).fillna(0)
    ret = close.pct_change().fillna(0)
    stancer = pos * ret
    eq = (1 + stancer).cumprod()
    yrs = (close.index[-1] - close.index[0]).days / 365.25
    cagr = eq.iloc[-1] ** (1 / yrs) - 1
    mdd = (eq / eq.cummax() - 1).min()
    above = (close > ma).astype(int)
    crosses = int(above.diff().abs().sum())
    return cagr, mdd, crosses / yrs, pos.mean()

def bear_exit_dd(close, n, peaks):
    """每次真熊: 峰后首次收盘破MA时, 距峰值的回撤 (%). 越浅越好=保险越早生效."""
    ma = close.rolling(n).mean()
    out = []
    for pk in peaks:
        p = close.index.get_indexer([pd.Timestamp(pk)], method="nearest")[0]
        seg_c, seg_m = close.iloc[p:p+500], ma.iloc[p:p+500]
        below = (seg_c < seg_m).values
        if below.any():
            i = int(np.argmax(below))
            out.append((seg_c.iloc[i] / close.iloc[p] - 1) * 100)
        else:
            out.append(np.nan)
    return out

GRID = [20, 31, 50, 100, 150, 180, 190, 199, 200, 201, 210, 230, 250, 300]

for sym in ("SPY", "QQQ"):
    c = load(sym)
    ry = c.pct_change().fillna(0)
    eq_bh = (1 + ry).cumprod()
    yrs = (c.index[-1] - c.index[0]).days / 365.25
    print(f"\n===== {sym} ({c.index[0].date()}~{c.index[-1].date()}) "
          f"买入持有: CAGR {(eq_bh.iloc[-1]**(1/yrs)-1)*100:.1f}%, "
          f"MaxDD {((eq_bh/eq_bh.cummax()-1).min())*100:.0f}% =====")
    print(f"{'MA':>4} {'CAGR%':>7} {'MaxDD%':>7} {'鞭打/年':>7} {'在场%':>6}  真熊破线时回撤%")
    for n in GRID:
        cagr, mdd, cpy, tim = evaluate(c, n)
        dds = bear_exit_dd(c, n, BEARS[sym])
        dds_s = " ".join(f"{d:5.1f}" for d in dds)
        tag = " <== 200h代理" if n == 31 else (" <== 惯例" if n == 200 else "")
        print(f"{n:>4} {cagr*100:>7.2f} {mdd*100:>7.1f} {cpy:>7.1f} {tim*100:>6.1f}  {dds_s}{tag}")

# 真·200小时线: yfinance 1h 只回溯 ~730 天, 只能对照近两年鞭打频率
print("\n===== 真200小时线 vs 200日线: 近730天鞭打次数对照 =====")
for sym in ("SPY", "QQQ"):
    h = yf.download(sym, period="730d", interval="1h", auto_adjust=True, progress=False)
    if isinstance(h.columns, pd.MultiIndex):
        h.columns = h.columns.get_level_values(0)
    hc = h["Close"].dropna()
    hma = hc.rolling(200).mean()
    hcross = int((hc > hma).astype(int).diff().abs().sum())
    d = load(sym)
    d2 = d[d.index >= hc.index[0].tz_localize(None)]
    dma = d.rolling(200).mean()[d.index >= hc.index[0].tz_localize(None)]
    dcross = int((d2 > dma).astype(int).diff().abs().sum())
    print(f"  {sym}: 200小时线交叉 {hcross} 次, 200日线交叉 {dcross} 次 (同一时段)")
