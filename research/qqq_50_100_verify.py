# -*- coding: utf-8 -*-
"""验证: QQQ 上 50/100 日线是否稳健地优于 200 日线?
1) 交易成本敏感性 (0/5/10/20 bps 单边)
2) 分时段 CAGR (是不是全靠 2000-02)
3) 排除 2000-02 后的全期表现
4) 滚动5年窗口胜率 (MA50/MA100 vs MA200)
5) 真熊保险质量回顾
"""
import pandas as pd
import numpy as np
import yfinance as yf

df = yf.download("QQQ", period="max", interval="1d", auto_adjust=True, progress=False)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)
c = df["Close"].dropna()
c.index = pd.to_datetime(c.index).tz_localize(None)
ret = c.pct_change().fillna(0)

def strat_ret(n, cost_bps=0.0):
    ma = c.rolling(n).mean()
    pos = (c > ma).astype(float).shift(1).fillna(0)
    turn = pos.diff().abs().fillna(0)          # 进出各计一次
    return pos * ret - turn * cost_bps / 1e4

def cagr(r):
    eq = (1 + r).cumprod()
    yrs = (r.index[-1] - r.index[0]).days / 365.25
    return (eq.iloc[-1] ** (1 / yrs) - 1) * 100

def maxdd(r):
    eq = (1 + r).cumprod()
    return (eq / eq.cummax() - 1).min() * 100

NS = (50, 100, 200)

print("== 1) 交易成本敏感性: CAGR% (全期 1999-2026) ==")
print(f"{'成本bps':>8} {'MA50':>7} {'MA100':>7} {'MA200':>7} {'B&H':>7}")
for bps in (0, 5, 10, 20):
    row = [cagr(strat_ret(n, bps)) for n in NS]
    print(f"{bps:>8} {row[0]:>7.2f} {row[1]:>7.2f} {row[2]:>7.2f} {cagr(ret):>7.2f}")

print("\n== 2) 分时段 CAGR% (0成本) ==")
periods = [("1999-2002 泡沫+熊", "1999-03-10", "2002-12-31"),
           ("2003-2007 复苏牛", "2003-01-01", "2007-12-31"),
           ("2008-2009 金融危机", "2008-01-01", "2009-12-31"),
           ("2010-2019 长牛", "2010-01-01", "2019-12-31"),
           ("2020-2026 疫情后", "2020-01-01", "2026-12-31")]
print(f"{'时段':<22} {'MA50':>7} {'MA100':>7} {'MA200':>7} {'B&H':>7}")
for name, a, b in periods:
    sl = slice(a, b)
    row = [cagr(strat_ret(n)[sl]) for n in NS]
    print(f"{name:<20} {row[0]:>7.2f} {row[1]:>7.2f} {row[2]:>7.2f} {cagr(ret[sl]):>7.2f}")

print("\n== 3) 排除 2000-2002 (把该段收益置0=当时空仓旁观) ==")
mask = ~((c.index >= "2000-03-27") & (c.index <= "2002-12-31"))
print(f"{'':<8} {'MA50':>7} {'MA100':>7} {'MA200':>7}")
r0 = [cagr(strat_ret(n).where(mask, 0.0)) for n in NS]
print(f"{'CAGR%':<8} {r0[0]:>7.2f} {r0[1]:>7.2f} {r0[2]:>7.2f}")
m0 = [maxdd(strat_ret(n).where(mask, 0.0)) for n in NS]
print(f"{'MaxDD%':<8} {m0[0]:>7.1f} {m0[1]:>7.1f} {m0[2]:>7.1f}")

print("\n== 4) 滚动5年窗口: MA50/MA100 相对 MA200 的胜率 ==")
rs = {n: strat_ret(n) for n in NS}
eqs = {n: (1 + rs[n]).cumprod() for n in NS}
win50 = win100 = tot = 0
for start in pd.date_range(c.index[0], c.index[-1] - pd.DateOffset(years=5), freq="MS"):
    end = start + pd.DateOffset(years=5)
    w = {}
    for n in NS:
        seg = eqs[n][start:end]
        if len(seg) < 100: break
        w[n] = seg.iloc[-1] / seg.iloc[0]
    if len(w) < 3: continue
    tot += 1
    win50 += w[50] > w[200]
    win100 += w[100] > w[200]
print(f"  {tot} 个滚动窗口: MA50 胜 MA200 {win50/tot*100:.0f}%, MA100 胜 MA200 {win100/tot*100:.0f}%")

print("\n== 4b) 同样加 10bps 成本后的滚动胜率 ==")
rs = {n: strat_ret(n, 10) for n in NS}
eqs = {n: (1 + rs[n]).cumprod() for n in NS}
win50 = win100 = tot = 0
for start in pd.date_range(c.index[0], c.index[-1] - pd.DateOffset(years=5), freq="MS"):
    end = start + pd.DateOffset(years=5)
    w = {}
    for n in NS:
        seg = eqs[n][start:end]
        if len(seg) < 100: break
        w[n] = seg.iloc[-1] / seg.iloc[0]
    if len(w) < 3: continue
    tot += 1
    win50 += w[50] > w[200]
    win100 += w[100] > w[200]
print(f"  {tot} 个滚动窗口: MA50 胜 MA200 {win50/tot*100:.0f}%, MA100 胜 MA200 {win100/tot*100:.0f}%")

print("\n== 5) 体制开关视角: 状态切换频率 与 真熊离场点 ==")
BEAR_PEAKS = ["2000-03-27", "2018-08-29", "2020-02-19", "2021-12-27", "2025-02-19"]
for n in NS:
    ma = c.rolling(n).mean()
    above = (c > ma).astype(int)
    yrs = (c.index[-1] - c.index[0]).days / 365.25
    cr = int(above.diff().abs().sum()) / yrs
    exits = []
    for pk in BEAR_PEAKS:
        p = c.index.get_indexer([pd.Timestamp(pk)], method="nearest")[0]
        seg_c, seg_m = c.iloc[p:p+500], ma.iloc[p:p+500]
        below = (seg_c < seg_m).values
        exits.append((seg_c.iloc[int(np.argmax(below))] / c.iloc[p] - 1) * 100 if below.any() else np.nan)
    print(f"  MA{n:>3}: 切换 {cr:4.1f} 次/年, 真熊破线回撤: " + " ".join(f"{e:5.1f}" for e in exits))
