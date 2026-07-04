# -*- coding: utf-8 -*-
"""QQQ 100日线专查: -53% MaxDD 和 CAGR 凹坑来自哪段行情?
拆解: 策略最大回撤窗口 / 分年度收益 / 每段持仓(交叉到交叉)的盈亏分布。"""
import pandas as pd
import numpy as np
import yfinance as yf

df = yf.download("QQQ", period="max", interval="1d", auto_adjust=True, progress=False)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)
c = df["Close"].dropna()
c.index = pd.to_datetime(c.index).tz_localize(None)
ret = c.pct_change().fillna(0)

def strat(n):
    ma = c.rolling(n).mean()
    pos = (c > ma).astype(float).shift(1).fillna(0)
    eq = (1 + pos * ret).cumprod()
    return pos, eq

print("== 策略最大回撤窗口 (发生在什么时候) ==")
for n in (50, 100, 150, 200):
    pos, eq = strat(n)
    dd = eq / eq.cummax() - 1
    ti = dd.idxmin()
    pk = eq[:ti].idxmax()
    print(f"  MA{n:>3}: MaxDD {dd.min()*100:6.1f}%  {pk.date()} -> {ti.date()}")

print("\n== 分年度收益: MA100 vs MA50 / MA200 / 买入持有 ==")
rows = {}
for n in (50, 100, 200):
    pos, eq = strat(n)
    rows[f"MA{n}"] = (1 + pos * ret).groupby(c.index.year).prod() - 1
rows["B&H"] = (1 + ret).groupby(c.index.year).prod() - 1
tbl = (pd.DataFrame(rows) * 100).round(1)
print(tbl.to_string())

print("\n== MA100 每段持仓盈亏 (交叉进->交叉出), 最差10段 ==")
pos, eq = strat(100)
grp = (pos.diff() != 0).cumsum()
segs = []
for g, idx in pos.groupby(grp).groups.items():
    if pos.loc[idx].iloc[0] == 1.0:
        r = (1 + ret.loc[idx]).prod() - 1
        segs.append((idx[0].date(), idx[-1].date(), len(idx), r * 100))
sd = pd.DataFrame(segs, columns=["in", "out", "days", "pnl%"]).sort_values("pnl%")
print(sd.head(10).to_string(index=False))
w = sd[sd["pnl%"] > 0]
print(f"\n段数 {len(sd)}, 胜率 {len(w)/len(sd)*100:.0f}%, "
      f"平均赢 {w['pnl%'].mean():.1f}%, 平均输 {sd[sd['pnl%']<=0]['pnl%'].mean():.1f}%")
print("\n== 各MA在 2000-2002 熊内的策略净值损耗 (鞭打成本) ==")
for n in (50, 100, 150, 200):
    pos, eq = strat(n)
    seg = eq["2000-03-27":"2002-10-09"]
    print(f"  MA{n:>3}: 熊内策略净值 {(seg.iloc[-1]/seg.iloc[0]-1)*100:6.1f}%  (同期QQQ -83%)")
