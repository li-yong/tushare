# -*- coding: utf-8 -*-
"""
熊市/牛市"确认"实证:
- 熊市开始:从最高点回撤多少 % (或离峰多少天) 时,历史上有多大概率最终跌满 -20% (经典熊市定义)?
- 牛市开始:从熊市最低点反弹多少 % (或多少天) 时,有多大概率底部已成 (不再创新低)?
用 SPY / QQQ 日线收盘价,yfinance 全历史。
"""
import sys
import pandas as pd
import numpy as np
import yfinance as yf

BEAR_DD = -0.20   # 经典熊市定义: 收盘从峰值 -20%
CORR_DD = -0.10   # 只统计至少到过 -10% 的回撤事件(小于10%的噪声不计为"事件")

def load(sym):
    df = yf.download(sym, period="max", interval="1d", auto_adjust=True, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    s = df["Close"].dropna()
    s.index = pd.to_datetime(s.index).tz_localize(None)
    return s

def episodes(close):
    """峰->谷->收复 的回撤事件。峰=运行最高收盘。
    事件从创新高后首次回撤开始,到收盘重新超过该峰值结束(或样本末尾)。"""
    peak = close.cummax()
    dd = close / peak - 1.0
    eps = []
    in_ep = False
    for i in range(len(close)):
        if not in_ep:
            if dd.iloc[i] < 0:
                in_ep = True
                start = i - 1 if i > 0 else 0   # 峰值日
                trough_i = i
        else:
            if close.iloc[i] < close.iloc[trough_i]:
                trough_i = i
            if dd.iloc[i] >= 0:  # 收复峰值,事件结束
                eps.append((start, trough_i, i))
                in_ep = False
    if in_ep:
        eps.append((start, trough_i, None))  # 未收复(可能是进行中)
    return eps, dd

def bear_confirm(close, sym):
    eps, dd = episodes(close)
    rows = []
    for (p, t, r) in eps:
        maxdd = close.iloc[t] / close.iloc[p] - 1.0
        if maxdd > CORR_DD:
            continue  # 没到 -10%,不算事件
        rows.append(dict(
            peak=close.index[p].date(), trough=close.index[t].date(),
            maxdd=maxdd, is_bear=maxdd <= BEAR_DD,
            days_to_trough=t - p,
            recovered=r is not None,
        ))
        # 各阈值首次触及的天数
        for th in (-0.05, -0.08, -0.10, -0.12, -0.15, -0.20):
            seg = close.iloc[p:t+1] / close.iloc[p] - 1.0
            hit = np.argmax(seg.values <= th) if (seg.values <= th).any() else None
            rows[-1][f"d{int(-th*100)}"] = hit
    ev = pd.DataFrame(rows)
    print(f"\n===== {sym} 熊市确认(峰值回撤事件, 至少-10%) =====")
    print(f"样本 {close.index[0].date()} ~ {close.index[-1].date()}, 事件数 {len(ev)}, 其中成熊(-20%+) {ev.is_bear.sum()}")
    with pd.option_context("display.width", 200):
        print(ev[["peak","trough","maxdd","is_bear","days_to_trough","d10","d15","d20"]]
              .assign(maxdd=lambda x: (x.maxdd*100).round(1)).to_string(index=False))
    # 条件概率: 已回撤到 X%,最终成熊的概率
    print("\n-- 回撤深度 -> 最终成熊(-20%)的条件概率 --")
    for th in (-0.10, -0.12, -0.15, -0.18):
        sub = ev[ev.maxdd <= th]
        n = len(sub); k = sub.is_bear.sum()
        print(f"  跌到 {th*100:.0f}% : {k}/{n} = {k/n*100:.0f}% 最终成熊")
    # 成熊事件里, -15% -> -20% 的中位天数; 峰值 -> -20% 天数
    bears = ev[ev.is_bear]
    if len(bears):
        print("\n-- 成熊事件的时间结构 (交易日) --")
        print(f"  峰值->首次-10%: 中位 {bears.d10.median():.0f} (范围 {bears.d10.min():.0f}~{bears.d10.max():.0f})")
        print(f"  峰值->首次-15%: 中位 {bears.d15.median():.0f} (范围 {bears.d15.min():.0f}~{bears.d15.max():.0f})")
        print(f"  峰值->首次-20%(官方确认): 中位 {bears.d20.median():.0f} (范围 {bears.d20.min():.0f}~{bears.d20.max():.0f})")
    # 非熊事件(修正)最终深度分布
    corr = ev[~ev.is_bear]
    print(f"\n-- 未成熊的修正: 最深回撤分布: 中位 {corr.maxdd.median()*100:.1f}%, 最深 {corr.maxdd.min()*100:.1f}% --")
    # 纯时间维度: 离峰 N 天仍未收复 -> 成熊概率
    print("\n-- 离峰天数(仍低于峰值) -> 最终成熊概率 --")
    for nd in (20, 40, 60, 90, 120):
        # 事件持续超过 nd 天(峰后 nd 天仍未收复)
        dur = ev.apply(lambda x: True, axis=1)
        sub = []
        for (p, t, r) in eps:
            maxdd_ = close.iloc[t]/close.iloc[p]-1.0
            if maxdd_ > CORR_DD: continue
            end = r if r is not None else len(close)-1
            if end - p >= nd:
                sub.append(maxdd_ <= BEAR_DD)
        if sub:
            print(f"  峰后{nd}天未收复: {sum(sub)}/{len(sub)} = {sum(sub)/len(sub)*100:.0f}% 成熊")
    return ev, eps

def bull_confirm(close, sym, eps):
    """牛市确认: 熊市(-20%)事件内,从(事后已知的)每个局部低点反弹 X% 时,
    该低点是否就是最终底(之后不再创更低)?
    做法: 在每个成熊事件内逐日模拟: 维护运行低点, 当收盘较运行低点反弹 >= th 时记一次'确认信号',
    检查之后是否创出更低 -> 假信号。统计各阈值的假信号率与确认延迟。"""
    print(f"\n===== {sym} 牛市确认(熊市内反弹阈值) =====")
    rows = []
    for (p, t, r) in eps:
        maxdd = close.iloc[t]/close.iloc[p]-1.0
        if maxdd > BEAR_DD:
            continue
        end = r if r is not None else len(close)-1
        seg = close.iloc[p:end+1]
        true_low_i = t - p  # 事件内索引
        for th in (0.05, 0.08, 0.10, 0.15, 0.20):
            run_low = seg.iloc[0]; run_low_i = 0
            armed = True   # 每创一次新低后, 只在首次反弹>=th时记一次信号
            signals = []
            for i in range(1, len(seg)):
                if seg.iloc[i] < run_low:
                    run_low = seg.iloc[i]; run_low_i = i; armed = True
                elif armed and seg.iloc[i] / run_low - 1.0 >= th:
                    signals.append((i, run_low_i)); armed = False
            n_false = sum(1 for (si, li) in signals if li != true_low_i)
            true_sig = [(si, li) for (si, li) in signals if li == true_low_i]
            delay = true_sig[0][0] - true_low_i if true_sig else None
            # 确认时已从底部涨了多少
            rise_at = seg.iloc[true_sig[0][0]] / seg.iloc[true_low_i] - 1.0 if true_sig else None
            rows.append(dict(sym=sym, peak=close.index[p].date(), th=th,
                             n_false=n_false, confirmed=bool(true_sig),
                             delay_days=delay, rise_at_confirm=rise_at))
    df = pd.DataFrame(rows)
    for th in (0.05, 0.08, 0.10, 0.15, 0.20):
        sub = df[df.th == th]
        if not len(sub): continue
        conf = sub[sub.confirmed]
        print(f"  反弹+{th*100:.0f}%确认: 假信号总数 {sub.n_false.sum()} (每熊平均 {sub.n_false.mean():.1f}), "
              f"真底确认延迟 中位 {conf.delay_days.median():.0f} 天"
              + (f", 范围 {conf.delay_days.min():.0f}~{conf.delay_days.max():.0f}" if len(conf) else ""))
    # 明细: 每个熊市各阈值假信号
    with pd.option_context("display.width", 200):
        piv = df.pivot_table(index="peak", columns="th", values="n_false", aggfunc="sum")
        piv.columns = [f"+{int(c*100)}%假信号" for c in piv.columns]
        print(piv.to_string())
    return df

def ma200_check(close, sym, eps):
    """对照: 收盘跌破200日均线作为'熊市确认'的表现(用户系统的脊梁)。"""
    ma = close.rolling(200).mean()
    print(f"\n-- 对照: {sym} 收盘首次跌破200日线 vs 事件结局 --")
    for (p, t, r) in eps:
        maxdd = close.iloc[t]/close.iloc[p]-1.0
        if maxdd > CORR_DD: continue
        end = r if r is not None else len(close)-1
        seg_c = close.iloc[p:end+1]; seg_m = ma.iloc[p:end+1]
        below = seg_c < seg_m
        first = np.argmax(below.values) if below.any() else None
        dd_at = seg_c.iloc[first]/close.iloc[p]-1.0 if first is not None else None
        tag = "熊" if maxdd <= BEAR_DD else "修正"
        if first is not None:
            print(f"  {close.index[p].date()} [{tag} {maxdd*100:.0f}%]: 峰后{first}天破200线, 当时回撤{dd_at*100:.1f}%")
        else:
            print(f"  {close.index[p].date()} [{tag} {maxdd*100:.0f}%]: 全程未破200线")

for sym in ("SPY", "QQQ"):
    c = load(sym)
    ev, eps = bear_confirm(c, sym)
    bull_confirm(c, sym, eps)
    ma200_check(c, sym, eps)
