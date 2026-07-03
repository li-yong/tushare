#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""t_us_return_concentration.py — 收益结构实证 (一次性研究脚本)

假设 (用户 2026-06): 股票收益主要来自少数几次爆发上涨 / 跳空高开,
大部分时间在小幅震荡。本脚本在 SP500∪NDX 上量化这个假设到底强不强,
并反推 gap 阈值该卡在哪。

数据源: yfinance only (ADR-0001, 与全系统同源)。复用 t_us_undervalue.load_universe。

三块输出:
  ①集中度    剔除 top-N% 交易日后, 买入持有收益的衰减 (踏空最好几天检验)。
  ②隔夜/日内 每日收益拆成跳空段 g=open/prev_close-1 与日内段 d=close/open-1,
             看累计对数收益里隔夜占多少 (呼应 overnight-vs-intraday 笔记)。
  ③gap 阈值  按跳空大小分桶, 算每桶"成为大涨日"的精度与相对基率 lift。

用法:
  python t_us_return_concentration.py                       # both, 5y, 全池
  python t_us_return_concentration.py --universe ndx --years 3
  python t_us_return_concentration.py --limit 50            # 抽样跑快点
  python t_us_return_concentration.py --force               # 忽略当日缓存重抓
出力:
  控制台报告 + result/us_return_concentration_<date>.txt (若 result/ 存在)
  per-ticker 明细 → result/us_return_concentration_<date>.csv
"""
import os
import sys
import json
import pickle
import logging
import datetime
import warnings
from optparse import OptionParser

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('ret_conc')

SCRATCH = '/tmp/claude-1000/-home-ryan-tushare-ryan/ce77ce2e-57c9-4b95-a925-72e74e214894/scratchpad'
RESULT_DIR = '/home/ryan/DATA/result'
TODAY = datetime.date.today().isoformat()

# “大涨日”阈值: 单日收益进入该股自身分布的 top 1% (相对口径, 跨股可比)。
BIG_DAY_PCTL = 99.0
# gap 分桶边界 (开盘相对昨收, %)。
GAP_BUCKETS = [-100, -3, -1, 0, 1, 2, 3, 5, 100]


def download_ohlc(tickers, years, force):
    """批量下载 OHLC (auto_adjust), 缓存到 scratch pickle (当日键)。
    返回 {ticker: DataFrame[Open,High,Low,Close]}。"""
    import yfinance as yf
    key = f'ohlc_{len(tickers)}_{years}y_{TODAY}'
    cache = os.path.join(SCRATCH, key + '.pkl')
    if not force and os.path.exists(cache):
        log.info(f'OHLC 当日缓存命中 → {cache}')
        with open(cache, 'rb') as fh:
            return pickle.load(fh)

    out = {}
    period = f'{years}y'
    # 分批, 避免一次请求太多被限流。
    CHUNK = 80
    for i in range(0, len(tickers), CHUNK):
        batch = tickers[i:i + CHUNK]
        log.info(f'下载 {i + 1}-{i + len(batch)} / {len(tickers)} ...')
        data = yf.download(batch, period=period, auto_adjust=True,
                           progress=False, group_by='ticker', threads=True)
        for t in batch:
            try:
                df = data[t] if len(batch) > 1 else data
                df = df[['Open', 'High', 'Low', 'Close']].dropna()
            except Exception:
                continue
            if len(df) < 250:                      # 不足 ~1 年, 跳过
                continue
            out[t] = df
    log.info(f'有效 OHLC {len(out)} / {len(tickers)} 只')
    try:
        os.makedirs(SCRATCH, exist_ok=True)
        with open(cache, 'wb') as fh:
            pickle.dump(out, fh)
    except Exception as e:
        log.warning(f'缓存写入失败 {e}')
    return out


def analyze_ticker(df):
    """单只票的收益结构。返回 dict 指标, 数据不足返回 None。"""
    o = df['Open'].to_numpy(float)
    c = df['Close'].to_numpy(float)
    prev_c = c[:-1]
    o, c = o[1:], c[1:]                            # 对齐到“有昨收”的日子
    if len(c) < 250 or (prev_c <= 0).any():
        return None

    r = c / prev_c - 1.0                           # 全日简单收益
    g = o / prev_c - 1.0                           # 跳空段 (隔夜)
    d = c / o - 1.0                                # 日内段
    n = len(r)

    # ── ①集中度: 剔除 top-N% 收益日后的累计衰减 ──
    log1p_r = np.log1p(r)
    total_log = log1p_r.sum()                       # 全程对数收益
    order = np.argsort(r)[::-1]                      # 收益从大到小
    decay = {}
    for pct in (0.5, 1, 2, 5):
        k = max(1, int(round(n * pct / 100)))
        kept = total_log - log1p_r[order[:k]].sum()  # 剔掉最好的 k 天
        decay[pct] = kept
    # top 1% 天贡献了多少 (占全程对数收益比例)
    k1 = max(1, int(round(n * 0.01)))
    top1_contrib = log1p_r[order[:k1]].sum() / total_log if total_log > 0 else np.nan

    # ── ②隔夜 vs 日内: 累计对数收益拆分 ──
    sum_log_g = np.log1p(g).sum()
    sum_log_d = np.log1p(d).sum()
    overnight_share = sum_log_g / (sum_log_g + sum_log_d) if (sum_log_g + sum_log_d) != 0 else np.nan

    # ── ③gap 阈值: 大涨日 vs gap 分桶 ──
    big_thr = np.percentile(r, BIG_DAY_PCTL)
    is_big = r >= big_thr
    base_rate = is_big.mean()                        # 该股大涨日基率 (≈1%)
    # 大涨日里跳空高开的占比 / 中位 gap
    big_gap = g[is_big]
    gapup_among_big = (big_gap > 0).mean() if len(big_gap) else np.nan
    med_gap_big = np.median(big_gap) if len(big_gap) else np.nan
    # 每个 gap 桶: 样本数、命中大涨日数 (用于全局聚合精度/lift)
    bucket_tot = np.zeros(len(GAP_BUCKETS) - 1)
    bucket_big = np.zeros(len(GAP_BUCKETS) - 1)
    bucket_intraday = np.zeros(len(GAP_BUCKETS) - 1)  # 该桶日内段收益和 (gap后是否续涨)
    gpct = g * 100
    idx = np.digitize(gpct, GAP_BUCKETS) - 1
    idx = np.clip(idx, 0, len(GAP_BUCKETS) - 2)
    for b in range(len(GAP_BUCKETS) - 1):
        m = idx == b
        bucket_tot[b] = m.sum()
        bucket_big[b] = is_big[m].sum()
        bucket_intraday[b] = d[m].sum()

    return {
        'n_days': n,
        'cagr_logret': total_log,
        'decay': decay,
        'top1_contrib': top1_contrib,
        'overnight_share': overnight_share,
        'base_rate': base_rate,
        'gapup_among_big': gapup_among_big,
        'med_gap_big': med_gap_big,
        'bucket_tot': bucket_tot,
        'bucket_big': bucket_big,
        'bucket_intraday': bucket_intraday,
    }


def fmt_pct(x):
    return f'{x * 100:6.1f}%' if x is not None and not (isinstance(x, float) and np.isnan(x)) else '   n/a'


def main():
    p = OptionParser()
    p.add_option('--universe', default='both', help='sp500 | ndx | both (default both)')
    p.add_option('--years', type='int', default=5, help='回看年数 (default 5)')
    p.add_option('--limit', type='int', default=0, help='只取前 N 只 (调试提速; 0=全池)')
    p.add_option('--force', action='store_true', help='忽略当日缓存, 重抓')
    opts, _ = p.parse_args()

    from t_us_undervalue import load_universe
    tickers = load_universe(opts.universe, opts.force)
    if not tickers:
        log.error('股票池为空 — 检查网络/Wikipedia 或 --universe。')
        sys.exit(1)
    if opts.limit:
        tickers = tickers[:opts.limit]
    # yfinance 用连字符 (BRK.B → BRK-B)
    tickers = [t.replace('.', '-') for t in tickers]

    ohlc = download_ohlc(tickers, opts.years, opts.force)
    rows, agg = [], []
    for t, df in ohlc.items():
        m = analyze_ticker(df)
        if m is None:
            continue
        m['ticker'] = t
        agg.append(m)
        rows.append({
            'ticker': t, 'n_days': m['n_days'],
            'total_logret': round(m['cagr_logret'], 3),
            'ret_no_top1pct': round(m['decay'][1], 3),
            'top1pct_contrib': round(m['top1_contrib'], 3),
            'overnight_share': round(m['overnight_share'], 3),
            'gapup_among_big': round(m['gapup_among_big'], 3),
            'med_gap_big_%': round(m['med_gap_big'] * 100, 2),
        })
    if not agg:
        log.error('无有效样本。')
        sys.exit(1)

    N = len(agg)
    # ── 聚合 ──
    def med(key, sub=None):
        vals = [(a[key] if sub is None else a[key][sub]) for a in agg]
        vals = [v for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
        return float(np.median(vals)) if vals else float('nan')

    lines = []
    P = lines.append
    P('═' * 72)
    P(f'收益结构实证 · {opts.universe.upper()} · {opts.years}y · {N} 只有效  ({TODAY})')
    P('假设: 收益集中在少数爆发/跳空, 大部分时间小幅震荡')
    P('═' * 72)

    P('\n① 集中度 — 剔除最好的几天后, 累计对数收益还剩多少 (中位数)')
    full = med('cagr_logret')
    P(f'    全程 (买入持有)            {full:7.3f}   (= 100% 基准)')
    for pct in (0.5, 1, 2, 5):
        kept = med('decay', pct)
        share = kept / full if full else float('nan')
        P(f'    剔除 top {pct:>3}% 交易日       {kept:7.3f}   剩 {fmt_pct(share)}')
    P(f'    → top 1% 交易日贡献了全程收益的 中位 {fmt_pct(med("top1_contrib"))}')
    P('    解读: 剩余越低 = 收益越集中在少数日 → 假设越成立 (不能踏空)')

    P('\n② 隔夜 vs 日内 — 累计对数收益里, 跳空段(隔夜)占比 (中位数)')
    os_share = med('overnight_share')
    P(f'    隔夜(gap)段占比             {fmt_pct(os_share)}')
    P(f'    日内段占比                 {fmt_pct(1 - os_share)}')
    P('    解读: 隔夜占比高 → 方向信息在隔夜/跳空, 印证 overnight-vs-intraday')

    P('\n③ gap 阈值 — 大涨日(各股 top 1%)与跳空高开的关系')
    P(f'    大涨日中"跳空高开"占比      中位 {fmt_pct(med("gapup_among_big"))}')
    P(f'    大涨日的 中位跳空幅度        {med("med_gap_big") * 100:+.2f}%')
    # 全局分桶聚合: 精度 = P(大涨日 | gap∈桶), lift = 精度/总体基率
    tot = np.sum([a['bucket_tot'] for a in agg], axis=0)
    big = np.sum([a['bucket_big'] for a in agg], axis=0)
    intr = np.sum([a['bucket_intraday'] for a in agg], axis=0)
    overall_base = big.sum() / tot.sum() if tot.sum() else float('nan')
    P(f'\n    按跳空幅度分桶 (全样本 {int(tot.sum()):,} 个交易日, 总体大涨基率 {fmt_pct(overall_base)}):')
    P('    gap 区间        样本占比   P(大涨日)   lift     桶内日内段均收益')
    P('    ' + '-' * 64)
    for b in range(len(GAP_BUCKETS) - 1):
        lo, hi = GAP_BUCKETS[b], GAP_BUCKETS[b + 1]
        label = f'[{lo:+d}%,{hi:+d}%)' if abs(lo) < 100 and abs(hi) < 100 else \
                (f'<{hi:+d}%' if lo <= -100 else f'≥{lo:+d}%')
        if tot[b] == 0:
            continue
        prec = big[b] / tot[b]
        lift = prec / overall_base if overall_base else float('nan')
        samp = tot[b] / tot.sum()
        intraday_mean = intr[b] / tot[b]
        P(f'    {label:14} {samp * 100:6.1f}%   {prec * 100:6.2f}%   {lift:5.1f}x   {intraday_mean * 100:+7.3f}%')
    P('    解读: lift 在哪个桶开始陡升 = gap 阈值该卡哪;')
    P('          日内段均收益>0 = 跳空后当日仍续涨(gap-confirmation 偏多)')
    P('═' * 72)

    report = '\n'.join(lines)
    print('\n' + report)

    # 落盘 (result/ 存在才写, 与系统其它脚本一致)
    if os.path.isdir(RESULT_DIR):
        out_dir = os.path.join(RESULT_DIR, 'us_return_concentration')
        os.makedirs(out_dir, exist_ok=True)
        txt = os.path.join(out_dir, f'us_return_concentration_{TODAY}.txt')
        csv = os.path.join(out_dir, f'us_return_concentration_{TODAY}.csv')
        try:
            with open(txt, 'w') as fh:
                fh.write(report + '\n')
            pd.DataFrame(rows).sort_values('top1pct_contrib', ascending=False).to_csv(csv, index=False)
            log.info(f'已写 {txt}')
            log.info(f'已写 {csv}')
        except Exception as e:
            log.warning(f'落盘失败 {e}')
    else:
        # 兜底: 写到 scratch
        txt = os.path.join(SCRATCH, f'us_return_concentration_{TODAY}.txt')
        with open(txt, 'w') as fh:
            fh.write(report + '\n')
        log.info(f'result/ 不存在, 报告写到 {txt}')


if __name__ == '__main__':
    main()
