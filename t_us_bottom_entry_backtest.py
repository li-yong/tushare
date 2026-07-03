# coding: utf-8
"""
US Bottom-Entry Backtest — 超跌底部入场 · 事件研究回测器

把 docs/twenty_week_trend_system.md §7.5–7.6 的实证固化成可复现脚本(原为 scratchpad
一次性脚本)。回答:"对优质标的, 底部(跌破20周线)放量上涨是不是好入场?"

方法(事件研究, point-in-time):
  事件 = 收盘距 252日高 <= -DROP 且 收盘 < 20周线(~100日SMA)。前向 21/63/126/252 日收益,
  基准 = 同日 QQQ。三类对照隔离"放量"贡献,reclaim 组测"站回线上"入场,
  PIT 质量(SEC EDGAR XBRL, filed<=信号日的 10-K ROE)拆桶测"优质"限定词。

已落地结论(2026-06-29, 详见 docs §7.5–7.6):
  1. edge 在"超跌+线下"状态本身, 越早进越好(线下 > 站回)。
  2. 机械"放量"过滤不贡献 EV、略拖累 —— 别当 go-signal。
  3. alpha 靠右尾少数大复苏(中位仅打平 QQQ)→ 必须分散多只 + 拿得住。
  4. 质量过滤的 job 是【防归零/左尾保护】不是【收益放大器】: 优质组反弹略输垃圾组(垃圾高beta+
     幸存者偏差吹大), 但深跌>40%率约为垃圾组一半。
  ⚠ 池=当前成分=幸存者偏差: 归零的真价值陷阱被删, 绝对收益偏乐观、质量护身被低估; 组间相对比较仍成立。

数据源: yfinance(行情, ADR-0001 同源) + SEC EDGAR XBRL(PIT 质量)。
输出: /home/ryan/DATA/result/us_bottom_entry_backtest/us_bottom_entry_backtest_<date>.md + 终端。

Usage:
  python t_us_bottom_entry_backtest.py                 # 全量(下载+PIT质量+事件研究)
  python t_us_bottom_entry_backtest.py --universe ndx  # 仅 Nasdaq-100(快)
  python t_us_bottom_entry_backtest.py --no-sec        # 跳过 PIT 质量(只跑 §7.5 部分)
  python t_us_bottom_entry_backtest.py --force         # 忽略缓存重拉
  python t_us_bottom_entry_backtest.py --years 10 --drop 25 --roe-min 12
"""
import os
import sys
import json
import time
import pickle
import logging
import datetime
import warnings
import urllib.request
from optparse import OptionParser

warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger('bottom_bt')

REPO = '/home/ryan/tushare_ryan'
RESULT_DIR = '/home/ryan/DATA/result'
CACHE_DIR = '/home/ryan/DATA/backtest_cache'
SEC_UA = 'Ryan Research sunraise2005@gmail.com'   # SEC 要求带联系方式的 User-Agent

# ── 事件/质量参数 ──────────────────────────────────────────────────────────
DROP_FROM_HIGH = -0.30     # 超跌门槛(距252日高)
MA_WK = 100                # 20周线 ~ 100 交易日
HI_LOOKBACK = 252
VOL_LOOKBACK = 20
COOLDOWN = 21              # 同标的事件冷却(交易日), 防重叠重复计数
FWD = [21, 63, 126, 252]   # 前向窗口
ROE_MIN = 0.12             # 质量门槛: 近≤3年年报 ROE 均值
ROE_STALE_DAYS = 700       # 最新可用年报超过此天数 → 质量 UNKNOWN


# ── 股票池 ─────────────────────────────────────────────────────────────────
def load_universe_tickers(which, force):
    from t_us_undervalue import load_universe
    u = load_universe(which, force=force)
    return sorted(set(t.replace('.', '-').upper() for t in u if t and t.isascii()))


# ── 行情下载(缓存)────────────────────────────────────────────────────────
def download_bars(tickers, years, force):
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache = os.path.join(CACHE_DIR, f'bars_{years}.pkl')
    if os.path.exists(cache) and not force:
        with open(cache, 'rb') as f:
            d = pickle.load(f)
        log.info(f'[bars cache] {len(d)} symbols')
        return d
    import yfinance as yf
    out, chunk = {}, 40
    for i in range(0, len(tickers), chunk):
        part = tickers[i:i + chunk]
        log.info(f'  download {i}-{i + len(part)} / {len(tickers)}')
        try:
            data = yf.download(part, period=f'{years}y', auto_adjust=True,
                               group_by='ticker', threads=True, progress=False)
        except Exception as e:
            log.warning(f'   chunk err {e}'); continue
        for t in part:
            try:
                df = (data[t][['Close', 'Volume']].dropna() if len(part) > 1
                      else data[['Close', 'Volume']].dropna())
                if len(df) > HI_LOOKBACK + max(FWD):
                    out[t] = df
            except Exception:
                pass
    with open(cache, 'wb') as f:
        pickle.dump(out, f)
    log.info(f'[bars done] {len(out)} usable symbols')
    return out


# ── PIT 质量库(SEC EDGAR XBRL)─────────────────────────────────────────────
def _sec_get(url):
    req = urllib.request.Request(url, headers={'User-Agent': SEC_UA})
    return json.load(urllib.request.urlopen(req, timeout=30))


def _parse_company_roe(facts):
    """逐 fiscal-year-end: ROE = NetIncomeLoss(年度,10-K) / StockholdersEquity(期末).
    每个 end 取【最早 filed】(原始10-K, 防 restatement/comparative 前视)。"""
    g = facts.get('facts', {}).get('us-gaap', {})
    if 'NetIncomeLoss' not in g or 'StockholdersEquity' not in g:
        return []
    pd_ = datetime.date.fromisoformat
    ni = {}
    for u in g['NetIncomeLoss']['units'].get('USD', []):
        if '10-K' not in (u.get('form') or '') or 'start' not in u or 'end' not in u:
            continue
        d0, d1 = pd_(u['start']), pd_(u['end'])
        if not (350 <= (d1 - d0).days <= 380):   # 年度期间
            continue
        f = pd_(u['filed'])
        if d1 not in ni or f < ni[d1][1]:
            ni[d1] = (u['val'], f)
    se = {}
    for u in g['StockholdersEquity']['units'].get('USD', []):
        if '10-K' not in (u.get('form') or '') or 'end' not in u or 'start' in u:
            continue                              # instant only
        d1, f = pd_(u['end']), pd_(u['filed'])
        if d1 not in se or f < se[d1][1]:
            se[d1] = (u['val'], f)
    out = []
    for end, (niv, nif) in ni.items():
        if end not in se:
            continue
        sev, sef = se[end]
        roe = niv / sev if sev > 0 else -9.99     # 负权益 = 非优质
        out.append((end, roe, max(nif, sef)))
    out.sort()
    return out


def build_pit_quality(tickers, force):
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache = os.path.join(CACHE_DIR, 'pit_quality.pkl')
    if os.path.exists(cache) and not force:
        with open(cache, 'rb') as f:
            d = pickle.load(f)
        log.info(f'[pit cache] {len(d)} symbols')
        return d
    cikmap = {v['ticker']: str(v['cik_str']).zfill(10)
              for v in _sec_get('https://www.sec.gov/files/company_tickers.json').values()}
    res, miss, nocik = {}, [], []
    for i, t in enumerate(tickers):
        cik = cikmap.get(t) or cikmap.get(t.replace('-', '.')) or cikmap.get(t.replace('-', ''))
        if not cik:
            nocik.append(t); continue
        try:
            series = _parse_company_roe(_sec_get(
                f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'))
            if series:
                res[t] = series
            else:
                miss.append(t)
        except Exception:
            miss.append(t)
        time.sleep(0.13)                          # SEC 限速 ~10 req/s
        if (i + 1) % 50 == 0:
            log.info(f'  SEC {i + 1}/{len(tickers)} ok={len(res)}')
    with open(cache, 'wb') as f:
        pickle.dump(res, f)
    log.info(f'[pit done] {len(res)}/{len(tickers)} ; no-CIK={len(nocik)} empty={len(miss)}')
    return res


def quality_asof(series, ts, roe_min):
    """ts: pandas Timestamp → 'PASS'/'FAIL'/'UNKNOWN'(PIT, 只用 filed<=ts 的年报)"""
    if not series:
        return 'UNKNOWN'
    dd = ts.date()
    avail = [(e, r) for (e, r, f) in series if f <= dd]
    if len(avail) < 2:
        return 'UNKNOWN'
    avail.sort()
    if (dd - avail[-1][0]).days > ROE_STALE_DAYS:
        return 'UNKNOWN'
    last = avail[-3:] if len(avail) >= 3 else avail
    return 'PASS' if np.mean([r for _, r in last]) > roe_min else 'FAIL'


# ── 事件生成 ───────────────────────────────────────────────────────────────
def gen_events(df, qqq_close, series, drop_th, roe_min):
    c, v = df['Close'], df['Volume']
    roll_hi = c.rolling(HI_LOOKBACK).max()
    ma = c.rolling(MA_WK).mean()
    avgv = v.rolling(VOL_LOOKBACK).mean()
    drop, up, below = c / roll_hi - 1.0, c > c.shift(1), c < ma
    base_cond = (drop <= drop_th) & below
    reclaim = below.shift(1) & (c >= ma)
    was_oversold = (drop <= drop_th).rolling(60).max().astype(bool)
    qpos = {ts: i for i, ts in enumerate(qqq_close.index)}
    qv = qqq_close.values
    events, last = [], {k: -10**9 for k in
                        ['BASE', 'LOWVOL', 'SIG15', 'SIG20', 'SIG25', 'RECL', 'RECL_V']}

    def fwd_block(i):
        c0 = c.iloc[i]; ts = df.index[i]
        fwd = {h: c.iloc[i + h] / c0 - 1.0 for h in FWD}
        if ts in qpos:
            qi = qpos[ts]
            qa = {h: (qv[qi + h] / qv[qi] - 1.0 if qi + h < len(qv) else np.nan) for h in FWD}
        else:
            qa = {h: np.nan for h in FWD}
        return dict(fwd=fwd, qqq=qa)

    n = len(df)
    for i in range(HI_LOOKBACK, n - max(FWD)):
        ts = df.index[i]
        volr = v.iloc[i] / avgv.iloc[i] if avgv.iloc[i] > 0 else 0
        # 站回线上(reclaim, 需曾超跌背景)
        if reclaim.iloc[i] and was_oversold.iloc[i]:
            rec = fwd_block(i); rec['q'] = quality_asof(series, ts, roe_min); rec['volr'] = volr
            if i - last['RECL'] >= COOLDOWN:
                events.append(('RECL', rec)); last['RECL'] = i
            if volr >= 1.5 and i - last['RECL_V'] >= COOLDOWN:
                events.append(('RECL_V', rec)); last['RECL_V'] = i
        if not base_cond.iloc[i]:
            continue
        rec = fwd_block(i); rec['q'] = quality_asof(series, ts, roe_min)
        rec['volr'] = volr; rec['up'] = bool(up.iloc[i])
        if i - last['BASE'] >= COOLDOWN:
            events.append(('BASE', rec)); last['BASE'] = i
        if not up.iloc[i]:
            continue
        for thr, key in [(2.5, 'SIG25'), (2.0, 'SIG20'), (1.5, 'SIG15')]:
            if volr >= thr and i - last[key] >= COOLDOWN:
                events.append((key, rec)); last[key] = i
        if volr < 1.5 and i - last['LOWVOL'] >= COOLDOWN:
            events.append(('LOWVOL', rec)); last['LOWVOL'] = i
    return events


# ── 汇总 ───────────────────────────────────────────────────────────────────
def _stats(recs, h):
    r = np.array([x['fwd'][h] for x in recs], float)
    a = r - np.array([x['qqq'][h] for x in recs], float)
    m, am = ~np.isnan(r), ~np.isnan(a)
    return dict(
        n=int(m.sum()),
        mean=round(np.nanmean(r) * 100, 2), med=round(np.nanmedian(r) * 100, 2),
        win=round(np.nanmean(r[m] > 0) * 100, 1), trap=round(np.nanmean(r[m] < 0) * 100, 1),
        loss40=round(np.nanmean(r[m] < -0.40) * 100, 1),
        alpha=round(np.nanmean(a[am]) * 100, 2) if am.sum() else np.nan,
        beatQQQ=round(np.nanmean(a[am] > 0) * 100, 1) if am.sum() else np.nan)


def _table(buckets):
    rows = []
    for name, recs in buckets.items():
        if not recs:
            continue
        for h in FWD:
            s = _stats(recs, h); s.update(group=name, h=h); rows.append(s)
    cols = ['group', 'h', 'n', 'mean', 'med', 'win', 'trap', 'loss40', 'alpha', 'beatQQQ']
    return pd.DataFrame(rows)[cols]


def summarize(all_events, use_sec):
    g = {}
    for k, rec in all_events:
        g.setdefault(k, []).append(rec)
    base = g.get('BASE', [])
    out = {}
    out['signal'] = _table({k: g.get(k, []) for k in
                            ['BASE', 'LOWVOL', 'SIG15', 'SIG20', 'SIG25']})
    out['reclaim'] = _table({'RECL': g.get('RECL', []), 'RECL_V': g.get('RECL_V', [])})
    if use_sec:
        out['quality'] = _table({
            'Q=PASS': [x for x in base if x['q'] == 'PASS'],
            'Q=FAIL': [x for x in base if x['q'] == 'FAIL'],
            'Q=UNK': [x for x in base if x['q'] == 'UNKNOWN'],
            'PASS+vol1.5': [x for x in base if x['q'] == 'PASS' and x.get('up') and x['volr'] >= 1.5],
        })
    return out


# ── 报告 ───────────────────────────────────────────────────────────────────
def write_report(tables, meta):
    date_str = datetime.datetime.now().strftime('%Y%m%d')
    lines = [f"# 超跌底部入场 · 事件研究回测  ({date_str})", ""]
    lines += [f"- 池: {meta['universe']} ({meta['n_sym']} 只) × {meta['years']}y",
              f"- 事件: 距252日高 ≤ {int(DROP_FROM_HIGH*100)}% 且 收盘 < 20周线; 冷却 {COOLDOWN}d; 总 {meta['n_ev']} 事件",
              f"- 基准: 同日 QQQ; 前向窗口 {FWD} 交易日",
              "- ⚠ 幸存者偏差: 池=当前成分, 归零的真价值陷阱被删 → 绝对收益偏乐观、质量护身被低估; 组间相对比较仍成立。",
              "- 列: mean/med=前向收益均值/中位%, win=胜率%, trap=负收益率%, loss40=跌>40%率%, alpha=对QQQ超额%, beatQQQ=跑赢QQQ占比%",
              ""]
    sec = [('## 1. 放量是否加分 (BASE=只超跌+线下; LOWVOL/SIG=加量价过滤)', 'signal'),
           ('## 2. 站回20周线入场 (RECL=站回; RECL_V=站回且放量)', 'reclaim'),
           ('## 3. PIT 质量拆桶 (SEC EDGAR, filed<=信号日的10-K ROE)', 'quality')]
    for title, key in sec:
        if key not in tables:
            continue
        lines += [title, "", tables[key].to_markdown(index=False), ""]
    lines += ["## 结论 (详见 docs/twenty_week_trend_system.md §7.5–7.6)", "",
              "1. edge 在'超跌+线下'状态本身, 越早进越好 (线下 > 站回)。",
              "2. 机械'放量'过滤不贡献 EV、略拖累 — 别当 go-signal。",
              "3. alpha 靠右尾少数大复苏 (中位仅打平 QQQ) → 必须分散多只 + 拿得住。",
              "4. 质量过滤 = 防归零/左尾保护 (深跌率减半), 不是收益放大器。", ""]
    report = "\n".join(lines)
    out_dir = os.path.join(RESULT_DIR, 'us_bottom_entry_backtest')
    if os.path.isdir(RESULT_DIR):
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f'us_bottom_entry_backtest_{date_str}.md')
        with open(path, 'w') as f:
            f.write(report)
        log.info(f'报告 -> {path}')
    return report


# ── main ───────────────────────────────────────────────────────────────────
def main():
    p = OptionParser()
    p.add_option('--universe', default='both', help='sp500 | ndx | both (默认 both)')
    p.add_option('--years', type='int', default=15, help='历史年数 (默认 15)')
    p.add_option('--drop', type='float', default=30.0, help='超跌门槛%% (默认 30)')
    p.add_option('--roe-min', type='float', default=12.0, help='质量 ROE 门槛%% (默认 12)')
    p.add_option('--no-sec', action='store_true', help='跳过 PIT 质量 (只跑 §7.5)')
    p.add_option('--force', action='store_true', help='忽略所有缓存')
    opts, _ = p.parse_args()

    global DROP_FROM_HIGH, ROE_MIN
    DROP_FROM_HIGH = -abs(opts.drop) / 100.0
    ROE_MIN = opts.roe_min / 100.0

    tickers = load_universe_tickers(opts.universe, opts.force)
    log.info(f'universe {opts.universe}: {len(tickers)} tickers')
    bars = download_bars(sorted(set(tickers) | {'QQQ'}), opts.years, opts.force)
    if 'QQQ' not in bars:
        log.error('no QQQ bars — abort'); return
    quality = {} if opts.no_sec else build_pit_quality(
        [t for t in tickers if t in bars], opts.force)

    qqq = bars['QQQ']['Close']
    all_events, n_sym = [], 0
    for t, df in bars.items():
        if t == 'QQQ':
            continue
        all_events += gen_events(df, qqq, quality.get(t), DROP_FROM_HIGH, ROE_MIN)
        n_sym += 1
    log.info(f'symbols={n_sym} events={len(all_events)}')

    tables = summarize(all_events, use_sec=not opts.no_sec)
    report = write_report(tables, dict(universe=opts.universe, n_sym=n_sym,
                                       years=opts.years, n_ev=len(all_events)))
    print('\n' + report)


if __name__ == '__main__':
    main()
