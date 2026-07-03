# coding: utf-8
"""
US Bottom-Entry Screener — 超跌底部入场 · 实盘择时屏

t_us_bottom_entry_backtest.py 验证了方法论, 本脚本是它的【实盘】姊妹: 扫当前
SP500∪NDX, 找此刻处于"超跌 + 跌破20周线(线下)"入场状态的候选, 按质量分层。

与 t_us_undervalue.py 的分工(两者都'超跌优质', 但活不同):
  · t_us_undervalue.py = 研究候选清单, 供笔记 §二三问深度复核(宽, 富基本面/估值分位)。
  · 本脚本        = 择时入场屏, 绑定 20 周线系统 §7: "此刻是不是线下入场点"。

入场状态(docs/twenty_week_trend_system.md §7 + 实证 §7.5-7.6):
  超跌 = 收盘距 252日高 <= -DROP%  ;  线下 = 收盘 < 20周线(~100日SMA)。

纪律(实证钉死, 直接印在报告头):
  · 质量(ROE)= 防归零/左尾保护, 不是收益放大器 → PASS 优先, FAIL 高弹性但需查
    是否结构性受损(价值陷阱)。质量不是硬门, 是分层。
  · 分散持多只 + 拿得住: alpha 靠右尾少数大复苏, 单押 ≈ 打平 QQQ。
  · 越早进越好: 线下就分批建(probe), 别等"放量"或"站回均线"——机械放量不加分,
    站回入场反而更差。

数据: yfinance(近2y 日线) + SEC EDGAR XBRL PIT ROE(复用回测模块)。
输出: /home/ryan/DATA/result/us_bottom_entry/us_bottom_entry_<date>.md + 终端 + CSV。

Usage:
  python t_us_bottom_entry.py                    # 全量 SP500∪NDX
  python t_us_bottom_entry.py --universe ndx     # 仅 Nasdaq-100(快)
  python t_us_bottom_entry.py --quality-only     # 只列质量 PASS(防归零优先)
  python t_us_bottom_entry.py --drop 25          # 放宽超跌门槛到 25%
  python t_us_bottom_entry.py --no-sec           # 跳过质量分层(纯价量, 快)
  python t_us_bottom_entry.py --force            # 忽略当日缓存重拉
"""
import os
import sys
import pickle
import logging
import datetime
import warnings
from optparse import OptionParser

warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger('bottom_entry')

import t_us_bottom_entry_backtest as bt   # 复用 universe / SEC PIT 质量基建

RESULT_DIR = '/home/ryan/DATA/result'
CACHE_DIR = bt.CACHE_DIR
HI_LOOKBACK = bt.HI_LOOKBACK   # 252
MA_WK = bt.MA_WK               # 100  (~20周线)


def fetch_recent(tickers, which, force):
    """近2y 日线, 当日缓存(只为算 252日高 + 100日均线 + 最新收盘)。
    缓存名含 universe, 防同日 ndx/both 互相串用对方的部分缓存。"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    today = datetime.date.today().isoformat()
    cache = os.path.join(CACHE_DIR, f'recent_{which}_{today}.pkl')
    if os.path.exists(cache) and not force:
        with open(cache, 'rb') as f:
            return pickle.load(f)
    import yfinance as yf
    out, chunk = {}, 60
    for i in range(0, len(tickers), chunk):
        part = tickers[i:i + chunk]
        log.info(f'  download {i}-{i + len(part)} / {len(tickers)}')
        try:
            data = yf.download(part, period='2y', auto_adjust=True,
                               group_by='ticker', threads=True, progress=False)
        except Exception as e:
            log.warning(f'   chunk err {e}'); continue
        for t in part:
            try:
                df = (data[t]['Close'].dropna() if len(part) > 1
                      else data['Close'].dropna())
                if len(df) >= HI_LOOKBACK:
                    out[t] = df
            except Exception:
                pass
    with open(cache, 'wb') as f:
        pickle.dump(out, f)
    log.info(f'[bars] {len(out)} symbols')
    return out


def roe_summary(series, ts):
    """(label, 近≤3年ROE均值%, 最新FY末) — PIT-correct(只用 filed<=ts 的年报)。"""
    label = bt.quality_asof(series, ts, bt.ROE_MIN)
    if not series:
        return label, np.nan, None
    dd = ts.date()
    avail = sorted([(e, r) for (e, r, f) in series if f <= dd])
    if not avail:
        return label, np.nan, None
    last = avail[-3:] if len(avail) >= 3 else avail
    return label, round(np.mean([r for _, r in last]) * 100, 1), avail[-1][0]


def screen(bars, quality, drop_th):
    ts_now = pd.Timestamp(datetime.date.today())
    rows = []
    for t, close in bars.items():
        if t == 'QQQ' or len(close) < HI_LOOKBACK:
            continue
        c = float(close.iloc[-1])
        hi = float(close.iloc[-HI_LOOKBACK:].max())
        ma = float(close.iloc[-MA_WK:].mean())
        from_hi = c / hi - 1.0
        if not (from_hi <= drop_th and c < ma):     # 超跌 且 线下
            continue
        q, roe, fy = roe_summary(quality.get(t), ts_now)
        rows.append(dict(
            ticker=t, from_hi=round(from_hi * 100, 1), close=round(c, 2),
            ma20wk=round(ma, 2), vs_ma=round((c / ma - 1) * 100, 1),
            roe3y=roe, quality=q))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # 质量分层(PASS 优先=防归零), 层内按超跌深度排
    order = {'PASS': 0, 'UNKNOWN': 1, 'FAIL': 2}
    df['_q'] = df['quality'].map(order).fillna(1)
    df = df.sort_values(['_q', 'from_hi']).drop(columns='_q').reset_index(drop=True)
    return df


def write_report(df, meta):
    date_str = datetime.datetime.now().strftime('%Y%m%d')
    head = [f"# 超跌底部入场 · 实盘候选  ({date_str})", "",
            f"- 池: {meta['universe']} ({meta['n_sym']} 只) | 入场状态: 距252日高 ≤ {meta['drop']}% 且 收盘<20周线",
            f"- 命中: {len(df)} 只" + ("" if df.empty else f"(PASS {meta['n_pass']} / UNKNOWN {meta['n_unk']} / FAIL {meta['n_fail']})"),
            "",
            "**用法纪律(实证, docs/twenty_week_trend_system.md §7.5-7.6):**",
            "1. 质量=防归零/左尾保护非收益放大器 → PASS 优先; FAIL 高反弹弹性但**先查是否结构性受损**(价值陷阱), 不是自动可买。",
            "2. **分散持多只 + 拿得住** —— alpha 靠右尾少数大复苏, 单押 ≈ 打平 QQQ。",
            "3. **越早进越好**: 线下就分批建(probe), 别等放量/站回均线(机械放量不加分, 站回入场更差)。",
            "4. 进场后交还 20 周线系统 Layer 0-2 管退出。", ""]
    if df.empty:
        body = "_当前无标的处于'超跌+线下'入场状态。_"
    else:
        body = df.to_markdown(index=False)
    report = "\n".join(head) + "\n" + body + "\n"
    out_dir = os.path.join(RESULT_DIR, 'us_bottom_entry')
    if os.path.isdir(RESULT_DIR):
        os.makedirs(out_dir, exist_ok=True)
        base = os.path.join(out_dir, f'us_bottom_entry_{date_str}')
        with open(base + '.md', 'w') as f:
            f.write(report)
        if not df.empty:
            df.to_csv(base + '.csv', index=False)
        log.info(f'报告 -> {base}.md')
    return report


def main():
    p = OptionParser()
    p.add_option('--universe', default='both', help='sp500 | ndx | both (默认 both)')
    p.add_option('--drop', type='float', default=30.0, help='超跌门槛%% (默认 30)')
    p.add_option('--quality-only', action='store_true', help='只列质量 PASS')
    p.add_option('--no-sec', action='store_true', help='跳过质量分层(纯价量)')
    p.add_option('--force', action='store_true', help='忽略当日缓存重拉')
    opts, _ = p.parse_args()
    drop_th = -abs(opts.drop) / 100.0

    tickers = bt.load_universe_tickers(opts.universe, opts.force)
    log.info(f'universe {opts.universe}: {len(tickers)} tickers')
    bars = fetch_recent(tickers, opts.universe, opts.force)
    quality = {} if opts.no_sec else bt.build_pit_quality(
        [t for t in tickers if t in bars], opts.force)

    df = screen(bars, quality, drop_th)
    if not df.empty and opts.quality_only:
        df = df[df['quality'] == 'PASS'].reset_index(drop=True)
    meta = dict(universe=opts.universe, n_sym=len(bars), drop=int(opts.drop),
                n_pass=int((df['quality'] == 'PASS').sum()) if not df.empty else 0,
                n_unk=int((df['quality'] == 'UNKNOWN').sum()) if not df.empty else 0,
                n_fail=int((df['quality'] == 'FAIL').sum()) if not df.empty else 0)
    report = write_report(df, meta)
    print('\n' + report)


if __name__ == '__main__':
    main()
