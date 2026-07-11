# coding: utf-8
"""
Signal attribution report (信号归因) — reads the signal ledger and answers
"which signal type actually pays?" with forward outcomes per source × type.

Input : /home/ryan/DATA/result/us_signal_log/us_signal_ledger.csv
        (written live by the scanners via signal_ledger.log_signals; one row
        per signal episode, entry/stop frozen at first_seen)
Output: result/us_signal_log/us_signal_attrib_<date>.txt (+ stdout)

Per episode, anchored at first_seen with the frozen entry/stop:
  fwd 21/63/126d   — close-to-close return from entry over N sessions
  alpha 63d        — fwd 63d minus QQQ over the same dates (theme-beta check:
                     a signal that doesn't beat QQQ is packaging, not edge)
  stop-hit         — first session whose CLOSE < frozen stop within 63d
                     (ADR-0002 close-evaluated; proxy for -1R before the move)
Only episodes old enough for a horizon count toward it; younger ones are
reported as pending. Groups smaller than --min-n are shown but flagged — do
NOT read EV off single-digit n.

This measures the SIGNAL (fixed-horizon, frozen stop), not the full position-
management stack (BE move, 20wMA trail, trims) — by design: entry alpha and
exit discipline are separate questions (方法论: 入场和退出是两份工作).

Usage:
  python t_us_signal_attrib.py              # full report
  python t_us_signal_attrib.py --min-n 10   # flag threshold for small groups
  python t_us_signal_attrib.py --ledger P   # alternate ledger (testing)
"""

import os
import sys
import logging
import datetime

import numpy as np
import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Reuse the scanner's data layer (yfinance + stale-cache fallback, ADR-0001)
# so attribution sees exactly the bars the signals were generated from.
import t_us_tech_swing as tsw

from signal_ledger import LEDGER_PATH

HORIZONS  = [21, 63, 126]     # sessions ≈ 1m / 3m / 6m
ALPHA_H   = 63                # horizon used for the QQQ-alpha and win-rate cols
BENCH     = 'QQQ'
OUT_DIR   = '/home/ryan/DATA/result/us_signal_log'
# 止损尸检登记簿 (实践论: 吃一堑要收据). 每笔 stop-hit episode 一行, 周日复盘
# 手工填 falsified_assumption — 入场时哪条假设被证伪, 四选一:
#   pool(池子错) / regime(体制读错) / context(信号在此语境无效) / tail(运气尾部)
# 积累后看认识偏差聚在哪一格; 已填的行永不被脚本改写.
AUTOPSY_PATH = os.path.join(OUT_DIR, 'stopout_autopsy.csv')
AUTOPSY_COLS = ['first_seen', 'source', 'ticker', 'signal_type', 'market_state',
                'stop_hit_d', 'falsified_assumption', 'notes']


def _bars(ticker: str) -> pd.DataFrame:
    try:
        return tsw._fetch_daily_full(ticker)
    except Exception as e:
        logging.warning(f'{ticker}: bars unavailable ({e})')
        return pd.DataFrame()


def _anchor_idx(daily: pd.DataFrame, first_seen: pd.Timestamp) -> int | None:
    """Index of the first bar ≥ first_seen (the signal was emitted after that
    day's close; entry per the checklist is that close, so anchor at it)."""
    pos = daily.index.searchsorted(first_seen)
    if pos >= len(daily):
        return None
    return int(pos)


def evaluate(ledger: pd.DataFrame) -> pd.DataFrame:
    """One outcome row per episode; NaN where the horizon hasn't matured."""
    bench = _bars(BENCH)
    out = []
    for _, ep in ledger.iterrows():
        ticker = ep['ticker']
        daily  = _bars(ticker)
        rec = {'source': ep['source'], 'ticker': ticker,
               'signal_type': ep['signal_type'],
               'market_state': ep.get('market_state'),
               'confidence': ep.get('confidence'),
               'rr_ok': ep.get('rr_ok'), 'er_blackout': ep.get('er_blackout'),
               'first_seen': ep['first_seen']}
        out.append(rec)
        if daily.empty:
            continue
        first_seen = pd.Timestamp(ep['first_seen'])
        a = _anchor_idx(daily, first_seen)
        if a is None:
            continue
        entry = float(ep['entry']) if pd.notna(ep['entry']) else float(daily['close'].iloc[a])
        stop  = float(ep['stop'])  if pd.notna(ep['stop'])  else None
        closes = daily['close']

        for h in HORIZONS:
            if a + h < len(closes):
                rec[f'fwd{h}'] = (float(closes.iloc[a + h]) / entry - 1) * 100

        # QQQ over the identical calendar window (alpha horizon only)
        if f'fwd{ALPHA_H}' in rec and not bench.empty:
            d0, d1 = daily.index[a], daily.index[a + ALPHA_H]
            b = bench['close']
            b0 = b.asof(d0)
            b1 = b.asof(d1)
            if pd.notna(b0) and pd.notna(b1) and b0 > 0:
                rec[f'alpha{ALPHA_H}'] = rec[f'fwd{ALPHA_H}'] - (b1 / b0 - 1) * 100

        # First close below the frozen stop within the alpha horizon. A hit
        # counts as soon as it happens; a non-hit only counts once the full
        # window has been observed — otherwise young episodes dilute the rate.
        if stop is not None and stop > 0:
            win = closes.iloc[a + 1: a + ALPHA_H + 1]
            hit = win[win < stop]
            if len(hit):
                rec['stop_hit'] = True
                rec['stop_hit_d'] = int(win.index.get_loc(hit.index[0])) + 1
            elif len(win) >= ALPHA_H:
                rec['stop_hit'] = False
    return pd.DataFrame(out)


def _update_autopsy(df: pd.DataFrame) -> pd.DataFrame:
    """Append newly stop-hit episodes to the autopsy book; return the full book.

    Append-only by episode key (source|ticker|first_seen) — hand-written
    verdicts in existing rows are never touched.
    """
    if os.path.exists(AUTOPSY_PATH):
        book = pd.read_csv(AUTOPSY_PATH, dtype=str).fillna('')
    else:
        book = pd.DataFrame(columns=AUTOPSY_COLS)

    if 'stop_hit' in df.columns:
        hits = df[df['stop_hit'].eq(True)]
    else:
        hits = pd.DataFrame()
    if not len(hits):
        return book

    def _key(d):
        return (d['source'].astype(str) + '|' + d['ticker'].astype(str)
                + '|' + d['first_seen'].astype(str))

    known = set(_key(book)) if len(book) else set()
    new = hits[~_key(hits).isin(known)]
    if len(new):
        add = new[['first_seen', 'source', 'ticker', 'signal_type',
                   'market_state', 'stop_hit_d']].astype(str)
        # stop_hit_d 在混有非命中行的 df 里是 float 列 — 落盘成整数字符串
        add['stop_hit_d'] = new['stop_hit_d'].map(
            lambda v: str(int(v)) if pd.notna(v) else '')
        add['falsified_assumption'] = ''
        add['notes'] = ''
        book = pd.concat([book, add[AUTOPSY_COLS]], ignore_index=True)
        os.makedirs(OUT_DIR, exist_ok=True)
        book.to_csv(AUTOPSY_PATH, index=False)
        logging.info(f'{len(new)} new stop-hit episode(s) → {AUTOPSY_PATH}')
    return book


def _agg(df: pd.DataFrame, by: list[str]) -> list[list]:
    rows = []
    for key, g in df.groupby(by, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        n = len(g)
        f63 = g.get(f'fwd{ALPHA_H}', pd.Series(dtype=float)).dropna()
        rec = list(key) + [n, len(f63)]
        for h in HORIZONS:
            s = g.get(f'fwd{h}', pd.Series(dtype=float)).dropna()
            rec.append(f'{s.median():+.1f}' if len(s) else '—')
        rec.append(f'{(f63 > 0).mean() * 100:.0f}%' if len(f63) else '—')
        al = g.get(f'alpha{ALPHA_H}', pd.Series(dtype=float)).dropna()
        rec.append(f'{al.median():+.1f}' if len(al) else '—')
        sh = g.get('stop_hit', pd.Series(dtype=object)).dropna()
        rec.append(f'{(sh.astype(bool)).mean() * 100:.0f}%' if len(sh) else '—')
        rows.append(rec)
    return sorted(rows, key=lambda r: -r[len(by)])


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--ledger', dest='ledger', default=LEDGER_PATH,
                      help='ledger CSV to evaluate (default: live ledger)')
    parser.add_option('--min-n', dest='min_n', default=5, type='int',
                      help='groups below this episode count are flagged ⚠ (default 5)')
    parser.add_option('--output', dest='output', default=None,
                      help='report path (default: result/us_signal_log/us_signal_attrib_<date>.txt)')
    opts, _ = parser.parse_args()

    if not os.path.exists(opts.ledger):
        logging.warning(f'no ledger yet at {opts.ledger} — nothing to attribute. '
                        'It fills up as the daily scan runs live.')
        return

    ledger = pd.read_csv(opts.ledger)
    if ledger.empty:
        logging.warning('ledger is empty — nothing to attribute.')
        return
    logging.info(f'{len(ledger)} episode(s) in ledger; evaluating forward outcomes …')
    df = evaluate(ledger)

    lines = []

    def p(*args):
        line = ' '.join(str(a) for a in args)
        lines.append(line)
        print(line)

    today = datetime.date.today()
    mature = df.get(f'fwd{ALPHA_H}', pd.Series(dtype=float)).notna().sum()
    p()
    p('=' * 72)
    p(f'  SIGNAL ATTRIBUTION 信号归因  —  {today}')
    p('=' * 72)
    p(f'  episodes: {len(df)} total · {mature} mature at {ALPHA_H}d · '
      f'{len(df) - mature} pending (太新, 窗口未满)')
    p(f'  按 first_seen 冻结的 entry/stop 计; 衡量的是【信号】不是完整持仓管理')
    p()

    hdr_tail = ['n', f'n{ALPHA_H}d'] + [f'med{h}d%' for h in HORIZONS] + [
        f'win{ALPHA_H}d', f'medαvsQQQ{ALPHA_H}d%', f'stop-hit≤{ALPHA_H}d']

    p(f'[ BY SOURCE × SIGNAL TYPE ]   (⚠ = n < {opts.min_n}, 别读EV)')
    rows = _agg(df, ['source', 'signal_type'])
    for r in rows:
        if r[2] < opts.min_n:
            r[1] = f'{r[1]} ⚠'
    p(tab_mod.tabulate(rows, headers=['source', 'type'] + hdr_tail, tablefmt='simple'))
    p()

    p('[ BY SIGNAL TYPE × MARKET STATE ]   (同一信号在不同体制下是不是两回事)')
    rows = _agg(df, ['signal_type', 'market_state'])
    for r in rows:
        if r[2] < opts.min_n:
            r[1] = f'{r[1]} ⚠'
    p(tab_mod.tabulate(rows, headers=['type', 'state'] + hdr_tail, tablefmt='simple'))
    p()

    # ── 止损尸检 (实践论: 每次止损必须回答"入场时哪条假设被证伪") ──────────────
    book = _update_autopsy(df)
    p('[ STOP-OUT AUTOPSY 止损尸检 ]   (每笔 stop-hit 一行, 周日复盘手工填 falsified_assumption)')
    if not len(book):
        p('  尚无 stop-hit episode.')
    else:
        pending = book[book['falsified_assumption'].astype(str).str.strip() == '']
        p(f'  登记簿: {AUTOPSY_PATH}')
        p(f'  {len(book)} 笔已登记 · {len(pending)} 笔待尸检')
        for _, r in pending.head(10).iterrows():
            p(f"    ◻ {r['first_seen']}  {r['source']}/{r['ticker']}  {r['signal_type']}  "
              f"state={r['market_state']}  D+{r['stop_hit_d']} 收盘破冻结止损")
        if len(pending) > 10:
            p(f'    … 还有 {len(pending) - 10} 笔, 见登记簿')
        p('  填法四选一: pool(池子错) / regime(体制读错) / context(信号在此语境无效) / tail(运气尾部)')
        p('  看的是认识偏差聚在哪一格, 不是单笔盈亏; 该列分组统计待样本 ≥5 后加进本报告')
    p()

    p('[ 读法 / LEGEND ]')
    p(f'  medNd% = 信号日收盘入场后 N 交易日的中位收益; win{ALPHA_H}d = {ALPHA_H}d 胜率')
    p(f'  medαvsQQQ = 相同区间减 QQQ — 跑不赢 QQQ 的信号是包装不是 edge (评审 Q7)')
    p(f'  stop-hit = {ALPHA_H}d 内日收盘跌破冻结止损的比例 (≈先亏1R的概率)')
    p(f'  n{ALPHA_H}d < n: 差值是窗口未满的新 episode; 每周日 cron 重算, 样本随时间长大')
    p('  ⚠ 单一 regime 警告: 前 12 个月的样本几乎全来自同一市场体制, 结论仅在该体制内有效')
    p()

    out_file = opts.output
    if out_file is None and os.path.isdir(os.path.dirname(OUT_DIR)):
        os.makedirs(OUT_DIR, exist_ok=True)
        out_file = os.path.join(OUT_DIR, f'us_signal_attrib_{today.strftime("%Y%m%d")}.txt')
    if out_file:
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        logging.info(f'Attribution report → {out_file}')


if __name__ == '__main__':
    main()
