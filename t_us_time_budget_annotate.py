#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
时间预算回填标注 (registry #26 的复盘闭环半边)。

晨报的 ⏳ 行管"现在持仓横盘了" (t_us_tech_swing);本脚本管"当日报告里触发的
信号, 预算期满后到底动没动" — 按信号日体制查预算 (STRONG 20 / MIXED 10 /
WEAK 5 个交易日), 期满后回头把判定直接写进那天的 us_daily_report_<date>.md
末尾, 让复盘 (daily-report review protocol) 翻旧报告时当场看到每个触发
ticker 的时间预算结局, 不用自己再查行情。

触发名单不解析 markdown — 用 signal ledger (`us_signal_ledger.csv`) 的
first_seen 分组: episode 冻结了 entry/stop/market_state, 是"当日触发"的
权威记录 (tech_swing + news_top 两个 live 源, 与 t_us_signal_attrib 同底账)。

判定 (huice 路径重放实证, 见 rule_registry #26):
  ✗ 破止损      窗口内日收盘 < 冻结 stop (止损层已管, 非时间预算问题)
  ⏳ 没动       第B日涨幅 ≤ +3% — 后续期望已低于换新信号 → 若已持仓=换仓候选
  ~ 温吞        +3% ~ +10% — huice 里该桶后续同样负期望, 仅次于没动
  ✓ 已启动     > +10% — 唯一正期望桶 (对的票头一两周自我证明)

幂等: 每份报告只标注一次 (HTML 注释标记), 期满前跳过、下次运行补上。
Cron: us_daily_run.sh 末尾 (daily-report 之后, run_step 非致命)。
"""
import argparse
import glob
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

import pandas as pd

import t_us_tech_swing as tsw   # 单一真相源: 预算/横盘带常数 + bar 缓存层

LEDGER_CSV  = '/home/ryan/DATA/result/us_signal_log/us_signal_ledger.csv'
REPORT_DIR  = '/home/ryan/DATA/result/daily_report'
MARKER      = '<!-- time_budget_annotated -->'
MOVED_PCT   = 10.0    # ">10% = 已启动" 桶边界 (huice 唯一正期望桶)
LOOKBACK_D  = 90      # 只回看最近 N 个自然日的报告 (更早的复盘价值已过期)


def _report_path(date_iso: str) -> str | None:
    """us_daily_report_<yyyymmdd>.md — 平放或子目录都认 (result/ 递归 glob 契约)。"""
    ymd = date_iso.replace('-', '')
    flat = os.path.join(REPORT_DIR, f'us_daily_report_{ymd}.md')
    if os.path.exists(flat):
        return flat
    hits = glob.glob(f'/home/ryan/DATA/result/**/us_daily_report_{ymd}.md',
                     recursive=True)
    return hits[0] if hits else None


def _annotate_group(date_iso: str, group: pd.DataFrame, dry_run: bool) -> str:
    """一份报告一组 episodes; 返回状态串 (日志/汇总用)。"""
    path = _report_path(date_iso)
    if path is None:
        return 'no-report'
    with open(path, encoding='utf-8') as fh:
        content = fh.read()
    if MARKER in content:
        return 'done'

    # 预算按信号日体制 (market_state 是全局的, 同日各行一致; 空/未知按 MIXED)
    states = group['market_state'].dropna()
    state = states.mode().iloc[0] if not states.empty else 'MIXED'
    budget = tsw.TIME_BUDGET_N.get(state, tsw.TIME_BUDGET_N['MIXED'])

    d = pd.Timestamp(date_iso)
    rows, immature = [], False
    for _, ep in group.iterrows():
        ticker = str(ep['ticker'])
        try:
            bars = tsw._fetch_daily(ticker)
        except Exception:
            bars = pd.DataFrame()
        if bars.empty:
            rows.append((ep, None))
            continue
        a = int((bars.index <= d).sum()) - 1   # 信号日收盘 bar (文件名戳晚美东一天, ≤d 取最后)
        if a < 0:
            rows.append((ep, None))
            continue
        if a + budget >= len(bars):
            immature = True                    # 第 B 交易日还没到 — 整份报告下次再来
            break
        c0 = float(bars['close'].iloc[a])
        cb = float(bars['close'].iloc[a + budget])
        ret = (cb / c0 - 1) * 100

        alpha = None
        try:
            qqq = tsw._fetch_daily('QQQ')
            q0 = float(qqq['close'].iloc[int((qqq.index <= bars.index[a]).sum()) - 1])
            qb = float(qqq['close'].iloc[int((qqq.index <= bars.index[a + budget]).sum()) - 1])
            alpha = ret - (qb / q0 - 1) * 100
        except Exception:
            pass

        # 冻结 stop 换算到当前复权口径 (ledger entry ≈ 当时的复权收盘)
        stopped = False
        stop, entry = ep.get('stop'), ep.get('entry')
        if pd.notna(stop) and pd.notna(entry) and float(entry) > 0:
            adj_stop = float(stop) * c0 / float(entry)
            stopped = bool((bars['close'].iloc[a + 1:a + budget + 1] < adj_stop).any())

        if stopped:
            verdict = '✗ 破止损'
        elif ret <= tsw.TIME_BUDGET_FLAT_PCT:
            verdict = '⏳ 没动 → 若已持仓=换仓候选'
        elif ret <= MOVED_PCT:
            verdict = '~ 温吞'
        else:
            verdict = '✓ 已启动'
        rows.append((ep, dict(c0=c0, cb=cb, ret=ret, alpha=alpha, verdict=verdict,
                              asof=bars.index[a + budget].date())))
    if immature:
        return f'immature (<{budget}d)'

    asof = next((r['asof'] for _, r in rows if r), '?')
    lines = [
        '', '---', '',
        f'## ⏳ 时间预算复核 — 第{budget}交易日回填 '
        f'(信号日体制 {state}, 复核收盘 {asof}, 写入 {pd.Timestamp.today().date()})',
        MARKER, '',
        'huice 实证 (registry #26): 对的票头一两周自我证明 — 第5天已涨>10%是唯一正期望桶;',
        f'到第{budget}日仍没动(≤+{tsw.TIME_BUDGET_FLAT_PCT:.0f}%)的信号, 后续期望已低于换一个新信号。',
        '',
        f'| Ticker | 来源 | 类型 | 信号日收盘 | 第{budget}日收盘 | 涨幅 | α vs QQQ | 判定 |',
        '|---|---|---|---|---|---|---|---|',
    ]
    for ep, r in rows:
        if r is None:
            lines.append(f"| {ep['ticker']} | {ep['source']} | {ep['signal_type']} "
                         f'| — | — | — | — | 无数据 |')
            continue
        al = f"{r['alpha']:+.1f}%" if r['alpha'] is not None else '—'
        lines.append(f"| {ep['ticker']} | {ep['source']} | {ep['signal_type']} "
                     f"| {r['c0']:.2f} | {r['cb']:.2f} | {r['ret']:+.1f}% "
                     f"| {al} | {r['verdict']} |")
    block = '\n'.join(lines) + '\n'

    if dry_run:
        print(f'--- would append to {path}:')
        print(block)
    else:
        with open(path, 'a', encoding='utf-8') as fh:
            fh.write(block)
    n_flat = sum(1 for _, r in rows if r and r['verdict'].startswith('⏳'))
    return f'annotated ({len(rows)} tickers, {n_flat} ⏳)'


def main():
    ap = argparse.ArgumentParser(description='时间预算回填标注 daily_report')
    ap.add_argument('--dry-run', action='store_true', help='只打印, 不写报告文件')
    ap.add_argument('--lookback', type=int, default=LOOKBACK_D,
                    help=f'回看天数 (自然日, 默认 {LOOKBACK_D})')
    args = ap.parse_args()

    if not os.path.exists(LEDGER_CSV):
        logging.warning(f'ledger 不存在: {LEDGER_CSV} — 无可标注')
        return
    ledger = pd.read_csv(LEDGER_CSV)
    cutoff = (pd.Timestamp.today() - pd.Timedelta(days=args.lookback)).date().isoformat()
    ledger = ledger[ledger['first_seen'] >= cutoff]

    for date_iso, group in sorted(ledger.groupby('first_seen'), reverse=True):
        status = _annotate_group(date_iso, group, args.dry_run)
        if status != 'done':      # 已标注过的日子太多, 只报有动作/待熟的
            logging.info(f'{date_iso}: {status}')


if __name__ == '__main__':
    main()
