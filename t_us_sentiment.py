# -*- coding: utf-8 -*-
"""
t_us_sentiment.py — 美股 持仓×观察仓 实时情绪/催化剂扫描 (Grok 情绪层)

定位:本脚本属于"情绪与事件层",叠加在 finlib 量化信号之上,不做价格预测、
不算技术指标。对每只票用 Grok(grok-4.3 + web_search/x_search)实时检索
新闻/web/X(Twitter),产出结构化 stance/catalysts/risks,回答量价信号
回答不了的"为什么"。详见 grok_lib.py。

候选域:
  观察仓 = select.yml 的 US_SWING_MAG7/SEMIS/AI_CHAIN/HYPERSCALERS 并集
           (BAROMETERS 纯 ETF 不扫)
  持仓   = Futu 实时持仓 (OpenD 未开则优雅降级,只扫观察仓)
一只票可同时属于两类,扫描去重;输出按"持仓转空"优先排序——最该看的预警在最前。

输出:打印摘要 + result/us_sentiment_<date>.csv
成本:每只票约 $0.10-0.12 (grok-4.3 + ~4 次 server-side 检索)。同日已扫则跳过,
      --force_run 重扫。

用法:
  python t_us_sentiment.py                 # 持仓+观察仓全扫 (同日缓存)
  python t_us_sentiment.py --limit 3       # 试跑前 3 只
  python t_us_sentiment.py --hold-only     # 只扫持仓
  python t_us_sentiment.py --watch-only    # 只扫观察仓
  python t_us_sentiment.py --include-etf   # 连宽基/现金 ETF 一起扫
  python t_us_sentiment.py --force_run     # 忽略同日缓存重扫
"""
from __future__ import annotations

import os
import sys
import argparse
import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
import pandas as pd

import grok_lib

WATCHLIST_FILE = '/home/ryan/tushare_ryan/select.yml'
RESULT_DIR = '/home/ryan/DATA/result'
WATCH_KEYS = ['US_SWING_MAG7', 'US_SWING_SEMIS',
              'US_SWING_AI_CHAIN', 'US_SWING_HYPERSCALERS']
# 宽基/现金类 ETF:无个股级催化剂,默认不扫 (--include-etf 覆盖)
SKIP_ETF = {'SGOV', 'VOO', 'QQQ', 'TQQQ', 'SQQQ', 'SOXX', 'SOXL',
            'SPY', 'IVV', 'BIL', 'SHV', 'VTI', 'DIA'}
STANCE_ORDER = {'bearish': 0, 'unclear': 1, 'neutral': 2, 'bullish': 3}


def load_watchlist() -> set[str]:
    cfg = yaml.safe_load(open(WATCHLIST_FILE)) or {}
    out: set[str] = set()
    for k in WATCH_KEYS:
        for t in (cfg.get(k) or []):
            out.add(str(t).strip().upper())
    return out


def load_holdings() -> set[str]:
    """从 Futu 实时拉 US 持仓 ticker。OpenD 未开/报错则返回空集 (优雅降级)。"""
    try:
        from futu import (OpenSecTradeContext, TrdMarket, TrdEnv,
                          RET_OK, SecurityFirm)
    except Exception as e:
        print(f'  [holdings] futu 不可用,跳过持仓: {e!r}')
        return set()
    ctx = None
    try:
        ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US,
                                  host='127.0.0.1', port=11111,
                                  security_firm=SecurityFirm.FUTUSECURITIES)
        ret, data = ctx.position_list_query(trd_env=TrdEnv.REAL)
        if ret != RET_OK or data is None or len(data) == 0:
            print(f'  [holdings] 无持仓或查询失败: {data}')
            return set()
        codes = [str(c).split('.')[-1].upper() for c in data['code'].tolist()]
        return set(codes)
    except Exception as e:
        print(f'  [holdings] OpenD 未连或异常,只扫观察仓: {e!r}')
        return set()
    finally:
        if ctx is not None:
            try:
                ctx.close()
            except Exception:
                pass


def scan_one(ticker: str, sources: str, days_back: int, model: str) -> dict:
    """对单票扫描,异常包成一行,绝不让整批挂掉。"""
    try:
        d = grok_lib.sentiment_scan(ticker, model=model, days_back=days_back)
        meta = d.get('_meta', {})
        return {
            'ticker': ticker, 'source': sources,
            'stance': d.get('stance', 'unclear'),
            'confidence': d.get('confidence'),
            'summary': d.get('summary', ''),
            'catalysts': ' | '.join(d.get('catalysts', []) or []),
            'risks': ' | '.join(d.get('risks', []) or []),
            'key_facts': ' | '.join(d.get('key_facts', []) or []),
            'as_of': d.get('as_of', ''),
            'citations': ' '.join((meta.get('citations') or [])[:8]),
            'tokens': meta.get('total_tokens'),
            'cost_usd_est': meta.get('cost_usd_est'),
            'error': '',
        }
    except Exception as e:
        return {'ticker': ticker, 'source': sources, 'stance': 'ERROR',
                'confidence': None, 'summary': repr(e)[:200],
                'catalysts': '', 'risks': '', 'key_facts': '', 'as_of': '',
                'citations': '', 'tokens': None, 'cost_usd_est': None,
                'error': repr(e)[:200]}


STANCE_CN = {'bearish': '🔴 看空', 'unclear': '⚪ 不明',
             'neutral': '🟡 中性', 'bullish': '🟢 看多', 'ERROR': '❌ 失败'}


def _fmt_bullets(text: str) -> str:
    """把 ' | ' 分隔的串渲染成 markdown 子弹列表。"""
    parts = [p.strip() for p in str(text or '').split('|') if p.strip()]
    return '\n'.join(f'    - {p}' for p in parts) if parts else '    - (无)'


def _render_ticker(r: dict) -> str:
    conf = r.get('confidence')
    conf_s = f'{conf:.0%}' if isinstance(conf, (int, float)) else str(conf)
    lines = [
        f'### {r["ticker"]}  {STANCE_CN.get(r["stance"], r["stance"])}'
        f'  ·  把握 {conf_s}  ·  `{r["source"]}`',
        '',
        f'**结论**:{r.get("summary", "")}',
        '',
        '**催化剂**',
        _fmt_bullets(r.get('catalysts')),
        '',
        '**风险**',
        _fmt_bullets(r.get('risks')),
        '',
        '**事实依据**',
        _fmt_bullets(r.get('key_facts')),
    ]
    cites = [c for c in str(r.get('citations', '')).split() if c][:6]
    if cites:
        lines += ['', '**来源**:' + ' '.join(f'[{i+1}]({u})'
                                              for i, u in enumerate(cites))]
    as_of = r.get('as_of')
    if as_of:
        lines += ['', f'*{as_of}*']
    return '\n'.join(lines)


def write_markdown_report(rows: list[dict], path: str, total_cost: float,
                          n_hold: int, n_watch: int, model: str) -> None:
    """生成人类可读的 Markdown 报告。"""
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    holds = [r for r in rows if 'HOLD' in r['source']]
    watch_only = [r for r in rows if 'HOLD' not in r['source']]
    alerts = [r for r in holds if r['stance'] in ('bearish', 'unclear', 'ERROR')]
    dist = {}
    for r in rows:
        dist[r['stance']] = dist.get(r['stance'], 0) + 1
    dist_s = ' · '.join(f'{STANCE_CN.get(k, k)} {v}'
                        for k, v in sorted(dist.items(),
                                           key=lambda kv: STANCE_ORDER.get(kv[0], 9)))

    out = [f'# 美股情绪/催化剂报告 · {now}', '',
           f'> Grok 情绪层 ({model} + web/X 实时检索)。'
           f'扫描 {len(rows)} 只(持仓 {n_hold} / 观察 {n_watch})。',
           f'> 分布:{dist_s}。估算成本 ≈ ${total_cost:.2f}。', '',
           '本报告属"情绪与事件层",叠加在量价信号之上,**不构成交易指令**。', '',
           '---', '']

    # 1. 持仓预警(最该看)
    out += ['## ⚠️ 持仓预警(看空 / 不明)', '']
    if alerts:
        out += ['| 票 | 情绪 | 把握 | 一句话 |', '|---|---|---|---|']
        for r in alerts:
            conf = r.get('confidence')
            conf_s = f'{conf:.0%}' if isinstance(conf, (int, float)) else conf
            out.append(f'| **{r["ticker"]}** | {STANCE_CN.get(r["stance"])} '
                       f'| {conf_s} | {str(r["summary"])[:50]} |')
        out += ['', '详情见下方各票卡片。']
    else:
        out += ['_无持仓转空,本日无红色预警。_']
    out += ['', '---', '']

    # 2. 持仓全览
    out += ['## 📌 持仓全览', '']
    for r in holds:
        out += [_render_ticker(r), '', '---', '']

    # 3. 观察仓
    out += ['## 🔭 观察仓', '']
    for r in watch_only:
        out += [_render_ticker(r), '', '---', '']

    errs = [r for r in rows if r['stance'] == 'ERROR']
    if errs:
        out += ['## ❌ 扫描失败', '',
                ', '.join(r['ticker'] for r in errs), '']

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(out))


def main() -> int:
    ap = argparse.ArgumentParser(description='美股持仓×观察仓 Grok 情绪扫描')
    ap.add_argument('--hold-only', action='store_true', help='只扫持仓')
    ap.add_argument('--watch-only', action='store_true', help='只扫观察仓')
    ap.add_argument('--include-etf', action='store_true', help='纳入宽基/现金 ETF')
    ap.add_argument('--days-back', type=int, default=7, help='检索回溯天数')
    ap.add_argument('--limit', type=int, default=0, help='只扫前 N 只 (试跑)')
    ap.add_argument('--workers', type=int, default=4, help='并发数')
    ap.add_argument('--model', default=grok_lib.DEFAULT_MODEL)
    ap.add_argument('--force_run', action='store_true', help='忽略同日缓存重扫')
    args = ap.parse_args()

    date_str = datetime.datetime.now().strftime('%Y%m%d')
    # --limit 是试跑,写独立文件且不参与同日缓存,避免污染正式全量结果
    suffix = f'_partial{args.limit}' if args.limit else ''
    out_csv = f'{RESULT_DIR}/us_sentiment_{date_str}{suffix}.csv'
    if os.path.exists(out_csv) and not args.force_run and not args.limit:
        print(f'今日已扫,跳过 (--force_run 重扫): {out_csv}')
        print(pd.read_csv(out_csv).to_string(index=False))
        return 0

    watch = set() if args.hold_only else load_watchlist()
    hold = set() if args.watch_only else load_holdings()

    # ticker -> 来源标签
    src: dict[str, set[str]] = {}
    for t in hold:
        src.setdefault(t, set()).add('HOLD')
    for t in watch:
        src.setdefault(t, set()).add('WATCH')

    if not args.include_etf:
        # 宽基/现金类 ETF 无个股级催化剂,持仓里的也一并跳过
        for t in list(src):
            if t in SKIP_ETF:
                src.pop(t, None)

    tickers = sorted(src)
    if args.limit:
        tickers = tickers[:args.limit]
    if not tickers:
        print('无可扫标的 (OpenD 未开且 --hold-only?)')
        return 0

    src_label = {t: '+'.join(sorted(src[t])) for t in tickers}
    print(f'扫描 {len(tickers)} 只 (持仓 {len(hold)} / 观察 {len(watch)}), '
          f'model={args.model}, days_back={args.days_back}')
    print('  ', ', '.join(f'{t}[{src_label[t]}]' for t in tickers))

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(scan_one, t, src_label[t], args.days_back,
                          args.model): t for t in tickers}
        for fut in as_completed(futs):
            r = fut.result()
            rows.append(r)
            print(f'  ✓ {r["ticker"]:<6} {r["stance"]:<8} '
                  f'conf={r["confidence"]} | {r["summary"][:60]}')

    # 排序:持仓转空最前;同 stance 内按是否持仓、confidence
    def sort_key(r):
        is_hold = 'HOLD' in r['source']
        return (STANCE_ORDER.get(r['stance'], 9), 0 if is_hold else 1,
                -(r['confidence'] or 0) if isinstance(r['confidence'],
                                                      (int, float)) else 0)
    rows.sort(key=sort_key)

    total_cost = sum(r['cost_usd_est'] or 0 for r in rows)
    out_md = out_csv.rsplit('.', 1)[0] + '.md'
    df = pd.DataFrame(rows)
    if os.path.isdir(RESULT_DIR):
        df.to_csv(out_csv, index=False)
        write_markdown_report(rows, out_md, total_cost,
                              len(hold), len(watch), args.model)

    print('\n' + '=' * 70)
    print('情绪扫描结果 (持仓转空优先)')
    print('=' * 70)
    for r in rows:
        flag = '⚠️ ' if ('HOLD' in r['source'] and
                          r['stance'] in ('bearish', 'unclear')) else '   '
        print(f'{flag}{r["ticker"]:<6} [{r["source"]:<10}] '
              f'{r["stance"]:<8} conf={r["confidence"]}')
        print(f'      {r["summary"]}')
        if r['risks']:
            print(f'      风险: {r["risks"][:120]}')
    print('=' * 70)
    print(f'写出: {out_md}  (人类可读报告)')
    print(f'      {out_csv}  (流水线 CSV)  | 估算成本 ≈ ${total_cost:.2f}')
    errs = [r for r in rows if r['stance'] == 'ERROR']
    if errs:
        print(f'⚠️ {len(errs)} 只扫描失败: {[r["ticker"] for r in errs]}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
