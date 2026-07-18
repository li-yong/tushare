# coding: utf-8
"""
Swell detector (无风有涌·SWELL 标注) — 价格有风而叙事无名的名字
(docs/tide_wave_wind.md §6 落地方向四; rule_registry #24). 提示不门控.

框架论断: 涌浪(swell)可以从几千公里外的风暴传来 — 本地无新闻但价格持续单向
推, 能量来自远方(别的板块/宏观资金面), 往往比有新闻的行情更可信, 因为涌浪
意味着源头风暴真实且巨大。low_bounce --grok 的镜像: 那边是"跌了查有没有
催化剂"(有=真反转), 这边是"涨了查是不是没有催化剂"(没有=SWELL)。

价格漏斗先行 (免费), Grok 殿后 (~$0.1/票, 只问前 TOPN 名):

  涌的形状 (三道价格门, 全池扫描):
    1. rel21 ≥ REL_MIN        — 21日跑赢 SPY 足够多 (有涌)
    2. maxday/cum ≤ SPIKE_FRAC — 碾磨式: 最大单日涨幅占累计涨幅比例低
                                 (单日暴动贡献大头的, 多半有名有姓, 那是
                                  gap_scan/earnings_react 的地盘)
    3. close ≥ NEAR_HIGH×63日高 — 贴近高位 (排除死猫跳, 那是 low_bounce 的地盘)
  名字的有无 (Grok swell_scan): news_intensity none/light = SWELL 确认
      (无人问津的涌浪); heavy = 有名之风 (正常行情, 非涌)。

n=0 假说 (纯标注, 前向积累): SWELL 名字的后续表现优于同涨幅的有名之风。
样本记 result/us_swell/swell_log.csv, ≥10 个成熟样本后第一次统计。

周日 us_weekly_run.sh 运行 (涌浪以周为观察尺度, 且控制 Grok 成本)。

Usage:
  python t_us_swell.py                 # weekly run (价格漏斗 + Grok 前 TOPN)
  python t_us_swell.py --no-grok      # 价格漏斗 only (免费, 不写 log)
  python t_us_swell.py --topn 5       # Grok 名额 (默认 8, ~$0.8/run)
"""

import os
import sys
import logging
import datetime
from optparse import OptionParser

import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)

import t_us_tech_swing as tsw                     # 数据层复用 (ADR-0001)
from t_us_undervalue import load_universe

OUT_DIR   = '/home/ryan/DATA/result/us_swell'
SWELL_LOG = os.path.join(OUT_DIR, 'swell_log.csv')

BENCH      = 'SPY'
UNIVERSE   = 'both'   # SP500∪NDX 质量池 — 涌浪要在有主语的池子里找 (方法论 §2.1)
WIN        = 21       # 涌的观察窗 (一个月的碾磨)
REL_MIN    = 0.08     # 21日相对 SPY 超额 ≥ +8%
SPIKE_FRAC = 0.40     # 最大单日涨幅 ≤ 累计涨幅的 40% (碾磨门)
NEAR_HIGH  = 0.95     # 现价 ≥ 63日高 × 0.95 (排除死猫跳)
TOPN       = 8        # Grok 名额

LOG_COLS = ['date', 'ticker', 'rel21', 'cum21', 'spike_frac', 'close',
            'news_intensity', 'is_swell', 'catalysts', 'summary', 'cost_usd']


# ── 价格漏斗: 涌的形状 ────────────────────────────────────────────────────────

def scan_price(universe: list) -> pd.DataFrame:
    bench = tsw._fetch_daily_full(BENCH)['close']
    b_ret = float(bench.iloc[-1] / bench.iloc[-1 - WIN] - 1)
    rows = []
    for t in universe:
        try:
            d = tsw._fetch_daily_full(t)
            c = d['close']
            if len(c) < 63 + 2:
                continue
            cum = float(c.iloc[-1] / c.iloc[-1 - WIN] - 1)
            rel = cum - b_ret
            if rel < REL_MIN or cum <= 0:
                continue
            daily = c.iloc[-WIN:].pct_change().dropna()
            spike = float(daily.max() / cum) if cum > 0 else 1.0
            if spike > SPIKE_FRAC:
                continue
            if float(c.iloc[-1]) < NEAR_HIGH * float(c.iloc[-63:].max()):
                continue
            rows.append({'ticker': t, 'rel21': rel, 'cum21': cum,
                         'spike_frac': spike, 'close': float(c.iloc[-1])})
        except Exception as e:
            logging.debug(f'{t}: {e}')
    df = pd.DataFrame(rows)
    return (df.sort_values('rel21', ascending=False).reset_index(drop=True)
            if len(df) else df)


# ── Grok: 名字的有无 ──────────────────────────────────────────────────────────

def check_names(cands: pd.DataFrame, topn: int, today: str) -> pd.DataFrame:
    import grok_lib
    log = (pd.read_csv(SWELL_LOG, dtype=str) if os.path.exists(SWELL_LOG)
           else pd.DataFrame(columns=LOG_COLS))
    # 本周已查过的不重复烧钱 (同一 episode 内价格门可能连续数周命中)
    recent_cut = (pd.Timestamp(today) - pd.Timedelta(days=6)).date().isoformat()
    seen = set(log[log['date'] >= recent_cut]['ticker']) if len(log) else set()

    out = []
    for r in cands.head(topn).itertuples():
        if r.ticker in seen:
            logging.info(f'{r.ticker}: 近7日已查过, 跳过')
            continue
        res = grok_lib.swell_scan(r.ticker, days_back=WIN)
        cost = (res.get('_meta') or {}).get('cost_usd_est') or 0.0
        rec = {'date': today, 'ticker': r.ticker,
               'rel21': round(r.rel21, 4), 'cum21': round(r.cum21, 4),
               'spike_frac': round(r.spike_frac, 3), 'close': round(r.close, 2),
               'news_intensity': res.get('news_intensity'),
               'is_swell': res.get('news_intensity') in ('none', 'light'),
               'catalysts': ' | '.join(res.get('catalysts') or []),
               'summary': res.get('summary'), 'cost_usd': cost}
        out.append(rec)
        logging.info(f"{r.ticker}: {rec['news_intensity']} "
                     f"{'SWELL✓' if rec['is_swell'] else '有名之风'} (${cost:.2f})")
    if out:
        log = pd.concat([log, pd.DataFrame(out)[LOG_COLS].astype(str)],
                        ignore_index=True)
        os.makedirs(OUT_DIR, exist_ok=True)
        log.to_csv(SWELL_LOG, index=False)
    return pd.DataFrame(out)


# ── Report ────────────────────────────────────────────────────────────────────

def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--no-grok', dest='no_grok', action='store_true', default=False,
                      help='价格漏斗 only (不调 Grok, 不写 log)')
    parser.add_option('--topn', dest='topn', default=TOPN, type='int',
                      help=f'Grok 复核名额 (默认 {TOPN}, ~$0.1/票)')
    parser.add_option('--universe', dest='universe', default=UNIVERSE,
                      help=f'池子 (默认 {UNIVERSE})')
    opts, _ = parser.parse_args()

    today = datetime.date.today().isoformat()
    lines = []

    def p(*args):
        line = ' '.join(str(a) for a in args)
        lines.append(line)
        print(line)

    p()
    p('=' * 78)
    p(f'  SWELL 无风有涌  —  {today}   (潮浪风框架落地四, 提示不门控)')
    p('=' * 78)

    universe = load_universe(opts.universe, False)
    logging.info(f'universe {opts.universe}: {len(universe)} names')
    cands = scan_price(universe)

    p()
    p(f'[ 价格漏斗 涌的形状 (池 {opts.universe}; rel21≥{REL_MIN:+.0%} · '
      f'单日贡献≤{SPIKE_FRAC:.0%} · 贴63日高≥{NEAR_HIGH:.0%}) ]')
    if not len(cands):
        p('  本周无命中 — 池内没有碾磨式跑赢的名字')
    else:
        for r in cands.head(15).itertuples():
            p(f'  {r.ticker:<6} rel21 {r.rel21:+6.1%} · 累计 {r.cum21:+6.1%} · '
              f'最大单日占 {r.spike_frac:>4.0%} · close {r.close:.2f}')
        if len(cands) > 15:
            p(f'  … 共 {len(cands)} 个命中')

    p()
    if opts.no_grok:
        p('[ 名字复核 ]  --no-grok, 本次跳过 (纯价格候选, 未标注)')
    elif not len(cands):
        p('[ 名字复核 ]  无候选, 不调 Grok')
    else:
        try:
            res = check_names(cands, opts.topn, today)
            n_total = (pd.read_csv(SWELL_LOG)['ticker'].count()
                       if os.path.exists(SWELL_LOG) else 0)
            cost = res['cost_usd'].sum() if len(res) else 0.0
            p(f'[ 名字复核 (Grok 前 {opts.topn} 名, ~${cost:.2f}; '
              f'log 累计 {n_total} 条) ]')
            if not len(res):
                p('  本周候选近7日均已查过, 无新增')
            for r in res.itertuples():
                mark = '🌊 SWELL' if r.is_swell else '   有名之风'
                p(f'  {mark}  {r.ticker:<6} [{r.news_intensity}] {r.summary}')
                if r.catalysts:
                    p(f'             催化剂: {r.catalysts}')
        except Exception as e:
            logging.warning(f'Grok 复核失败: {e}')
            p('[ 名字复核 ]  ⚠ Grok 不可用, 本周纯价格候选')

    p()
    p('[ 读法 ]')
    p('  SWELL = 涨得多且碾磨式, 但媒体无人问津 — 能量来自远方, 源头风暴真实且大;')
    p('  有名之风 = 涨幅有催化剂解释, 正常行情。n=0 假说: SWELL 后续优于同涨幅的')
    p('  有名之风; 样本在 swell_log.csv 前向积累, ≥10 个成熟样本后第一次统计。')
    p('  标注不门控, 不构成入场信号 (入场仍走各扫描器的 Setup 纪律)。registry #24。')
    p()

    os.makedirs(OUT_DIR, exist_ok=True)
    out_file = os.path.join(OUT_DIR, f'us_swell_{today}.txt')
    with open(out_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    logging.info(f'Swell report → {out_file}')


if __name__ == '__main__':
    main()
