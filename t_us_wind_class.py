# coding: utf-8
"""
Gust/monsoon classifier (阵风/季风分类器) — the wind layer's memory
(docs/tide_wave_wind.md §6 落地方向三; rule_registry #23). 提示不门控.

框架论断: 阵风(单日新闻, 制造一两个浪就散) 只值得浪级响应; 信风/季风(持续
数季的叙事, 连续把能量泵进同一片海域) 才值得仓位级响应。此前系统的风层是
无记忆的 — sector_rotation 是当日快照, Grok 扫描每次都是新问句。本脚本给
风装记忆, 两个断面:

  价格端 (客观, 全史立即可算) — 对 sector_rotation 的 20 只 ETF, 从 bars
      逐日重建对 SPY 的相对线: 持续性 = 近 PERSIST_WIN 日里相对线贴近其
      63 日高(≤NEAR_TOL)的天数占比; 新鲜度 = 相对线新高 ≤FRESH_NH_D 日
      或 rel21 ≥ FRESH_REL21。二维合成:
        季风 MONSOON        = 新鲜 × 高持续    → 仓位级响应
        阵风 GUST           = 新鲜 × 低持续    → 浪级响应, 不上仓位
        成势 BUILDING       = 新鲜 × 中持续    → 观察升级
        季风间歇 MONSOON_PAUSE = 不新鲜 × 高持续 → 已持有的复查, 非新入场
        无风 CALM           = 其余
  叙事端 (Grok, 前向积累) — 每周一次 narrative_scan (~$0.1): 本周主导叙事
      (最多6条), slug 由模型给但历史清单喂回 prompt 强制复用(跨周身份)。
      persistence = 该 slug 出现过的周数: 1=新阵风, 2=成势, ≥3=季风叙事;
      上周还是季风、本周消失 = 季风消退(值得注意的退风事件)。
      日志 result/us_wind_class/narrative_log.csv, 无法回填, 越早积累越值钱。

周日 us_weekly_run.sh 运行(季风以周为最小分辨率)。--asof 只作用于价格端
(bars 截断, 叙事端跳过 — Grok 无法诚实地 point-in-time)。

Usage:
  python t_us_wind_class.py                # weekly run (价格端 + Grok 叙事端)
  python t_us_wind_class.py --no-grok      # 价格端 only (省 $0.1 / 无 key 时)
  python t_us_wind_class.py --asof 2024-06-03   # 价格端历史重放 (验证/校准)
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
from t_us_sector_rotation import SECTOR_ETFS

OUT_DIR  = '/home/ryan/DATA/result/us_wind_class'
NARR_LOG = os.path.join(OUT_DIR, 'narrative_log.csv')

BENCH        = 'SPY'
NH_WIN       = 63     # 相对线"新高"窗口 (与 sector_rotation 口径一致)
PERSIST_WIN  = 126    # 持续性回看窗 (~半年: 季风的最小时间尺度)
NEAR_TOL     = 0.03   # 相对线距其 63 日高 ≤3% 算"贴近高位"
FRESH_NH_D   = 10     # 新高距今 ≤10 日 = 新鲜
FRESH_REL21  = 0.03   # 或 21 日相对收益 ≥ +3% = 新鲜 (还没创高的阵风)
MONSOON_P    = 0.50   # 持续性 ≥ 此 → 季风档
GUST_P       = 0.25   # 持续性 < 此 → 阵风档

CLS_CN = {'MONSOON': '季风', 'GUST': '阵风', 'BUILDING': '成势',
          'MONSOON_PAUSE': '季风间歇', 'CALM': '无风'}
CLS_ACTION = {'MONSOON': '仓位级 — 值得追着配置的风',
              'GUST': '浪级 — 只做单浪, 不上仓位',
              'BUILDING': '观察升级 — 阵风在长成季风的路上',
              'MONSOON_PAUSE': '持有复查 — 老风歇脚, 非新入场理由',
              'CALM': '—'}
NARR_CN = {1: '新阵风', 2: '成势'}


# ── 价格端: 相对线持续性 × 新鲜度 ─────────────────────────────────────────────

def classify_etf(rel: pd.Series) -> dict | None:
    """rel = ETF/SPY 相对线 (date-indexed). 返回分类与读数。"""
    if len(rel) < NH_WIN + PERSIST_WIN + 22:
        return None
    roll_max = rel.rolling(NH_WIN).max()
    near = (rel >= roll_max * (1 - NEAR_TOL)).iloc[-PERSIST_WIN:]
    persist = float(near.mean())
    is_nh = rel >= roll_max - 1e-12
    nh_idx = is_nh[is_nh].index
    nh_days_ago = (len(rel) - 1 - rel.index.get_loc(nh_idx[-1])) if len(nh_idx) else None
    rel21 = float(rel.iloc[-1] / rel.iloc[-22] - 1)
    fresh = ((nh_days_ago is not None and nh_days_ago <= FRESH_NH_D)
             or rel21 >= FRESH_REL21)
    if fresh:
        cls = ('MONSOON' if persist >= MONSOON_P
               else 'GUST' if persist < GUST_P else 'BUILDING')
    else:
        cls = 'MONSOON_PAUSE' if persist >= MONSOON_P else 'CALM'
    return {'cls': cls, 'persist': persist, 'nh_days_ago': nh_days_ago,
            'rel21': rel21}


def price_wind(asof: str | None) -> pd.DataFrame:
    cut = pd.Timestamp(asof) if asof else None
    bench = tsw._fetch_daily_full(BENCH)['close']
    if cut is not None:
        bench = bench.loc[:cut]
    rows = []
    for etf, name in SECTOR_ETFS.items():
        try:
            c = tsw._fetch_daily_full(etf)['close']
            if cut is not None:
                c = c.loc[:cut]
            rel = (c / bench.reindex(c.index).ffill()).dropna()
            r = classify_etf(rel)
            if r:
                rows.append({'etf': etf, 'name': name, **r})
        except Exception as e:
            logging.warning(f'{etf}: {e}')
    order = ['MONSOON', 'BUILDING', 'GUST', 'MONSOON_PAUSE', 'CALM']
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df['ord'] = df['cls'].map(order.index)
    return df.sort_values(['ord', 'persist'], ascending=[True, False]).drop(columns='ord')


# ── 叙事端: Grok 主导叙事 + 前向持续性 ────────────────────────────────────────

def load_narr_log() -> pd.DataFrame:
    if os.path.exists(NARR_LOG):
        return pd.read_csv(NARR_LOG, dtype=str)
    return pd.DataFrame(columns=['date', 'slug', 'title_cn', 'direction',
                                 'sectors', 'tickers', 'one_line'])


def narrative_wind(today: str) -> tuple[list, list, float] | None:
    """Grok 扫描 + 记日志; 返回 (本周叙事带周数, 消退的季风, 成本$)。"""
    import grok_lib
    log = load_narr_log()
    # 近 12 周的 slug 清单喂回 prompt (跨周身份)
    recent = log[log['date'] >= (pd.Timestamp(today) - pd.Timedelta(weeks=12)).date().isoformat()]
    known = list(recent.drop_duplicates('slug', keep='last')[['slug', 'title_cn']]
                 .itertuples(index=False, name=None))
    res = grok_lib.narrative_scan(known_slugs=known)
    cost = (res.get('_meta') or {}).get('cost_usd_est') or 0.0
    narrs = res.get('narratives') or []
    if not narrs:
        logging.warning('narrative_scan 返回空 (解析失败或无叙事)')
        return [], [], cost

    rows = [{'date': today, 'slug': n['slug'], 'title_cn': n['title_cn'],
             'direction': n['direction'], 'sectors': '|'.join(n.get('sectors') or []),
             'tickers': '|'.join(n.get('tickers') or []), 'one_line': n['one_line']}
            for n in narrs]
    log = log[log['date'] != today] if len(log) else log
    log = pd.concat([log, pd.DataFrame(rows)], ignore_index=True).sort_values(['date', 'slug'])
    os.makedirs(OUT_DIR, exist_ok=True)
    log.to_csv(NARR_LOG, index=False)

    weeks_seen = log.groupby('slug')['date'].nunique()
    cur = [{**n, 'weeks': int(weeks_seen.get(n['slug'], 1))} for n in narrs]
    # 季风消退: 历史 ≥3 周的 slug 本周缺席 (只看近 12 周内还活跃过的)
    cur_slugs = {n['slug'] for n in narrs}
    faded = [(s, recent[recent['slug'] == s]['title_cn'].iloc[-1])
             for s in recent['slug'].unique()
             if s not in cur_slugs and weeks_seen.get(s, 0) >= 3]
    return cur, faded, cost


# ── Report ────────────────────────────────────────────────────────────────────

def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--no-grok', dest='no_grok', action='store_true', default=False,
                      help='跳过叙事端 Grok 调用 (价格端 only)')
    parser.add_option('--asof', dest='asof', default=None,
                      help='价格端历史重放 YYYY-MM-DD (叙事端自动跳过, 不写日志)')
    opts, _ = parser.parse_args()

    today = datetime.date.today().isoformat()
    asof = opts.asof
    lines = []

    def p(*args):
        line = ' '.join(str(a) for a in args)
        lines.append(line)
        print(line)

    p()
    p('=' * 78)
    p(f'  WIND CLASS 阵风/季风分类  —  {asof or today}'
      + ('   [--asof 重放, 仅价格端]' if asof else '')
      + '   (潮浪风框架落地三, 提示不门控)')
    p('=' * 78)

    # 价格端
    p()
    p(f'[ 价格端 相对线持续性 (基准 {BENCH}; 持续=近{PERSIST_WIN}日贴近63日RS高'
      f'≤{NEAR_TOL:.0%}的占比) ]')
    df = price_wind(asof)
    if df.empty:
        p('  ⚠ 无可分类读数 — bars 纵深不足 (数据层3年窗, 需211交易日热身,'
          ' --asof 最早约为2年9个月前)')
    else:
        p(f'  {"ETF":<5} {"板块":<6} {"分类":<10} {"持续":>5} {"RS新高":>7} '
          f'{"rel21":>7}   响应档位')
    for r in df.itertuples():
        nh = f'{r.nh_days_ago}日前' if r.nh_days_ago is not None else '—'
        p(f'  {r.etf:<5} {r.name:<6} {CLS_CN[r.cls]:<9} {r.persist:>5.0%} '
          f'{nh:>7} {r.rel21:>+7.1%}   {CLS_ACTION[r.cls]}')

    # 叙事端
    p()
    if asof:
        p('[ 叙事端 ]  --asof 重放不调 Grok (web 检索无法诚实 point-in-time)')
    elif opts.no_grok:
        p('[ 叙事端 ]  --no-grok, 本次跳过 (日志缺一周, 持续性计数会偏低)')
    else:
        try:
            out = narrative_wind(today)
            cur, faded, cost = out
            n_weeks = load_narr_log()['date'].nunique()
            p(f'[ 叙事端 主导叙事 (Grok, ~${cost:.2f}; 前向日志第 {n_weeks} 周) ]')
            if not cur:
                p('  本周扫描无叙事返回 (见 log)')
            for n in cur:
                tag = NARR_CN.get(n['weeks'], '季风叙事' if n['weeks'] >= 3 else '?')
                dir_cn = {'risk_on': '顺风', 'risk_off': '逆风',
                          'sector_specific': '板块'}.get(n['direction'], '?')
                tk = (' [' + ','.join((n.get('tickers') or [])[:5]) + ']'
                      if n.get('tickers') else '')
                p(f'  {tag:<4}·第{n["weeks"]}周 ({dir_cn}) {n["title_cn"]}{tk}')
                p(f'         {n["one_line"]}')
            for slug, title in faded:
                p(f'  ⚠ 季风消退: {title} ({slug}) — 历史≥3周, 本周缺席; 相关持仓复查')
            if n_weeks < 3:
                p('  注: 日志积累 <3 周, 所有叙事的周数都被低估 — 分类先别当真')
        except Exception as e:
            logging.warning(f'叙事端失败: {e}')
            p('[ 叙事端 ]  ⚠ Grok 不可用, 本周价格端 only')

    p()
    p('[ 读法 ]')
    p('  季风才值得仓位级响应; 阵风只做单浪。价格端持续性是客观刻度, 叙事端周数')
    p('  是前向积累 (无法回填, 早积累早值钱)。两端印证 = 真季风; 叙事有风价格无风,')
    p('  多半是还没落地的故事; 价格有风叙事无名, 是无风有涌 — 反而更可信。')
    p('  全部提示不门控 (registry #23)。')
    p()

    if not asof:
        os.makedirs(OUT_DIR, exist_ok=True)
        out_file = os.path.join(OUT_DIR, f'us_wind_class_{today}.txt')
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        logging.info(f'Wind class report → {out_file}')


if __name__ == '__main__':
    main()
