# coding: utf-8
"""
US Sector Rotation — 板块/主题轮动发现层 (发现器, 与 t_us_breadth_diffusion 确认器配对)

回答"钱现在往哪个板块流": 对固定 ETF 字典逐一算【相对强度线 = ETF收盘/SPY收盘】,
按 skyte 同款加权收益(0.4·P63+0.2·P126+0.2·P189+0.2·P252, 算在相对线上而非绝对价)
排序, 并给每个板块贴当下状态(测状态不打分, 守 docs/indicator_design_state_vs_debt):

  LEADING   领跑    相对线 5 日内创 63 日新高 —— 跑赢确认
  EMERGING  转强    未创新高, 但相对线 5日加速 连正≥3日 且 21日相对收益>0 —— 最值钱的一档
  FADING    转弱    相对线自 63 日峰值回落 ≥3% 且加速≤0 —— 领跑衰减
  LAGGING   落后    加权相对强度<0 —— 只陈述, 不构成反向买入理由(不做讨债单)
  NEUTRAL   中性    其余

第 2 层·广度确认(防"一只权重股抬着 ETF 假跑赢"): 排名前 CONFIRM_TOP 的板块, 从
SP500∪NDX 池按 yfinance sector/industry 元数据取成分(缓存 30 天, 免手写名单烂掉),
复用 t_us_breadth_diffusion 的状态机(基准=该 ETF 自身)出广度状态:
  相对线新高 + 广度 ESTABLISHED/MATURE = 真风口;  + 广度中性/背离 = 权重股独角戏。
池内成分 < MIN_MEMBERS 的主题 ETF(GDX/URA/TAN 等)只给价格视角, 注明不确认。

数据: 全部走 t_us_tech_swing 的 yfinance 缓存(ADR-0001); --asof 挂 tsw._ASOF 点位
回放(注意: sector 元数据没有历史版本, 回放时成分仍是今天的口径)。

Usage:
  python t_us_sector_rotation.py                    # 日常: 排序表 + top3 广度确认
  python t_us_sector_rotation.py --no-confirm       # 只要排序表(快)
  python t_us_sector_rotation.py --asof 2025-04-01  # 历史某天的轮动快照
  python t_us_sector_rotation.py --refresh-meta     # 强制重拉 sector/industry 元数据
"""

import os
import sys
import json
import logging
import datetime
import argparse

import numpy as np
import pandas as pd

sys.path.append('/home/ryan/tushare_ryan')
import t_us_tech_swing as tsw                     # 数据层复用 (ADR-0001)
import t_us_breadth_diffusion as bd               # 状态机/面板复用 (确认层)

log = logging.getLogger('sector_rotation')

RESULT_DIR = '/home/ryan/DATA/result'
OUT_SUB    = 'us_sector_rotation'
META_CACHE = '/home/ryan/DATA/pickle/us_sector_industry.json'

# ── ETF 字典: 11 个 SPDR sector + 常看行业/主题。加减条目改这里即可 ──────────────
SECTOR_ETFS = {
    # SPDR sectors
    'XLK':  '科技',        'XLC':  '通信服务',    'XLY':  '可选消费',
    'XLP':  '必选消费',    'XLF':  '金融',        'XLV':  '医疗保健',
    'XLI':  '工业',        'XLE':  '能源',        'XLB':  '原材料',
    'XLU':  '公用事业',    'XLRE': '房地产',
    # industry / theme
    'SMH':  '半导体',      'IGV':  '软件',        'XBI':  '生物科技',
    'ITA':  '航空军工',    'KRE':  '区域银行',    'XME':  '金属矿业',
    'GDX':  '金矿',        'URA':  '铀/核电',     'TAN':  '太阳能',
}

# 广度确认: ETF → 从 SP500∪NDX 池按元数据选成分的规则 (field, [子串匹配, 不区分大小写])
# 没有规则的 ETF(GDX/URA/TAN: 成分多在池外)不做确认, 只给价格视角。
MEMBERS_RULE = {
    'XLK':  ('sector', ['technology']),
    'XLC':  ('sector', ['communication services']),
    'XLY':  ('sector', ['consumer cyclical']),
    'XLP':  ('sector', ['consumer defensive']),
    'XLF':  ('sector', ['financial services']),
    'XLV':  ('sector', ['healthcare']),
    'XLI':  ('sector', ['industrials']),
    'XLE':  ('sector', ['energy']),
    'XLB':  ('sector', ['basic materials']),
    'XLU':  ('sector', ['utilities']),
    'XLRE': ('sector', ['real estate']),
    'SMH':  ('industry', ['semiconductor']),
    'IGV':  ('industry', ['software']),
    'XBI':  ('industry', ['biotech']),
    'ITA':  ('industry', ['aerospace']),
    'KRE':  ('industry', ['banks']),
    'XME':  ('industry', ['steel', 'copper', 'gold', 'aluminum', 'industrial metals', 'coal']),
}

# ── 阈值(集中在此) ─────────────────────────────────────────────────────────────
BENCH_PRIMARY  = 'SPY'    # 相对线/排序基准 (跑赢"大盘")
BENCH_SECOND   = 'QQQ'    # 参考列: 对成长盘的相对表现
RS_WEIGHTS     = bd.RS_WEIGHTS          # {63:0.4, 126:0.2, 189:0.2, 252:0.2}
NEWHIGH_WINDOW = 63       # 相对线"新高"窗口, 与 breadth 的口径一致
LEAD_SYNC      = 5        # 新高距今 ≤5 日 → LEADING
REL_SHORT      = 21       # 短窗相对收益(转强的方向门槛)
ACCEL_LAG      = 5        # 加速度: rel21(t) − rel21(t-5)
EMERGE_STREAK  = 3        # 加速连正天数 → EMERGING
FADE_DD        = 0.03     # 相对线自 63 日峰值回落比例 → FADING
CONFIRM_TOP    = 3        # 广度确认前 N 名
MIN_MEMBERS    = 8        # 池内成分少于此不做广度确认
META_TTL_DAYS  = 30       # sector/industry 元数据缓存有效期

STATE_CN = {'LEADING': '领跑', 'EMERGING': '转强', 'NEUTRAL': '中性',
            'FADING': '转弱', 'LAGGING': '落后'}
STATE_ORD = {'LEADING': 0, 'EMERGING': 1, 'NEUTRAL': 2, 'FADING': 3, 'LAGGING': 4}


# ── sector/industry 元数据 (确认层成分来源) ────────────────────────────────────
def load_meta(universe: list, refresh: bool) -> dict:
    """{sym: {'sector':…, 'industry':…}} — yfinance .info, 缓存 META_TTL_DAYS。"""
    cache = {}
    if os.path.exists(META_CACHE):
        try:
            with open(META_CACHE) as fh:
                cache = json.load(fh)
        except Exception:
            cache = {}
    today = datetime.date.today()
    stale_cut = (today - datetime.timedelta(days=META_TTL_DAYS)).isoformat()
    todo = [s for s in universe
            if refresh or s not in cache or cache[s].get('ts', '') < stale_cut]
    if todo:
        import yfinance as yf
        from concurrent.futures import ThreadPoolExecutor

        def one(sym):
            try:
                info = yf.Ticker(sym).info or {}
                return sym, info.get('sector'), info.get('industry')
            except Exception as e:
                log.debug(f'{sym}: info failed ({e})')
                return sym, None, None

        log.info(f'拉取 sector/industry 元数据: {len(todo)} 票 (缓存 {META_TTL_DAYS} 天)')
        with ThreadPoolExecutor(max_workers=6) as ex:
            for i, (sym, sec, ind) in enumerate(ex.map(one, todo), 1):
                cache[sym] = {'sector': sec, 'industry': ind, 'ts': today.isoformat()}
                if i % 50 == 0:
                    log.info(f'  meta {i}/{len(todo)}')
        os.makedirs(os.path.dirname(META_CACHE), exist_ok=True)
        with open(META_CACHE, 'w') as fh:
            json.dump(cache, fh)
    return cache


def members_of(etf: str, universe: list, meta: dict) -> list:
    rule = MEMBERS_RULE.get(etf)
    if not rule:
        return []
    field, pats = rule
    out = []
    for s in universe:
        v = (meta.get(s, {}).get(field) or '').lower()
        if any(p in v for p in pats):
            out.append(s)
    return sorted(out)


# ── 第 1 层: 相对强度 + 状态 ───────────────────────────────────────────────────
def rel_metrics(close: pd.Series, bench: pd.Series) -> dict:
    """单 ETF vs 单基准: 相对线的加权强度/分窗收益/新高距离/加速度/状态。"""
    rs = (close / bench).dropna()
    if len(rs) < REL_SHORT + ACCEL_LAG + 1:
        return {}
    m = {}
    strength = 0.0
    for lag, w in RS_WEIGHTS.items():
        r = rs.iloc[-1] / rs.iloc[-1 - lag] - 1.0 if len(rs) > lag else np.nan
        m[f'rel{lag}'] = r
        strength = strength + w * r if pd.notna(r) else np.nan
    m['strength'] = strength

    hi = rs.rolling(NEWHIGH_WINDOW, min_periods=NEWHIGH_WINDOW).max()
    is_nh = rs >= hi
    m['nh_days_ago'] = int((len(is_nh) - 1) - np.max(np.nonzero(is_nh.values))) \
        if is_nh.any() else 10**6

    rel21 = rs.pct_change(REL_SHORT)
    m['rel21'] = rel21.iloc[-1]
    acc = rel21 - rel21.shift(ACCEL_LAG)
    m['accel'] = acc.iloc[-1]
    streak = bd._streak(acc > 0)
    m['peak_dd'] = 1.0 - rs.iloc[-1] / hi.iloc[-1] if pd.notna(hi.iloc[-1]) else np.nan

    if m['nh_days_ago'] <= LEAD_SYNC:
        st = 'LEADING'
    elif streak.iloc[-1] >= EMERGE_STREAK and rel21.iloc[-1] > 0:
        st = 'EMERGING'
    elif pd.notna(m['peak_dd']) and m['peak_dd'] >= FADE_DD and acc.iloc[-1] <= 0 \
            and m['nh_days_ago'] < NEWHIGH_WINDOW:
        st = 'FADING'
    elif pd.notna(strength) and strength < 0:
        st = 'LAGGING'
    else:
        st = 'NEUTRAL'
    m['state'] = st
    return m


def build_table() -> pd.DataFrame:
    spy = tsw._fetch_daily(BENCH_PRIMARY)['close']
    qqq = tsw._fetch_daily(BENCH_SECOND)['close']
    rows = {}
    for etf, name in SECTOR_ETFS.items():
        df = tsw._fetch_daily(etf)
        if df.empty:
            log.warning(f'{etf} 无数据, 跳过')
            continue
        m = rel_metrics(df['close'], spy)
        if not m:
            log.warning(f'{etf} 历史不足, 跳过')
            continue
        mq = rel_metrics(df['close'], qqq)
        m['rel63_qqq'] = mq.get('rel63', np.nan)
        m['name'] = name
        rows[etf] = m
    t = pd.DataFrame(rows).T
    # 排序: 状态优先(领跑/转强在前), 同状态按加权强度
    t['_ord'] = t['state'].map(STATE_ORD)
    return t.sort_values(['_ord', 'strength'], ascending=[True, False]).drop(columns='_ord')


# ── 第 2 层: 广度确认 (复用 breadth diffusion 状态机, 成分来自元数据) ──────────
def confirm_breadth(etf: str, members: list, pool_close: pd.DataFrame,
                    rs_pct: pd.DataFrame) -> str:
    cols = [s for s in members if s in pool_close.columns]
    if len(cols) < MIN_MEMBERS:
        return f'  {etf}: 池内成分仅 {len(cols)} 票 (<{MIN_MEMBERS}), 不做广度确认'
    bench = tsw._fetch_daily(etf)['close']
    s = bd.apply_state_machine(bd.compute_series(pool_close[cols], bench, rs_pct[cols]))
    r = s.iloc[-1]
    st = r['state'] or '预热'
    extra = []
    if r['f_ad_divergence']:
        extra.append('AD背离!')
    if r['f_rs_rollover']:
        extra.append('rsB自峰值回落!')
    return (f'  {etf} ({len(cols)} 票): 广度状态 {st} {bd.STATE_CN.get(r["state"], "")}'
            f'  — NH-NL {r["diff"]:+.0f}, %>50MA {r["pct_above_50ma"]:.0%}, '
            f'rsB {r["rs_breadth"]:.0%}' + ('  [' + ' '.join(extra) + ']' if extra else ''))


def rs_pct_panel(panel: pd.DataFrame) -> pd.DataFrame:
    """全池逐日截面 RS 百分位 (skyte 同款), 算一次供所有板块切片。"""
    strength = None
    for lag, w in RS_WEIGHTS.items():
        p = panel / panel.shift(lag) - 1.0
        strength = p * w if strength is None else strength + p * w
    return strength.rank(axis=1, pct=True) * 100.0


# ── 报告 ───────────────────────────────────────────────────────────────────────
def report(t: pd.DataFrame, confirm_lines: list, asof: str) -> str:
    lines = []
    p = lines.append
    p('=' * 100)
    p(f'US 板块/主题轮动 — 相对强度 (基准 {BENCH_PRIMARY}, 参考 {BENCH_SECOND})   asof {asof}')
    p('=' * 100)
    p('')
    p(f'{"ETF":<6}{"板块":<8}{"状态":<10}{"加权RS":>8}{"rel21d":>8}{"rel63d":>8}'
      f'{"rel126d":>9}{"vsQQQ63":>9}{"RS新高":>8}{"5日加速":>9}')
    for etf, r in t.iterrows():
        def f(v, fmt='{:+7.1%}'):
            return '    n/a' if pd.isna(v) else fmt.format(v)
        nh = f'{int(r["nh_days_ago"]):>4}日前' if r['nh_days_ago'] < 10**6 else '     无'
        p(f'{etf:<6}{r["name"]:<{10 - sum(1 for c in str(r["name"]) if ord(c) > 127)}}'
          f'{STATE_CN[r["state"]]:<{12 - sum(1 for c in STATE_CN[r["state"]] if ord(c) > 127)}}'
          f'{f(r["strength"])} {f(r["rel21"])} {f(r["rel63"])} {f(r["rel126"], "{:+8.1%}")}'
          f' {f(r["rel63_qqq"], "{:+8.1%}")} {nh} {f(r["accel"], "{:+8.1%}")}')
    p('')
    p('状态: 领跑=相对线5日内创63日新高; 转强=加速连正≥3日且21日相对收益>0(最值钱);')
    p('      转弱=自63日峰值回落≥3%且加速≤0; 落后=加权RS<0 (只陈述, 不是反向买入理由)')
    if confirm_lines:
        p('')
        p(f'广度确认 (前 {CONFIRM_TOP} 名, 成分=SP500∪NDX 池内按 GICS 匹配, 基准=ETF 自身):')
        lines.extend(confirm_lines)
        p('  → 领跑/转强 + 广度确立/成熟 = 真风口; 广度中性/背离 = 权重股独角戏(选股信号非板块信号)')
    p('=' * 100)
    return '\n'.join(lines)


# ── main ───────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description='板块/主题轮动发现层 (相对强度排序 + 广度确认)')
    ap.add_argument('--asof', help='YYYY-MM-DD 点位回放 (成分元数据仍为今日口径)')
    ap.add_argument('--no-confirm', action='store_true', help='跳过第2层广度确认(快)')
    ap.add_argument('--top', type=int, default=CONFIRM_TOP, help=f'确认前N名, 默认{CONFIRM_TOP}')
    ap.add_argument('--refresh-meta', action='store_true', help='强制重拉 sector/industry 元数据')
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S')
    if args.asof:
        tsw._ASOF = pd.Timestamp(args.asof)
        log.info(f'--asof {args.asof}: 数据层截断, 不偷看未来')

    t = build_table()

    confirm_lines = []
    if not args.no_confirm:
        confirmable = [e for e in t.index if e in MEMBERS_RULE][:args.top]
        skipped = [e for e in t.index[:args.top] if e not in MEMBERS_RULE]
        if confirmable:
            from t_us_undervalue import load_universe
            universe = load_universe('both', force=False)
            meta = load_meta(universe, args.refresh_meta)
            pool_close = bd.build_close_panel(universe)
            rs_pct = rs_pct_panel(pool_close)
            for etf in confirmable:
                confirm_lines.append(confirm_breadth(etf, members_of(etf, universe, meta),
                                                     pool_close, rs_pct))
        for etf in skipped:
            confirm_lines.append(f'  {etf}: 无成分规则(成分多在池外), 仅价格视角')

    asof = (tsw._ASOF or pd.Timestamp.today()).strftime('%Y-%m-%d')
    txt = report(t, confirm_lines, asof)
    print(txt)

    out_dir = os.path.join(RESULT_DIR, OUT_SUB)
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f'us_sector_rotation_{asof}.txt')
    with open(out, 'w') as fh:
        fh.write(txt + '\n')
    t.round(4).to_csv(os.path.join(out_dir, f'us_sector_rotation_{asof}.csv'))
    log.info(f'报告 → {out}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
