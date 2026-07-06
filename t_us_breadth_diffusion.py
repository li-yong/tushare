# coding: utf-8
"""
US Breadth Diffusion — 板块广度扩散检测(spec: docs/breadth_diffusion_framework.md)

核心思想: 单票信号(RS突破/放量)是噪音候选, 板块级【广度扩散】才是"风口确立"的
确认。四个指标构成 领先→同步→滞后 序列, 同一组指标反向使用即检测衰竭:

  1. NH-NL 扩散速度   diff = 252日新高家数 − 新低家数; diff_accel = diff − diff(t-5)
  2. % Above 50MA     板块内收盘价站上50日线的占比 (Zweig thrust: <30% ≤10日内穿70%)
  3. A/D Line         板块内涨跌家数累积线, 与基准指数新高做同步/背离对比
  4. rs_breadth       板块内 RS百分位>80 的占比(最领先) —— RS百分位对接 skyte
                      relative-strength 管线输出(--rs-csv), 无则内部按同款公式算:
                      加权季度收益 0.4·P63+0.2·P126+0.2·P189+0.2·P252, 在参考池
                      (默认 SP500∪NDX)内做截面百分位。注意: 百分位必须在【大盘参考
                      池】内排, 若只在板块内部排, rs_breadth>80 占比恒等于 ~20%,
                      指标退化 —— 这正是要对接全市场 skyte 输出的原因。

状态机(明确阈值 + 二阶导, 不做主观打分/综合分 —— 守 docs/indicator_design_state_vs_debt,
只测当下状态, 不做 debt-assuming 反向赌注):

  IGNITION    早期点火   rs_breadth_accel 连续≥3日为正 且 仍在加速
  ESTABLISHED 确立       diff_accel 连续≥5日为正 且 pct_above_50ma>50%;
                         或 10日内出现过 Zweig thrust(<30%→>70% ≤10日)
  MATURE      成熟       AD Line 与基准指数 5日内同步创 63日新高(可加仓阶段)
  EXHAUSTION  衰竭预警   指数新高但 AD ≥10日未新高(背离), 或 rs_breadth 自63日
                         峰值回落≥15%(峰值≥20%)而指数仍在高位 97% 以内
  优先级: EXHAUSTION > MATURE > ESTABLISHED > IGNITION > NEUTRAL

机械扰动过滤(spec 要求4): 所有占比用【当日合格家数】做分母(自适应成分变动);
NH/NL 只统计满 252 根历史的票; A/D 只统计 t-1、t 两日都有收盘价的票; 合格家数
单日跳变>5% 视为再平衡/成分变更扰动日, 之后 5 日的二阶导(diff_accel /
rs_breadth_accel)置 NaN, 状态机不在虚假跳变上触发。

数据: 复用 t_us_tech_swing 的 yfinance 缓存(_fetch_daily, 守 ADR-0001 stale-cache
兜底); --asof 直接挂 tsw._ASOF, 全部序列截断到该日, 不偷看未来。

Usage:
  # 板块 = select.yml watchlist, 内部算 RS(参考池 SP500∪NDX), 基准 SMH
  python t_us_breadth_diffusion.py --watchlist US_SWING_SEMIS --benchmark SMH

  # 板块 = 手写列表, 对接已有 skyte RS percentile CSV (ticker,date,rs_percentile)
  python t_us_breadth_diffusion.py --tickers NVDA,AMD,AVGO,MU,TSM --rs-csv rs.csv

  # 整个 NDX 当板块看(基准 QQQ), 历史某天的快照
  python t_us_breadth_diffusion.py --pool ndx --asof 2025-03-01

  # 回测: thrust/确立 事件后 N 日基准表现
  python t_us_breadth_diffusion.py --watchlist US_SWING_SEMIS --benchmark SMH \\
      --backtest --start 2024-01-01 --end 2026-06-30
"""

import os
import sys
import logging
import datetime
import argparse

import numpy as np
import pandas as pd

sys.path.append('/home/ryan/tushare_ryan')
import t_us_tech_swing as tsw               # 数据层复用 (ADR-0001)

log = logging.getLogger('breadth_diffusion')

RESULT_DIR     = '/home/ryan/DATA/result'
OUT_SUB        = 'us_breadth_diffusion'
WATCHLIST_FILE = '/home/ryan/tushare_ryan/select.yml'

# ── 阈值(spec 要求5: 明确阈值判定, 全部集中在此, 不散落) ──────────────────────
NHNL_WINDOW      = 252    # 52周新高/新低窗口
ACCEL_LAG        = 5      # 二阶导窗口: x(t) − x(t-5)
MA_N             = 50     # % Above 50MA
RS_TOP           = 80.0   # RS百分位阈值 (>80 记入 rs_breadth)
RS_WEIGHTS       = {63: 0.4, 126: 0.2, 189: 0.2, 252: 0.2}   # skyte 加权季度
IGNITION_STREAK  = 3      # rs_breadth_accel 连续为正天数 → 点火
ESTABLISH_STREAK = 5      # diff_accel 连续为正天数 → 确立 (spec: 5-10日)
PCT50_CONFIRM    = 0.50   # 确立需 pct_above_50ma 突破 50%
ZWEIG_LOW        = 0.30   # Zweig thrust: 从 <30% ...
ZWEIG_HIGH       = 0.70   # ... 穿越 70%
ZWEIG_MAXDAYS    = 10     # ... 用时 ≤10 个交易日
ZWEIG_VALID_DAYS = 10     # thrust 出现后 N 日内直接视作"确立"
NEWHIGH_WINDOW   = 63     # AD/指数/rs_breadth 的"新高/峰值"滚动窗口(一个季度)
SYNC_DAYS        = 5      # AD 与指数新高的"同步"容差
DIVERGE_DAYS     = 10     # 指数新高但 AD ≥N日未新高 → 背离
RS_PEAK_DD       = 0.15   # rs_breadth 自峰值回落比例 → 衰竭预警
RS_PEAK_MIN      = 0.20   # 峰值至少到过 20%, 否则回落无意义
BENCH_NEAR_HIGH  = 0.97   # "指数仍在高位" = 收盘 ≥ 63日最高 × 0.97
MEMBER_JUMP      = 0.05   # 合格家数单日变动>5% → 机械扰动日
MIN_COVERAGE     = 0.30   # 有价票占比<30% 的远古区段整段砍掉(零星几票撑不起广度)
PCT80_LONG       = 20     # 辅助flag: pct_above 连续 N 日 >80% 且走平 → 后期
EVENT_DEDUP      = 20     # 回测: 事件去重窗口(交易日)

STATES = ['NEUTRAL', 'IGNITION', 'ESTABLISHED', 'MATURE', 'EXHAUSTION_WARN']
STATE_CN = {
    'NEUTRAL':         '中性',
    'IGNITION':        '早期点火',
    'ESTABLISHED':     '确立',
    'MATURE':          '成熟(可加仓)',
    'EXHAUSTION_WARN': '衰竭预警',
}


# ── 股票池 ─────────────────────────────────────────────────────────────────────
def load_sector(args) -> tuple:
    """返回 (label, tickers)。三选一: --tickers / --watchlist / --pool。"""
    if args.tickers:
        syms = [s.strip().upper() for s in args.tickers.split(',') if s.strip()]
        return ('custom', syms)
    if args.watchlist:
        import yaml
        with open(WATCHLIST_FILE) as fh:
            cfg = yaml.safe_load(fh) or {}
        rows = cfg.get(args.watchlist)
        if not rows:
            raise SystemExit(f'select.yml 里没有 {args.watchlist}')
        syms = []
        for r in rows:                       # 兼容 bare ticker / {TICKER: label}
            syms.append(list(r.keys())[0] if isinstance(r, dict) else str(r))
        return (args.watchlist.lower(), [s.upper() for s in syms])
    from t_us_undervalue import load_universe
    syms = load_universe(args.pool, force=False)
    if not syms:
        raise SystemExit(f'股票池 {args.pool} 抓取失败且无缓存')
    return (args.pool, syms)


# ── 价格面板 ───────────────────────────────────────────────────────────────────
def build_close_panel(tickers: list) -> pd.DataFrame:
    """dates × tickers 的收盘价面板, 逐票走 tsw 缓存(--asof 已在数据层截断)。"""
    cols = {}
    for i, sym in enumerate(tickers, 1):
        df = tsw._fetch_daily(sym)
        if not df.empty:
            cols[sym] = df['close']
        if i % 50 == 0:
            log.info(f'  bars {i}/{len(tickers)}')
    if not cols:
        raise SystemExit('一根K线都没拿到, 检查网络/缓存')
    panel = pd.DataFrame(cols).sort_index()
    ok = panel.notna().sum(axis=1) >= MIN_COVERAGE * panel.shape[1]
    if ok.any():
        panel = panel.loc[ok.idxmax():]      # 首个覆盖达标日起(部分缓存有几十年历史)
    log.info(f'价格面板: {panel.shape[1]} 票 × {panel.shape[0]} 日 '
             f'({panel.index[0].date()} → {panel.index[-1].date()})')
    return panel


# ── RS 百分位(指标4的输入): skyte CSV 对接, 或内部同款公式 ─────────────────────
def rs_percentile_from_csv(path: str, sector: list, dates: pd.DatetimeIndex) -> pd.DataFrame:
    """
    对接 skyte 管线输出。接受 tidy 长表, 列名不区分大小写, 要求含:
    ticker/symbol、date、rs_percentile/rs/percentile 三列。→ dates × sector 面板。
    """
    raw = pd.read_csv(path)
    cmap = {c.lower(): c for c in raw.columns}
    tcol = next((cmap[k] for k in ('ticker', 'symbol') if k in cmap), None)
    dcol = cmap.get('date')
    vcol = next((cmap[k] for k in ('rs_percentile', 'rs', 'percentile') if k in cmap), None)
    if not (tcol and dcol and vcol):
        raise SystemExit(f'--rs-csv 需要 ticker/date/rs_percentile 三列, 实际: {list(raw.columns)}')
    raw[dcol] = pd.to_datetime(raw[dcol])
    wide = raw.pivot_table(index=dcol, columns=tcol, values=vcol, aggfunc='last')
    wide.columns = [str(c).upper() for c in wide.columns]
    keep = [s for s in sector if s in wide.columns]
    missing = sorted(set(sector) - set(keep))
    if missing:
        log.warning(f'--rs-csv 缺 {len(missing)} 票的 RS: {",".join(missing[:10])}...')
    # 周频 skyte 输出也能用: 前向填充最多 5 个交易日
    return wide[keep].reindex(dates).ffill(limit=5)


def rs_percentile_internal(sector_panel: pd.DataFrame, ref_pool: str) -> pd.DataFrame:
    """
    skyte 同款: strength = Σ w_q · P(q日收益), 在【参考池 ∪ 板块】内逐日截面
    百分位(0-100), 取板块列。相对 QQQ: 每票减去同一个 QQQ strength 不改变截面
    排名, 故直接用绝对 strength 排名, 与 QQQ 基准版百分位逐点相等。
    """
    from t_us_undervalue import load_universe
    ref = load_universe(ref_pool, force=False)
    if not ref:
        log.warning(f'参考池 {ref_pool} 拿不到, 退化为板块内部排名(rs_breadth 会失真!)')
        ref = []
    extra = sorted(set(ref) - set(sector_panel.columns))
    log.info(f'RS 参考池 {ref_pool}: 板块外还需 {len(extra)} 票的历史')
    if len(extra) < len(sector_panel.columns):
        log.warning('板块占参考池比重过半: rs_breadth 会退化为常数(自己在自己内部排名, '
                    '>80分位恒≈20%), 点火/衰竭① 信号失效 —— 换更大的 --rs-ref 或对接 --rs-csv')
    panel = sector_panel
    if extra:
        panel = pd.concat([sector_panel, build_close_panel(extra)], axis=1).sort_index()

    strength = None
    for lag, w in RS_WEIGHTS.items():
        p = panel / panel.shift(lag) - 1.0
        strength = p * w if strength is None else strength + p * w
    pct = strength.rank(axis=1, pct=True) * 100.0     # 逐日截面百分位, NaN 不参与
    return pct[list(sector_panel.columns)]


# ── 四条时间序列 + 扰动过滤 ────────────────────────────────────────────────────
def _streak(cond: pd.Series) -> pd.Series:
    """连续 True 天数(False 处归零); NaN 视为 False。"""
    c = cond.fillna(False).astype(bool)
    grp = (~c).cumsum()
    return c.groupby(grp).cumsum().astype(int)


def _days_since(cond: pd.Series) -> pd.Series:
    """距最近一次 True 的交易日数(含当日=0), 从未 True 处为大数。"""
    c = cond.fillna(False).astype(bool)
    idx = pd.Series(np.arange(len(c)), index=c.index, dtype=float)
    last = idx.where(c).ffill()
    return (idx - last).fillna(np.inf)


def compute_series(close: pd.DataFrame, bench: pd.Series,
                   rs_pct: pd.DataFrame) -> pd.DataFrame:
    """四条序列 + 合格家数 + 扰动标记。close: dates × sector tickers。"""
    out = pd.DataFrame(index=close.index)

    # 1. NH-NL: 只统计满 252 根历史的票 (min_periods 硬约束 = 成分变更过滤)
    roll_max = close.rolling(NHNL_WINDOW, min_periods=NHNL_WINDOW).max()
    roll_min = close.rolling(NHNL_WINDOW, min_periods=NHNL_WINDOW).min()
    eligible = roll_max.notna()
    out['eligible'] = eligible.sum(axis=1)
    out['nh'] = ((close >= roll_max) & eligible).sum(axis=1)
    out['nl'] = ((close <= roll_min) & eligible).sum(axis=1)
    out['diff'] = out['nh'] - out['nl']
    out['diff_pct'] = out['diff'] / out['eligible'].replace(0, np.nan)   # 池子大小无关版
    out['diff_accel'] = out['diff'] - out['diff'].shift(ACCEL_LAG)

    # 2. % Above 50MA (分母 = 当日有 MA50 的票)
    ma = close.rolling(MA_N, min_periods=MA_N).mean()
    out['pct_above_50ma'] = (close > ma).sum(axis=1) / ma.notna().sum(axis=1).replace(0, np.nan)

    # 3. A/D Line: diff() 天然只统计前后两日都有价的票 (新增/退出成分当日不计)
    chg = close.diff()
    out['advances'] = (chg > 0).sum(axis=1)
    out['declines'] = (chg < 0).sum(axis=1)
    out['ad_line'] = (out['advances'] - out['declines']).cumsum()

    # 4. rs_breadth (分母 = 当日有 RS 值的票)
    rs_pct = rs_pct.reindex(close.index)
    rs_n = rs_pct.notna().sum(axis=1)
    out['rs_breadth'] = (rs_pct > RS_TOP).sum(axis=1) / rs_n.replace(0, np.nan)
    out['rs_breadth_accel'] = out['rs_breadth'] - out['rs_breadth'].shift(ACCEL_LAG)

    # 机械扰动(spec 要求4): 合格家数单日跳变 → 之后 ACCEL_LAG 日二阶导作废
    jump = out['eligible'].pct_change().abs() > MEMBER_JUMP
    out['disturbed'] = jump
    tainted = jump.rolling(ACCEL_LAG + 1, min_periods=1).max().astype(bool)
    out.loc[tainted, ['diff_accel', 'rs_breadth_accel']] = np.nan

    out['bench_close'] = bench.reindex(close.index)
    return out


# ── 状态机(spec 要求5: 阈值 + 二阶导, 无打分) ─────────────────────────────────
def apply_state_machine(s: pd.DataFrame) -> pd.DataFrame:
    pct, rs_b = s['pct_above_50ma'], s['rs_breadth']
    bench, ad = s['bench_close'], s['ad_line']

    # 点火: rs_breadth_accel 连续为正 且 仍在加速(今日 accel > 3日前 accel)
    acc = s['rs_breadth_accel']
    s['f_ignition'] = (_streak(acc > 0) >= IGNITION_STREAK) & \
                      (acc > acc.shift(IGNITION_STREAK))

    # Zweig thrust: 今日上穿 70%, 且过去 10 日内曾 <30%
    cross = (pct >= ZWEIG_HIGH) & (pct.shift(1) < ZWEIG_HIGH)
    s['f_zweig_thrust'] = cross & (pct.rolling(ZWEIG_MAXDAYS, min_periods=1).min().shift(1) < ZWEIG_LOW)

    # 确立: diff_accel 连正≥5 且 pct>50%; 或 近10日出现过 thrust
    s['f_established'] = ((_streak(s['diff_accel'] > 0) >= ESTABLISH_STREAK) & (pct > PCT50_CONFIRM)) \
                         | (_days_since(s['f_zweig_thrust']) <= ZWEIG_VALID_DAYS)

    # 成熟: AD 与指数 5 日内同步创 63 日新高
    bench_nh = bench >= bench.rolling(NEWHIGH_WINDOW, min_periods=NEWHIGH_WINDOW).max()
    ad_nh    = ad    >= ad.rolling(NEWHIGH_WINDOW, min_periods=NEWHIGH_WINDOW).max()
    recent_b = bench_nh.rolling(SYNC_DAYS, min_periods=1).max().astype(bool)
    recent_a = ad_nh.rolling(SYNC_DAYS, min_periods=1).max().astype(bool)
    s['f_mature'] = recent_b & recent_a

    # 衰竭①: 指数近5日新高, 但 AD ≥10日没跟上 → leadership narrowing
    s['f_ad_divergence'] = recent_b & (_days_since(ad_nh) >= DIVERGE_DAYS)

    # 衰竭②: rs_breadth 自峰值回落≥15%(峰值曾≥20%)且仍在回落(accel≤0, 排除
    # 回升途中的误报), 指数却仍贴着高位
    rs_peak = rs_b.rolling(NEWHIGH_WINDOW, min_periods=ACCEL_LAG).max()
    near_hi = bench >= BENCH_NEAR_HIGH * bench.rolling(NEWHIGH_WINDOW, min_periods=NEWHIGH_WINDOW).max()
    s['f_rs_rollover'] = (rs_peak >= RS_PEAK_MIN) & (rs_b <= rs_peak * (1 - RS_PEAK_DD)) \
                         & (acc <= 0) & near_hi

    # 辅助观察 flag(只提示不进状态): NH-NL 高位失速 / pct_above 长期>80% 走平
    diff_hi = s['diff'] >= 0.8 * s['diff'].rolling(NEWHIGH_WINDOW, min_periods=NEWHIGH_WINDOW).max()
    s['f_nhnl_stall'] = diff_hi & (s['diff'] > 0) & (s['diff_accel'] <= 0)
    s['f_pct80_flat'] = (_streak(pct > 0.80) >= PCT80_LONG) & (pct.diff(ACCEL_LAG).abs() < 0.02)

    # 优先级合成
    state = pd.Series('NEUTRAL', index=s.index)
    state[s['f_ignition']] = 'IGNITION'
    state[s['f_established']] = 'ESTABLISHED'
    state[s['f_mature']] = 'MATURE'
    state[s['f_ad_divergence'] | s['f_rs_rollover']] = 'EXHAUSTION_WARN'
    # 预热期(指标未就绪)不给状态
    state[s['diff'].isna() | pct.isna()] = ''
    s['state'] = state
    return s


# ── 报告 ───────────────────────────────────────────────────────────────────────
def _fmt_row(d, r) -> str:
    def f(v, fmt='{:6.1%}'):
        return '   n/a' if pd.isna(v) else fmt.format(v)
    return (f'{d.date()}  diff {r["diff"]:+4.0f} (acc {f(r["diff_accel"], "{:+4.0f}")})  '
            f'50MA {f(r["pct_above_50ma"])}  AD {r["ad_line"]:+6.0f}  '
            f'rsB {f(r["rs_breadth"])} (acc {f(r["rs_breadth_accel"], "{:+6.1%}")})  '
            f'{STATE_CN.get(r["state"], "预热")}')


def report(s: pd.DataFrame, label: str, bench_sym: str, sector_n: int,
           asof: str, tail_days: int = 15) -> str:
    lines = []
    p = lines.append
    today = s.index[-1]
    r = s.iloc[-1]
    p('=' * 100)
    p(f'板块广度扩散报告 — {label} ({sector_n} 票, 基准 {bench_sym})   asof {today.date()}')
    p('=' * 100)
    p('')
    st = r['state'] or '预热(历史不足)'
    p(f'当前状态: {st} {STATE_CN.get(r["state"], "")}')
    p('')
    p(f'  1. NH-NL        : NH {r["nh"]:.0f} / NL {r["nl"]:.0f}  diff {r["diff"]:+.0f} '
      f'({r["diff_pct"]:+.1%} of {r["eligible"]:.0f} 合格票)  5日加速 {r["diff_accel"]:+.0f}'
      if pd.notna(r['diff']) else '  1. NH-NL        : 预热中(<252日历史)')
    p(f'  2. %>50MA       : {r["pct_above_50ma"]:.1%}'
      + ('   [Zweig thrust 近10日触发!]' if s['f_zweig_thrust'].tail(ZWEIG_VALID_DAYS).any() else ''))
    p(f'  3. A/D Line     : {r["ad_line"]:+.0f}  (今日 涨{r["advances"]:.0f}/跌{r["declines"]:.0f})'
      + ('   [背离: 指数新高 AD 未跟]' if r['f_ad_divergence'] else ''))
    p(f'  4. rs_breadth   : {r["rs_breadth"]:.1%} (RS>{RS_TOP:.0f} 占比)  5日加速 '
      + ('n/a' if pd.isna(r['rs_breadth_accel']) else f'{r["rs_breadth_accel"]:+.1%}')
      + ('   [自峰值回落≥15%]' if r['f_rs_rollover'] else ''))
    warn = []
    if r['f_nhnl_stall']:
        warn.append('NH-NL 高位失速(diff 高但加速≤0)')
    if r['f_pct80_flat']:
        warn.append(f'%>50MA 连续{PCT80_LONG}日>80%且走平(边际参与减少)')
    if r['disturbed']:
        warn.append('今日合格家数跳变>5%(疑似成分变更), 二阶导已作废')
    if warn:
        p('')
        p('  辅助观察: ' + '; '.join(warn))
    p('')
    p(f'近 {tail_days} 日:')
    for d, row in s.tail(tail_days).iterrows():
        p('  ' + _fmt_row(d, row))
    p('')
    # 近一年状态区段, 快速看风口叙事
    yr = s['state'].tail(252)
    seg, prev = [], None
    for d, st_ in yr.items():
        if st_ != prev:
            seg.append([d, d, st_])
            prev = st_
        else:
            seg[-1][1] = d
    seg = [x for x in seg if x[2] not in ('', 'NEUTRAL')][-12:]
    if seg:
        p('近一年非中性区段:')
        for a, b, st_ in seg:
            p(f'  {a.date()} → {b.date()}  {st_:<15} {STATE_CN[st_]}')
    p('=' * 100)
    return '\n'.join(lines)


# ── 回测(spec 要求3): thrust/确立 事件后 N 日基准表现 ─────────────────────────
def backtest(s: pd.DataFrame, start: str, end: str, horizons: list, label: str) -> str:
    bench = s['bench_close']
    win = s.loc[(s.index >= pd.Timestamp(start)) & (s.index <= pd.Timestamp(end))]

    entered = (win['state'] == 'ESTABLISHED') & \
              (~win['state'].shift(1).isin(['ESTABLISHED', 'MATURE']))
    events = win.index[entered | win['f_zweig_thrust'].fillna(False)]
    # 去重: EVENT_DEDUP 交易日内只记第一个
    dedup, last_i = [], -10**9
    pos = {d: i for i, d in enumerate(s.index)}
    for d in events:
        if pos[d] - last_i >= EVENT_DEDUP:
            dedup.append(d)
            last_i = pos[d]

    lines = []
    p = lines.append
    p('=' * 100)
    p(f'回测 — {label}  {start} → {end}   事件 = 首次进入 ESTABLISHED 或 Zweig thrust '
      f'(去重 {EVENT_DEDUP} 日) — 共 {len(dedup)} 次')
    p('=' * 100)
    if not dedup:
        p('区间内无事件。')
        return '\n'.join(lines)

    rows = []
    for d in dedup:
        i = pos[d]
        row = {'date': d.date(), 'state': s.loc[d, 'state'],
               'thrust': bool(s.loc[d, 'f_zweig_thrust'])}
        for h in horizons:
            row[f'fwd{h}d'] = (bench.iloc[i + h] / bench.iloc[i] - 1.0) if i + h < len(bench) else np.nan
        rows.append(row)
    ev = pd.DataFrame(rows)

    hdr = f'{"date":<12}{"state":<17}{"thrust":<8}' + ''.join(f'{"fwd%d"%h+"d":>9}' for h in horizons)
    p(hdr)
    for _, r in ev.iterrows():
        p(f'{str(r["date"]):<12}{r["state"]:<17}{("Y" if r["thrust"] else "-"):<8}'
          + ''.join(('     n/a' if pd.isna(r[f'fwd{h}d']) else f'{r[f"fwd{h}d"]:>8.1%}') + ' '
                    for h in horizons))
    p('')
    p('汇总 (事件后基准收益 vs 同区间无条件基线):')
    base_ret = {h: (bench.shift(-h) / bench - 1.0).loc[win.index] for h in horizons}
    p(f'{"":12}' + ''.join(f'{"fwd%d"%h+"d":>12}' for h in horizons))
    p(f'{"事件均值":<10}' + ''.join(f'{ev[f"fwd{h}d"].mean():>11.1%} ' for h in horizons))
    p(f'{"事件中位":<10}' + ''.join(f'{ev[f"fwd{h}d"].median():>11.1%} ' for h in horizons))
    p(f'{"胜率":<11}' + ''.join(f'{(ev[f"fwd{h}d"] > 0).mean():>11.0%} ' for h in horizons))
    p(f'{"无条件均值":<9}' + ''.join(f'{base_ret[h].mean():>11.1%} ' for h in horizons))
    p('=' * 100)
    return '\n'.join(lines)


# ── main ───────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description='板块广度扩散检测 (docs/breadth_diffusion_framework.md)')
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--tickers', help='板块成分, 逗号分隔: NVDA,AMD,AVGO,...')
    g.add_argument('--watchlist', help='select.yml 的 key, 如 US_SWING_SEMIS')
    g.add_argument('--pool', choices=['sp500', 'ndx', 'both'], help='整个指数池当板块')
    ap.add_argument('--benchmark', default='QQQ', help='板块/基准指数 (AD对比+回测收益, 默认QQQ)')
    ap.add_argument('--rs-csv', help='skyte RS percentile 输出(tidy: ticker,date,rs_percentile); 不给则内部计算')
    ap.add_argument('--rs-ref', default='both', choices=['sp500', 'ndx', 'both'],
                    help='内部算 RS 时的截面参考池 (默认 both=SP500∪NDX)')
    ap.add_argument('--asof', help='YYYY-MM-DD 点位回放: 全部数据截断到该日')
    ap.add_argument('--backtest', action='store_true', help='事件回测模式')
    ap.add_argument('--start', help='回测起始 YYYY-MM-DD')
    ap.add_argument('--end', help='回测截止 YYYY-MM-DD')
    ap.add_argument('--horizons', default='5,10,20,60', help='回测前瞻天数, 默认 5,10,20,60')
    ap.add_argument('--tail', type=int, default=15, help='报告尾部展示天数')
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S')

    if args.asof:
        tsw._ASOF = pd.Timestamp(args.asof)
        log.info(f'--asof {args.asof}: 数据层截断, 不偷看未来')
    if args.backtest and not (args.start and args.end):
        ap.error('--backtest 需要 --start 和 --end')

    label, sector = load_sector(args)
    log.info(f'板块 [{label}]: {len(sector)} 票; 基准 {args.benchmark}')

    close = build_close_panel(sector)
    bench_df = tsw._fetch_daily(args.benchmark.upper())
    if bench_df.empty:
        raise SystemExit(f'基准 {args.benchmark} 无数据')
    bench = bench_df['close']

    if args.rs_csv:
        rs_pct = rs_percentile_from_csv(args.rs_csv, list(close.columns), close.index)
    else:
        rs_pct = rs_percentile_internal(close, args.rs_ref)

    s = apply_state_machine(compute_series(close, bench, rs_pct))

    date_tag = (tsw._ASOF or s.index[-1]).strftime('%Y-%m-%d')
    out_dir = os.path.join(RESULT_DIR, OUT_SUB)
    os.makedirs(out_dir, exist_ok=True)

    if args.backtest:
        horizons = [int(x) for x in args.horizons.split(',')]
        txt = backtest(s, args.start, args.end, horizons, label)
        print(txt)
        out = os.path.join(out_dir, f'us_breadth_backtest_{label}_{args.start}_{args.end}.txt')
    else:
        txt = report(s, label, args.benchmark.upper(), len(sector), date_tag, args.tail)
        print(txt)
        out = os.path.join(out_dir, f'us_breadth_diffusion_{label}_{date_tag}.txt')

    with open(out, 'w') as fh:
        fh.write(txt + '\n')
    csv_out = os.path.join(out_dir, f'us_breadth_series_{label}_{date_tag}.csv')
    s.round(4).to_csv(csv_out)
    log.info(f'报告 → {out}')
    log.info(f'四条序列+状态 → {csv_out}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
