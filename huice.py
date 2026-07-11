# coding: utf-8
"""
huice.py — 回测系统给出的指示 (backtest the system's directives)

系统的"指示"= live 扫描器在某天收盘后给出的可操作 Setup:
  · t_us_tech_swing  — BREAKOUT / PULLBACK (entry/stop/target, 按 market state 门控)
  · t_us_key_kline   — 进场型关键K线 (BREAKOUT / FIRST_KISS / POCKET_PIVOT / EARNINGS_GAP,
                       指示 = "若现价上车: 买当日收盘 / 止损 kb.stop")
  · t_us_gap_scan    — 未回补向上真缺口: GAP_A (缺口日·放量收强) / GAP_B (存活≥5日,
                       WEAK 市抑制, 同 live 弱市门控); 止损 = 缺口下沿 gap_lo
  · t_us_undervalue  — 超跌优质候选 (1年跌>30% + 3年ROE均值≥12%)。它明确不是带止损的
                       Setup (超跌票天生在20周线下, 套止损纪律=当天出场), 按其实证纪律
                       回测: 无管理买入持有 max-hold 日, 只看 α (分散+拿得住)。
                       ROE 按年报期末+90天滞后做点位可得性过滤; $1B 市值门槛在
                       SP500∪NDX 池上恒过, 跳过。
  · t_us_chanlun     — 缠论买点 (1B/2B/3B, 日线笔中枢简化版)。发出日 = 实际确认日
                       (笔端点被后续 bar 提交的第一天), entry = 确认日收盘 (melt-up
                       里比结构价高 12~21%, 用结构价即未来函数); 止损 = 失效位
                       (3B→ZG, 1B/2B→转折低点)。只回测买点。
  · t_us_tr_surge    — 近N日TR%持续放大 (波动状态读数, 无止损 Setup)。episode =
                       静默>7天后的首个命中日, 按窗口净涨跌拆 TR_UP/TR_DOWN/TR_CHOP
                       (±3%); managed=False 买入持有口径, 只看 fwd/α。
  · t_us_earnings_react — 财报强反应·守住 (PEAD 确认入场)。episode = ticker×财报日,
                       发出日 = 反应 bar (E/E+1 里 ≥7% 那根), entry = 反应日收盘,
                       止损 = close(财报日-1) (回吐尽 = 证伪)。REACT_E=盘前公布 /
                       REACT_E1=盘后公布。日历非严格点位 (退市票缺日历→幸存者偏差;
                       limit=12 覆盖 ~2020+), 价格条件本身完全因果。

本脚本做两件事:
  1. 点位重放 (point-in-time): 借 t_us_tech_swing 的 _ASOF 数据层, 只用 ≤当日 的 bar
     重现"那天系统会说什么" — 与 --asof 跑原脚本 byte 级同一逻辑。
  2. 前向验证: 用当日之后的 bar, 按系统自己的退出纪律模拟这笔指示的结局
     (ADR-0002 止损按日收盘判; L0 熔断 -30%/5日; +30% 保本; 20周线周收盘退出),
     并给固定视窗 fwd21/63d 与 vs QQQ 的 alpha — 回答"哪个筛子真有 edge"。

与 t_us_signal_attrib 的关系: attrib 只读 live ledger (真实 track record, 但只有
上线后的几个月); huice 用重放把同一口径推回历史, 样本大但是合成的 — 两者互为印证,
结论以 live ledger 为准 (合成样本没有"当时会不会真的下单"的摩擦)。

Usage:
  python huice.py -asof 2026-01-15 --ticker MU        # 单日: 当日指示 + 前向验证
  python huice.py --ticker MU --start 2025-01-01      # 区间: 逐日重放该票, episode 统计
  python huice.py --start 2025-01-01                  # 全 watchlist 重放 → 哪些信号有效
  python huice.py --start 2025-01-01 --source key_kline   # 只回测关键K线指示
  python huice.py --start 2025-01-01 --universe both      # SP500∪NDX 大池 (自动多进程)
  python huice.py --start 2025-01-01 --universe ndx --jobs 4
  python huice.py --start 2025-01-01 --source gap_scan --universe ndx   # 缺口指示
  python huice.py --start 2025-01-01 --source undervalue --universe both # 超跌优质
  python huice.py --start 2023-01-01 --source chanlun --universe ndx      # 缠论买点
  python huice.py --start 2025-01-01 --source earnings_react --universe all # 财报反应
  python huice.py --start 2025-01-01 --source all --universe both       # 七源全测

Output: stdout + result/us_huice/huice_<tag>_<date>.txt
"""

import os
import sys
import logging
import argparse
import datetime

import multiprocessing as mp

import numpy as np
import pandas as pd
import tabulate as tab_mod

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
# key_kline 在 _ASOF 截断出的 slice 上挂指标列会触发 chained-assignment 告警;
# 功能无碍 (每次都重算), 回测里静音以免刷屏
pd.set_option('mode.chained_assignment', None)

# 复用 live 扫描器本身 — 回测跑的必须是系统真跑的代码, 不是重写的近似
import t_us_tech_swing as tsw
import t_us_key_kline as kk
import t_us_gap_scan as gs
import t_us_chanlun as ch
import t_us_tr_surge as trs
import t_us_earnings_react as erx

OUT_DIR      = '/home/ryan/DATA/result/us_huice'
BENCH        = 'QQQ'
MAX_HOLD_D   = 126          # 模拟持仓上限 (≈6个月); 到期未触发退出按 TIME 平仓
EPISODE_GAP_D = 7           # 同 signal_ledger: 静默 >7 天再触发 = 新 episode
WARMUP_D     = 420          # 重放起点前需要的历史深度 (2y 周线 MA20w + MA150d 用)
HORIZONS     = [21, 63]     # 固定视窗 (信号本身的质量, 与持仓管理无关)

EXIT_CN = {
    'L0_CRASH':  'L0熔断(-30%/5日)',
    'STOP':      'L1.5止损(日收盘破位)',
    'BE_STOP':   '保本止损(+30%后回吐)',
    'L2_20WMA':  'L2 20周线(周收盘)',
    'TIME':      '到期平仓(126日)',
    'OPEN':      '仍持仓(数据未走完)',
}


# ── data depth ────────────────────────────────────────────────────────────────
def _ensure_deep_history(tickers: list, need_start: pd.Timestamp):
    """确保 tsw 数据层里每只票的 bar 覆盖到 need_start; 不够就用 yfinance 拉 max
    重写缓存 (同源同 adjust, 只是更长, live 扫描不受影响)。拉不到就用现有的并告警。"""
    import yfinance as yf
    for t in tickers:
        df = tsw._fetch_daily_full(t)
        if not df.empty and df.index[0] <= need_start:
            continue
        logging.info(f'{t}: cache starts {df.index[0].date() if not df.empty else "∅"}'
                     f' > need {need_start.date()} — refetching period=max')
        try:
            raw = yf.Ticker(t).history(period='max', auto_adjust=True)
            if raw.empty:
                raise ValueError('empty frame')
            full = raw.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low',
                                       'Close': 'close', 'Volume': 'volume'})[
                ['open', 'high', 'low', 'close', 'volume']].copy()
            full.index = pd.to_datetime(full.index).tz_localize(None)
            full.index.name = 'date'
            full = full.dropna(subset=['close'])
            os.makedirs(tsw.BAR_CACHE_DIR, exist_ok=True)
            full.reset_index().to_csv(tsw._cache_path(t), index=False)
            tsw._DAILY_MEMO[t] = full
        except Exception as e:
            logging.warning(f'{t}: deep refetch failed ({e}) — 用现有缓存, '
                            f'早期 episode 可能因 warmup 不足而缺失')


def _bulk_deepen(tickers: list, need_start: pd.Timestamp):
    """大池版深历史: 缓存不够深的票分批 yf.download period=max 重写缓存。
    (2022 等远期回测用; 逐票 history() 590 只要 ~20 分钟, bulk 只要几分钟。)"""
    import yfinance as yf
    short = []
    for t in tickers:
        df = tsw._read_cache(t)
        if df.empty or df.index[0] > need_start:
            short.append(t)
    if not short:
        return
    logging.info(f'{len(short)} 票缓存不够深 (< {need_start.date()}) — 分批深拉 period=max')
    CH = 100
    for i in range(0, len(short), CH):
        chunk = short[i:i + CH]
        try:
            data = yf.download(chunk, period='max', auto_adjust=True,
                               progress=False, group_by='ticker', threads=True)
        except Exception as e:
            logging.warning(f'  chunk {i // CH}: bulk download failed ({e}) — 这批用现有缓存')
            continue
        ok = 0
        for t in chunk:
            try:
                sub = data[t] if len(chunk) > 1 else data
                df = sub.rename(columns=str.lower)[
                    ['open', 'high', 'low', 'close', 'volume']].dropna(subset=['close'])
                if df.empty:
                    continue
                idx = pd.to_datetime(df.index)
                if getattr(idx, 'tz', None) is not None:
                    idx = idx.tz_localize(None)
                df.index = idx
                df.index.name = 'date'
                os.makedirs(tsw.BAR_CACHE_DIR, exist_ok=True)
                df.reset_index().to_csv(tsw._cache_path(t), index=False)
                tsw._DAILY_MEMO[t] = df
                ok += 1
            except Exception:
                continue
        logging.info(f'  深拉 {i + len(chunk)}/{len(short)} (本批成功 {ok})')


def _trading_days(start: pd.Timestamp, end: pd.Timestamp) -> list:
    bench = tsw._fetch_daily_full(BENCH)
    return [d for d in bench.index if start <= d <= end]


# ── market gate (per date, shared across tickers) ────────────────────────────
_STATE_MEMO: dict = {}   # d -> (raw_state, gated_state)


def _states_on(d: pd.Timestamp) -> tuple:
    """当日 (raw, gated) market state。raw = QQQ/SOXX 20周线三态 (gap_scan 的弱市
    门控用它); gated = 再叠加领导股 breadth 降级 (tech_swing 的扫描模式用它)。
    必须先锚 _ASOF 再查 memo: 调用方靠这里设定当日截断, memo 命中也不能跳过,
    否则后续 scan_stock 会用错日期的数据 (未来函数)。"""
    tsw._ASOF = d
    if d in _STATE_MEMO:
        return _STATE_MEMO[d]
    state, _ = tsw.get_market_state()
    lead_frac, _ = tsw.get_leadership_breadth()
    gated = tsw._gate_state(state, lead_frac)
    _STATE_MEMO[d] = (state, gated)
    return _STATE_MEMO[d]


def gate_state_on(d: pd.Timestamp) -> str:
    return _states_on(d)[1]


def raw_state_on(d: pd.Timestamp) -> str:
    return _states_on(d)[0]


# ── episode generators ────────────────────────────────────────────────────────
def tech_swing_episodes(ticker: str, days: list) -> list:
    """逐日点位重放 tech_swing.scan_stock; 连续出同一信号只算一个 episode
    (entry/stop 冻结在首日 — 和 signal_ledger 的口径一致)。"""
    eps, last_seen = [], {}          # signal_type -> last emission date
    for d in days:
        state = gate_state_on(d)     # sets tsw._ASOF = d
        if state == 'ERROR':
            continue
        r = tsw.scan_stock(ticker, state)
        s = r.get('signal')
        if not s:
            continue
        stype = s['type']
        prev = last_seen.get(stype)
        last_seen[stype] = d
        if prev is not None and (d - prev).days <= EPISODE_GAP_D:
            continue                 # 同一 episode 的延续, 不重复开仓
        eps.append({
            'source': 'tech_swing', 'ticker': ticker, 'date': d,
            'signal_type': stype, 'market_state': state,
            'confidence': s.get('confidence'),
            'entry': float(s['entry']), 'stop': float(s['stop']),
            'target': s.get('target'), 'rr': s.get('rr'), 'rr_ok': s.get('rr_ok'),
        })
    return eps


def key_kline_episodes(ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> list:
    """进场型关键K线 episode。检测器是因果的 (每根只看 ≤当根 的 bar), 所以整段跑一次,
    每根进场型关键K线的日期就是它的"发出日"; 指示 = 买当日收盘 / 止损 kb.stop。"""
    kk._ASOF = tsw._ASOF = end
    df = kk.prepare_frame(ticker, '2y')      # indicators attach 在全量史上, 无 warmup 损失
    if df.empty:
        return []
    bars = kk.collect_key_bars(df, ticker, fetch_earnings=False)
    eps = []
    for kb in bars:
        if kb['type'] not in kk.ENTRY_TYPES or kb.get('stop') is None:
            continue
        d = pd.Timestamp(kb['date'])
        if not (start <= d <= end):
            continue
        entry = float(df.loc[d, 'close']) if d in df.index else None
        if entry is None or entry <= kb['stop']:
            continue                          # 收盘已破位 → 指示当日即失效, 勿追
        eps.append({
            'source': 'key_kline', 'ticker': ticker, 'date': d,
            'signal_type': kb['type'], 'market_state': gate_state_on(d),
            'confidence': None,
            'entry': round(entry, 2), 'stop': float(kb['stop']),
            'target': None, 'rr': None, 'rr_ok': None,
        })
    return eps


def gap_episodes(ticker: str, days: list) -> list:
    """t_us_gap_scan 指示重放。检测因果 (缺口/回补只看已发生的 bar), 整段单遍:
    GAP_A = 缺口日当天即入选 (放量≥1.2× & 收强≥0.4, run_scan 的 Tier A 首日);
    GAP_B = 存活满 SCAN_SURVIVE 日未回补的首日 (live 弱市门控: raw state=WEAK 抑制)。
    entry = 发出日收盘, stop = 缺口下沿 (gap_lo = 昨高), 同 live 的 stop 语义。"""
    tsw._ASOF = None
    full = tsw._fetch_daily_full(ticker)
    if full.empty or len(full) < gs.VOL_WIN + 2:
        return []
    d = gs.annotate_gaps(full)
    idx, lows, closes = d.index, d['low'].values, d['close'].values
    start, end = days[0], days[-1]
    eps = []
    for pos in np.where(d['up_gap'].values & (d['gap_pct'].values >= gs.MIN_GAP_PCT))[0]:
        pos = int(pos)
        floor = float(d['prev_high'].iloc[pos])
        rel = np.where(lows[pos + 1:] <= floor)[0]
        fill_pos = pos + 1 + int(rel[0]) if len(rel) else None

        gd = idx[pos]
        vol, cpos = d['vol_mult'].iloc[pos], float(d['close_pos'].iloc[pos])
        if (start <= gd <= end and pd.notna(vol) and float(vol) >= gs.SCAN_MIN_VOL
                and cpos >= gs.SCAN_MIN_CPOS and float(closes[pos]) > floor):
            eps.append({
                'source': 'gap_scan', 'ticker': ticker, 'date': gd,
                'signal_type': 'GAP_A', 'market_state': raw_state_on(gd),
                'confidence': None, 'entry': round(float(closes[pos]), 2),
                'stop': round(floor, 2), 'target': None, 'rr': None, 'rr_ok': None,
            })

        q = pos + gs.SCAN_SURVIVE
        if q < len(d) and (fill_pos is None or fill_pos > q):
            qd = idx[q]
            if start <= qd <= end and float(closes[q]) > floor:
                mstate = raw_state_on(qd)
                if mstate != 'WEAK':          # live 默认 weak_gate: WEAK 抑制 Tier B
                    eps.append({
                        'source': 'gap_scan', 'ticker': ticker, 'date': qd,
                        'signal_type': 'GAP_B', 'market_state': mstate,
                        'confidence': None, 'entry': round(float(closes[q]), 2),
                        'stop': round(floor, 2), 'target': None, 'rr': None,
                        'rr_ok': None,
                    })
    return eps


def tr_surge_episodes(ticker: str, days: list) -> list:
    """t_us_tr_surge 指示重放 (近N日TR%持续放大, 波动状态读数)。检测因果 (TR 只用
    ≤当日 bar), 整段单遍: 滚动窗口命中 → 静默 >EPISODE_GAP_D 天后的首个命中日 =
    新 episode。信号本身不定义止损 (状态读数, 非带止损的 Setup), 按 undervalue
    口径 managed=False 买入持有 max-hold 日, 只看 fwd/α。signal_type 按窗口净涨跌
    拆 TR_UP / TR_DOWN / TR_CHOP (±3%), 对应 live 报告解读的三档。
    高位过滤同 live 默认: 收盘距 HI_DAYS 日最高收盘 ≤ NEAR_HIGH_PCT。"""
    tsw._ASOF = None
    full = tsw._fetch_daily_full(ticker)
    if full.empty or len(full) < trs.MIN_BARS + trs.DAYS:
        return []
    d = trs.annotate(full)
    n = trs.DAYS
    tot = d['tr_pct'].rolling(n).sum()
    lo  = d['tr_pct'].rolling(n).min()
    net = (d['close'] / d['close'].shift(n) - 1.0) * 100.0
    hit = tot.notna() & (tot > trs.TOTAL_MIN) & (lo >= trs.DAILY_MIN) \
          & (d['dfh'] >= -trs.NEAR_HIGH_PCT)
    start, end = days[0], days[-1]
    eps, prev = [], None
    for dt in d.index[hit]:
        if dt < start:
            prev = dt                # 窗口前就已在放大态 → 起点附近不算新触发
            continue
        if dt > end:
            break
        if prev is not None and (dt - prev).days <= EPISODE_GAP_D:
            prev = dt                # 同一波动事件的延续
            continue
        prev = dt
        nc = float(net.loc[dt])
        stype = 'TR_UP' if nc >= 3.0 else ('TR_DOWN' if nc <= -3.0 else 'TR_CHOP')
        eps.append({
            'source': 'tr_surge', 'ticker': ticker, 'date': dt,
            'signal_type': stype, 'market_state': gate_state_on(dt),
            'confidence': None, 'entry': round(float(d['close'].loc[dt]), 2),
            'stop': None, 'target': None, 'rr': None, 'rr_ok': None,
            'managed': False,
        })
    return eps


def earnings_react_episodes(ticker: str, days: list) -> list:
    """t_us_earnings_react 指示重放 (财报强反应·守住, PEAD 确认入场)。

    episode = ticker × 财报日 (天然去重, 不需要 EPISODE_GAP_D)。发出日 = 反应 bar
    (E 或 E+1 里收盘涨幅更大的那根; live 三条件在反应日收盘即全真 — 新鲜度 0/1 日,
    收盘必 > ref)。entry = 反应日收盘, stop = close(财报日-1) (live 的 ref_stop 同
    语义: 日收盘跌破 = 强反应被完全回吐), managed=True 走分层退出。signal_type 拆
    REACT_E(盘前公布, 涨在财报日) / REACT_E1(盘后公布, 涨在次日)。

    点位口径: 三个价格条件纯 bar 数据, 完全因果; 过去的财报公告日是历史事实,
    今天抓的日历里的过去日期无前视。真正的 caveat: ① 退市票缺日历 → 幸存者偏差
    (与 bar 缓存同源同 caveat); ② 默认 limit=12 日历覆盖 ~2020+, 更早区间的
    episode 会静默缺失。日历走共享磁盘缓存 (tsw.fetch_earnings_calendar)。"""
    tsw._ASOF = None
    full = tsw._fetch_daily_full(ticker)
    if full.empty or len(full) < erx.MIN_BARS:
        return []
    idx = full.index
    close = full['close'].values
    start, end = days[0], days[-1]
    # 价格漏斗 (同 live scan): 区间±1bar 内没有 ≥CHG_MIN% 的单日大阳 → 必无 episode,
    # 省一次日历请求
    lo = max(0, idx.searchsorted(start) - 1)
    hi = min(len(idx), idx.searchsorted(end, side='right') + 1)
    r1d = pd.Series(close[lo:hi]).pct_change()
    if not (r1d >= erx.CHG_MIN / 100.0).any():
        return []
    cal = tsw.fetch_earnings_calendar(ticker)
    if not cal:
        return []
    eps = []
    for r in cal:
        g = int(idx.searchsorted(r['date'], side='left'))
        if g <= 0 or g >= len(idx):
            continue
        chg_e = close[g] / close[g - 1] - 1.0
        chg_e1 = close[g + 1] / close[g] - 1.0 if g + 1 < len(idx) else np.nan
        if not np.isnan(chg_e1) and chg_e1 > chg_e:
            react, ri, stype = chg_e1, g + 1, 'REACT_E1'
        else:
            react, ri, stype = chg_e, g, 'REACT_E'
        if react < erx.CHG_MIN / 100.0:
            continue
        dt = idx[ri]
        if not (start <= dt <= end):
            continue
        ref = float(close[g - 1])
        entry = float(close[ri])
        if entry <= ref:
            continue                          # react≥7% 下理论不可能, 纯防御
        eps.append({
            'source': 'earnings_react', 'ticker': ticker, 'date': dt,
            'signal_type': stype, 'market_state': gate_state_on(dt),
            'confidence': None, 'entry': round(entry, 2), 'stop': round(ref, 2),
            'target': None, 'rr': None, 'rr_ok': None,
        })
    return eps


def chanlun_episodes(ticker: str, days: list) -> list:
    """缠论买点指示重放 (t_us_chanlun 日线笔中枢简化版)。

    结构在全历史上划分 (左缘固定; MU/NVDA 66 周快照零重绘 → append-only), 所以
    一遍全量计算得到的信号 = 每个时点 live 会看到的信号; 发出日 = 实际确认日
    (confirm_signals 截断重放: 笔端点分型被后续 bar 提交的第一天)。melt-up 里
    确认价比结构标注价高 12~21% — entry 必须用确认日收盘, 否则高估收益。
    指示 = 确认日收盘买入; stop = 信号失效位 (3B→中枢上沿ZG, 1B/2B→转折低点,
    与图上"失效·关注位"同语义)。只回测买点; 卖点是持仓管理层, 不构成开仓指示。
    """
    tsw._ASOF = None
    ch._ASOF = None
    full = tsw._fetch_daily_full(ticker)
    if full.empty or len(full) < 60:
        return []
    hist = ch.macd_hist(full['close']) / full['close']
    *_, signals = ch._compute_signals(full, hist)
    start, end = days[0], days[-1]
    # 确认最多滞后 CONF_MAX_BARS 根 bar; 结构日在窗口前 ~60 天内的也可能确认在窗口内
    cand = [s for s in signals if s['kind'].endswith('B')
            and (start - pd.Timedelta(days=60)) <= full.index[s['pt']['i']] <= end]
    ch.confirm_signals(full, hist, cand)
    eps = []
    for s in cand:
        ci = s.get('conf_i')
        if ci is None:
            continue
        d = full.index[ci]
        if not (start <= d <= end):
            continue
        entry = float(full['close'].iloc[ci])
        stop = float(s['ref'])
        if entry <= stop:
            continue                          # 确认日收盘已破失效位 → 指示当日即作废
        eps.append({
            'source': 'chanlun', 'ticker': ticker, 'date': d,
            'signal_type': s['kind'], 'market_state': gate_state_on(d),
            'confidence': None, 'entry': round(entry, 2), 'stop': round(stop, 2),
            'target': None, 'rr': None, 'rr_ok': None,
        })
    return eps


# ── undervalue: ROE 点位缓存 + 周频转入 episode ───────────────────────────────
ROE_CACHE_PATH = '/home/ryan/DATA/pickle/huice_roe_cache.json'
UNDERVAL_DROP  = 30.0   # 同 t_us_undervalue MIN_DROP_PCT
UNDERVAL_ROE   = 12.0   # 同 MIN_ROE_AVG
ROE_LAG_D      = 90     # 年报期末 + 90天 才算"当时可见" (filing 滞后, 防未来函数)
# yfinance 只给最近 ~4 份年报 → 2023 之前的 asof 没有任何"当时可见"的 ROE, 质量门
# 恒 False。远期回测 (如 2022) 用 --underval-skip-roe 退化为纯价格版 (跌幅门槛 only),
# signal_type 记为 UNDERVAL_P 以免和质量门控版混进同一聚合; 注意纯价格版少了
# "防归零"的左尾保护, 结果会低估质量门的价值。
_UNDERVAL_SKIP_ROE = False


def build_roe_cache(cands: list) -> dict:
    """yfinance 年报 ROE (净利润/股东权益) per ticker, 磁盘缓存 (年报一年一变,
    回测重复跑不重复拉)。值 = [[period_end_iso, roe_pct], ...] 新→旧; 拉不到 = []。"""
    import json
    cache = {}
    if os.path.exists(ROE_CACHE_PATH):
        with open(ROE_CACHE_PATH) as fh:
            cache = json.load(fh)
    missing = [t for t in cands if t not in cache]
    if missing:
        import yfinance as yf
        logging.info(f'ROE 缓存缺 {len(missing)} 票, 逐票拉年报 (慢, 只此一次) …')
        for i, t in enumerate(missing, 1):
            pairs = []
            try:
                tk = yf.Ticker(t)
                fin, bs = tk.financials, tk.balance_sheet
                if fin is not None and bs is not None and not fin.empty and not bs.empty:
                    def _row(df, *names):
                        for n in names:
                            m = [x for x in df.index if n.lower() in str(x).lower()]
                            if m:
                                return df.loc[m[0]]
                        return None
                    ni = _row(fin, 'Net Income')
                    eq = _row(bs, 'Stockholders Equity', 'Common Stock Equity',
                              'Total Equity Gross')
                    if ni is not None and eq is not None:
                        for c in fin.columns:            # 新→旧
                            n, e = ni.get(c), (eq.get(c) if c in eq.index else None)
                            if n is not None and e and e != 0 and pd.notna(n) and pd.notna(e):
                                pairs.append([pd.Timestamp(c).date().isoformat(),
                                              round(float(n) / float(e) * 100, 1)])
            except Exception as e:
                logging.debug(f'{t}: ROE fetch failed ({e})')
            cache[t] = pairs
            if i % 25 == 0:
                logging.info(f'  ROE {i}/{len(missing)}')
        os.makedirs(os.path.dirname(ROE_CACHE_PATH), exist_ok=True)
        with open(ROE_CACHE_PATH, 'w') as fh:
            json.dump(cache, fh)
    return cache


def _week_fridays(days: list) -> list:
    """区间内每周最后一个交易日 (live undervalue 周日跑, 看到的是周五收盘)。"""
    out, cur_wk = [], None
    for d in days:
        wk = d.to_period('W')
        if cur_wk is not None and wk != cur_wk:
            out.append(prev)
        cur_wk, prev = wk, d
    out.append(days[-1])
    return out


def undervalue_episodes(tickers: list, days: list) -> list:
    """超跌优质清单的周频重放: 某周五 (1年跌幅≤-30%) 且 (点位可见的≥2份年报
    ROE 均值≥12%) 且 上周不满足 → 转入清单 = 一个 episode。无止损 (managed=False),
    与其实证用法一致 (分散买入持有)。"""
    fridays = _week_fridays(days)
    # 先用纯价格粗筛候选, 只对候选拉 ROE (成本递增顺序, 同 live 脚本的哲学)
    cands, frames = [], {}
    for t in tickers:
        df = tsw._fetch_daily_full(t)
        if df.empty or len(df) < 260:
            continue
        closes = df['close']
        r252 = closes / closes.shift(252) - 1
        window = r252[(r252.index >= fridays[0]) & (r252.index <= fridays[-1])]
        if not window.empty and float(window.min()) * 100 <= -UNDERVAL_DROP:
            cands.append(t)
            frames[t] = closes
    logging.info(f'undervalue: 价格粗筛 {len(cands)}/{len(tickers)} 票曾达跌幅门槛'
                 + (' (--underval-skip-roe: 纯价格版, 无质量门)' if _UNDERVAL_SKIP_ROE else ''))
    roe_cache = {} if _UNDERVAL_SKIP_ROE else build_roe_cache(cands)
    stype = 'UNDERVAL_P' if _UNDERVAL_SKIP_ROE else 'UNDERVAL'

    eps = []
    for t in cands:
        closes = frames[t]
        pairs = roe_cache.get(t) or []
        qual_prev = False
        for f in fridays:
            hist = closes[(closes.index > f - pd.Timedelta(days=365)) & (closes.index <= f)]
            if len(hist) < 200:
                qual_prev = False
                continue
            drop_ok = (float(hist.iloc[-1]) / float(hist.iloc[0]) - 1) * 100 <= -UNDERVAL_DROP
            qual = False
            if drop_ok:
                if _UNDERVAL_SKIP_ROE:
                    qual = True
                else:
                    usable = [r for pe, r in pairs
                              if pd.Timestamp(pe) + pd.Timedelta(days=ROE_LAG_D) <= f][:3]
                    qual = len(usable) >= 2 and sum(usable) / len(usable) >= UNDERVAL_ROE
            if qual and not qual_prev:
                eps.append({
                    'source': 'undervalue', 'ticker': t, 'date': f,
                    'signal_type': stype, 'market_state': gate_state_on(f),
                    'confidence': None, 'entry': round(float(hist.iloc[-1]), 2),
                    'stop': None, 'target': None, 'rr': None, 'rr_ok': None,
                    'managed': False,
                })
            qual_prev = qual
    return eps


# ── forward simulation (系统自己的退出纪律) ───────────────────────────────────
_SIM_MEMO: dict = {}    # ticker -> (wk_p, ma20w_p, ma10w_p, week_last, roll5max)
                        # 大池下同一票有几十个 episode, 周线族只算一次


def _sim_frames(ticker: str, daily: pd.DataFrame):
    if ticker in _SIM_MEMO:
        return _SIM_MEMO[ticker]
    closes = daily['close']
    wk = closes.resample('W').last().dropna()
    ma20w = wk.rolling(tsw.MA_WEEKLY).mean()
    ma10w = wk.rolling(tsw.MA_WEEKLY_FAST).mean()
    wk_p, ma20w_p, ma10w_p = (s.copy() for s in (wk, ma20w, ma10w))
    for s in (wk_p, ma20w_p, ma10w_p):
        s.index = s.index.to_period('W')
    week_of = daily.index.to_period('W')
    week_last = {p: g[-1] for p, g in daily.groupby(week_of).groups.items()}
    roll5max = closes.rolling(tsw.CRASH_WINDOW_D).max()
    _SIM_MEMO[ticker] = (wk_p, ma20w_p, ma10w_p, week_of, week_last, roll5max)
    return _SIM_MEMO[ticker]


def simulate(ticker: str, entry_date: pd.Timestamp, entry: float,
             stop0: float | None, max_hold: int | None = None,
             managed: bool = True) -> dict:
    """从信号日收盘入场, 按 20周趋势系统的分层纪律走到退出:
       每日收盘: L0 熔断 (5日峰值回撤≤-30%) → 止损 (close<stop, ADR-0002 收盘判,
       +30% 后 stop 抬到保本) → 周末: 周收盘<20wMA 退出 (L2)。
       另记 trim 触发 (≥+25% 且周收盘<10wMA) 但不模拟部分仓位。
       managed=False (undervalue 等无止损指示): 纯买入持有 max_hold 日, 只算收益/α。"""
    if max_hold is None:
        max_hold = MAX_HOLD_D
    tsw._ASOF = None
    daily = tsw._fetch_daily_full(ticker)
    out = {'exit_reason': None, 'exit_date': None, 'days_held': None,
           'ret_pct': None, 'r_mult': None, 'trim_hit': False,
           'alpha_pct': None}
    if daily.empty or entry_date not in daily.index:
        return out
    a = daily.index.get_loc(entry_date)
    closes = daily['close']

    # 周线 (20w/10w MA), 键 = 周 period; 每周最后一个交易日做周收盘检查
    wk_p, ma20w_p, ma10w_p, week_of, week_last, roll5max = _sim_frames(ticker, daily)
    risk = (entry - stop0) if stop0 is not None else None
    stop, be_armed = stop0, False
    exit_i, reason = None, None

    last_i = min(a + max_hold, len(daily) - 1)
    for i in range(a + 1, last_i + 1) if managed else []:
        c = float(closes.iloc[i])
        # L0 熔断: 近5日收盘峰值回撤
        if c / float(roll5max.iloc[i]) - 1 <= -tsw.HARD_STOP_PCT / 100:
            exit_i, reason = i, 'L0_CRASH'
            break
        # 止损按日收盘判 (ADR-0002)
        if c < stop:
            exit_i, reason = i, ('BE_STOP' if be_armed else 'STOP')
            break
        # +30% 保本
        if not be_armed and c >= entry * (1 + tsw.BREAKEVEN_PCT / 100):
            be_armed, stop = True, max(stop, entry)
        # 周收盘检查 (只在该周最后一个交易日)
        d = daily.index[i]
        p = week_of[i]
        if week_last.get(p) == d and p in wk_p.index:
            m20 = ma20w_p.get(p)
            if pd.notna(m20) and c < float(m20):
                exit_i, reason = i, 'L2_20WMA'
                break
            m10 = ma10w_p.get(p)
            if (not out['trim_hit'] and pd.notna(m10)
                    and c / entry - 1 >= tsw.TRIM_PCT / 100 and c < float(m10)):
                out['trim_hit'] = True

    if exit_i is None:
        exit_i = last_i
        reason = 'TIME' if last_i == a + max_hold else 'OPEN'

    exit_px = float(closes.iloc[exit_i])
    ret = exit_px / entry - 1
    out.update({
        'exit_reason': reason, 'exit_date': daily.index[exit_i],
        'days_held': exit_i - a, 'ret_pct': ret * 100,
        'r_mult': (exit_px - entry) / risk if (risk is not None and risk > 0) else None,
    })

    # 固定视窗 + alpha vs QQQ (衡量信号本身, 与退出纪律无关)
    for h in HORIZONS:
        if a + h < len(closes):
            out[f'fwd{h}'] = (float(closes.iloc[a + h]) / entry - 1) * 100
    bench = tsw._fetch_daily_full(BENCH)['close']
    d0, d1 = daily.index[a], daily.index[exit_i]
    b0, b1 = bench.asof(d0), bench.asof(d1)
    if pd.notna(b0) and pd.notna(b1) and b0 > 0:
        out['alpha_pct'] = ret * 100 - (b1 / b0 - 1) * 100
    if 'fwd63' in out:
        j = min(a + 63, len(daily) - 1)
        b1h = bench.asof(daily.index[j])
        if pd.notna(b0) and pd.notna(b1h) and b0 > 0:
            out['alpha63'] = out['fwd63'] - (b1h / b0 - 1) * 100
    return out


# ── reporting ─────────────────────────────────────────────────────────────────
def _fmt(v, nd=1):
    return f'{v:+.{nd}f}' if isinstance(v, (int, float)) and pd.notna(v) else '—'


def episode_table(eps: list) -> str:
    rows = []
    for e in eps:
        rows.append([
            e['date'].strftime('%Y-%m-%d'), e['ticker'], e['source'],
            e['signal_type'], e['market_state'], e.get('confidence') or '—',
            f"{e['entry']:.2f}",
            f"{e['stop']:.2f}" if e.get('stop') is not None else '—',
            EXIT_CN.get(e.get('exit_reason'), e.get('exit_reason') or '—'),
            e['exit_date'].strftime('%m-%d') if e.get('exit_date') is not None else '—',
            e.get('days_held') if e.get('days_held') is not None else '—',
            _fmt(e.get('ret_pct')), _fmt(e.get('r_mult'), 2),
            _fmt(e.get('fwd63')), _fmt(e.get('alpha63')),
        ])
    hdr = ['date', 'ticker', 'source', 'type', 'state', 'conf', 'entry', 'stop',
           'exit', 'exit_d', 'hold', 'ret%', 'R', 'fwd63%', 'α63%']
    return tab_mod.tabulate(rows, headers=hdr, tablefmt='simple')


def _agg(eps: list, by: list, min_n: int) -> str:
    df = pd.DataFrame(eps)
    rows = []
    for key, g in df.groupby(by, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        ret = g['ret_pct'].dropna()
        r_m = g['r_mult'].dropna()
        al  = g['alpha63'].dropna() if 'alpha63' in g else pd.Series(dtype=float)
        f63 = g['fwd63'].dropna() if 'fwd63' in g else pd.Series(dtype=float)
        stopped = g['exit_reason'].isin(['STOP', 'L0_CRASH']).mean() * 100
        n = len(g)
        flag = ' ⚠' if n < min_n else ''
        # 结论列: 样本够 + 中位α>0 + 胜率≥50% → ✅有效; α≤0 → ❌无效; 否则 △弱
        if n < min_n:
            verdict = '⚠样本不足'
        elif not len(al):
            verdict = '⚠窗口未满'
        elif al.median() > 0 and (ret > 0).mean() >= 0.5:
            verdict = '✅ 有效'
        elif al.median() <= 0:
            verdict = '❌ 无α'
        else:
            verdict = '△ 弱'
        rows.append(list(key) + [
            f'{n}{flag}',
            f'{(ret > 0).mean() * 100:.0f}%' if len(ret) else '—',
            _fmt(ret.median()) if len(ret) else '—',
            _fmt(r_m.median(), 2) if len(r_m) else '—',
            f'{g["days_held"].median():.0f}' if g['days_held'].notna().any() else '—',
            _fmt(f63.median()) if len(f63) else '—',
            _fmt(al.median()) if len(al) else '—',
            f'{stopped:.0f}%',
            verdict,
        ])
    rows.sort(key=lambda r: -float(str(r[len(by)]).rstrip(' ⚠') or 0))
    hdr = by + ['n', 'win%', 'medRet%', 'medR', 'medHold',
                'medFwd63%', 'medα63%', 'stop/crash%', '结论']
    return tab_mod.tabulate(rows, headers=hdr, tablefmt='simple')


def run_report(eps: list, tag: str, min_n: int, start, end, out_path=None):
    lines = []

    def p(*a):
        line = ' '.join(str(x) for x in a)
        lines.append(line)
        print(line)

    p()
    p('=' * 78)
    p(f'  回测报告 HUICE  —  {tag}   ({start.date()} → {end.date()})')
    p('=' * 78)
    if not eps:
        p('  区间内系统没有发出任何指示 (无 episode)。')
    else:
        p(f'  episodes: {len(eps)}   模拟口径: 信号日收盘入场, 系统分层纪律退出'
          f' (L0熔断/收盘止损/+30%保本/20周线), 上限 {MAX_HOLD_D} 日')
        p()
        detail = episode_table(eps)
        if len(eps) <= 200:
            p('[ EPISODES 明细 ]')
            p(detail)
        else:
            # 大池: 明细只进文件, stdout 只留聚合 (几千行刷屏没有信息量)
            lines.append('[ EPISODES 明细 ]')
            lines.append(detail)
            print(f'[ EPISODES 明细 ]  {len(eps)} 条 — 只写入报告文件')
        p()
        p(f'[ 按 source × 信号类型 ]   (⚠ = n < {min_n}, 别读EV)')
        p(_agg(eps, ['source', 'signal_type'], min_n))
        p()
        p('[ 按 信号类型 × market state ]   (同一信号在不同体制下是不是两回事)')
        p(_agg(eps, ['signal_type', 'market_state'], min_n))
        p()
        p('[ 读法 ]')
        p('  ret%/R  = 按系统退出纪律模拟的整笔结果 (R = 相对冻结初始止损的倍数)')
        p('  fwd63%  = 不管退出、拿满63交易日的信号裸收益;  α63 = 减同期 QQQ')
        p('  结论: ✅=中位α>0且胜率≥50%; ❌=跑不赢QQQ(包装不是edge); ⚠=样本不足')
        p('  注意: 合成重放没有"当时会不会真下单"的摩擦, 结论以 live ledger'
          ' (t_us_signal_attrib) 为准, 两者互相印证')
    p()

    if out_path is None:
        os.makedirs(OUT_DIR, exist_ok=True)
        out_path = os.path.join(
            OUT_DIR, f'huice_{tag}_{datetime.date.today().strftime("%Y%m%d")}.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    logging.info(f'回测报告 → {out_path}')


# ── single-asof mode ──────────────────────────────────────────────────────────
def run_single(ticker: str, asof: pd.Timestamp, source: str, min_n: int):
    """单日: 重现当日系统指示, 然后 (若有指示且未来数据存在) 前向验证。"""
    days = _trading_days(asof - pd.Timedelta(days=10), asof)
    if not days:
        logging.error(f'{asof.date()} 附近无交易日数据')
        return
    d = days[-1]                      # asof 落在非交易日 → 用其前最近的交易日
    if d != asof:
        logging.info(f'{asof.date()} 非交易日, 锚定到 {d.date()}')

    state = gate_state_on(d)
    print(f'\n{"=" * 78}\n  {ticker} @ {d.date()}   market state (gated): {state}\n{"=" * 78}')

    eps = []
    if source in ('tech_swing', 'both', 'all'):
        tsw._ASOF = d
        r = tsw.scan_stock(ticker, state)
        s = r.get('signal')
        print(f'\n[tech_swing]  close={r.get("close")}  ma20w={r.get("ma20w")}'
              f'  exit_signal={r.get("exit_signal")}'
              f'  gap={r["gap"]["label"] if r.get("gap") else "—"}')
        if s:
            print(f'  指示: {s["type"]}  entry={s["entry"]}  stop={s["stop"]}'
                  f'  target={s.get("target")}  rr={s.get("rr")}  conf={s.get("confidence")}')
            eps.append({'source': 'tech_swing', 'ticker': ticker, 'date': d,
                        'signal_type': s['type'], 'market_state': state,
                        'confidence': s.get('confidence'),
                        'entry': float(s['entry']), 'stop': float(s['stop'])})
        else:
            print('  指示: 无 Setup (当日不满足入场条件)')

    if source in ('key_kline', 'both', 'all'):
        kk._ASOF = tsw._ASOF = d
        df = kk.prepare_frame(ticker, '1y')
        bars = kk.collect_key_bars(df, ticker, fetch_earnings=False) if not df.empty else []
        st = kk.compute_status(df, bars) if not df.empty else {}
        fresh = st.get('fresh')
        print(f'\n[key_kline]  posture={st.get("posture")}  '
              f'fresh={fresh["type"] + "@" + fresh["date"].strftime("%m-%d") if fresh else "—"}'
              f'  stop={st.get("stop")}  alive={st.get("alive")}'
              f'  fresh_enough={st.get("fresh_enough")}')
        if fresh and st.get('alive') and st.get('fresh_enough'):
            print(f'  指示: 若现价上车 买 {st["price"]:.2f} / 止损 {st["stop"]:.2f}'
                  f' (1R={st["risk_pct"] * 100:.1f}%)')
            eps.append({'source': 'key_kline', 'ticker': ticker, 'date': d,
                        'signal_type': fresh['type'], 'market_state': state,
                        'confidence': None,
                        'entry': float(st['price']), 'stop': float(st['stop'])})
        else:
            print('  指示: 无可上车的新鲜进场信号')

    if source in ('gap_scan', 'all'):
        tsw._ASOF = None
        full = tsw._fetch_daily_full(ticker)
        dd = gs.annotate_gaps(full)
        dd = dd[dd.index <= d]
        hits = []
        if len(dd) > gs.VOL_WIN + 2:
            last_close = float(dd['close'].iloc[-1])
            recent = dd.tail(max(gs.SCAN_LOOKBACK, gs.SCAN_MAX_AGE))
            for ts, r in recent[(recent['up_gap'])
                                & (recent['gap_pct'] >= gs.MIN_GAP_PCT)].iterrows():
                pos = dd.index.get_loc(ts)
                floor = float(r['prev_high'])
                if (dd['low'].iloc[pos + 1:] <= floor).any():
                    continue                                   # 已回补 → 失效
                age = len(dd) - 1 - pos
                tier_a = (age < gs.SCAN_LOOKBACK
                          and pd.notna(r['vol_mult']) and r['vol_mult'] >= gs.SCAN_MIN_VOL
                          and r['close_pos'] >= gs.SCAN_MIN_CPOS)
                tier_b = gs.SCAN_SURVIVE <= age <= gs.SCAN_MAX_AGE
                if not (tier_a or tier_b) or last_close <= floor:
                    continue
                tier = 'A' if tier_a else 'B'
                if tier == 'B' and raw_state_on(d) == 'WEAK':
                    continue                                   # live 弱市门控
                hits.append((tier, ts, floor, r))
        hits.sort(key=lambda h: h[0])          # A 在前 (更新鲜·止损更紧, 同 live 排序)
        print(f'\n[gap_scan]  未回补向上真缺口 {len(hits)} 个')
        for tier, ts, floor, r in hits:
            print(f'  指示: GAP_{tier}  缺口日 {ts.date()}  gap {r["gap_pct"]:+.1f}%'
                  f'  买 {float(dd["close"].iloc[-1]):.2f} / 止损(下沿) {floor:.2f}')
        if hits:
            tier, ts, floor, r = hits[0]
            eps.append({'source': 'gap_scan', 'ticker': ticker, 'date': d,
                        'signal_type': f'GAP_{tier}', 'market_state': raw_state_on(d),
                        'confidence': None,
                        'entry': float(dd['close'].iloc[-1]), 'stop': floor})

    if source in ('undervalue', 'all'):
        tsw._ASOF = None
        closes = tsw._fetch_daily_full(ticker)['close']
        hist = closes[(closes.index > d - pd.Timedelta(days=365)) & (closes.index <= d)]
        drop = (float(hist.iloc[-1]) / float(hist.iloc[0]) - 1) * 100 if len(hist) > 200 else None
        roe_cache = build_roe_cache([ticker])
        usable = [r for pe, r in (roe_cache.get(ticker) or [])
                  if pd.Timestamp(pe) + pd.Timedelta(days=ROE_LAG_D) <= d][:3]
        roe_avg = round(sum(usable) / len(usable), 1) if len(usable) >= 2 else None
        qual = (drop is not None and drop <= -UNDERVAL_DROP
                and roe_avg is not None and roe_avg >= UNDERVAL_ROE)
        print(f'\n[undervalue]  1年涨跌 {drop:+.1f}% · 3年ROE均值(点位可见) '
              f'{roe_avg if roe_avg is not None else "—"}%'
              f' → {"✓ 在超跌优质清单" if qual else "不在清单"}' if drop is not None
              else '\n[undervalue]  历史不足1年, 无法判定')
        if qual:
            print(f'  指示: 超跌优质候选 (无止损, 分散买入持有口径)')
            eps.append({'source': 'undervalue', 'ticker': ticker, 'date': d,
                        'signal_type': 'UNDERVAL', 'market_state': gate_state_on(d),
                        'confidence': None, 'entry': float(hist.iloc[-1]),
                        'stop': None, 'managed': False})

    if source in ('chanlun', 'all'):
        got = chanlun_episodes(ticker, [d])
        print(f'\n[chanlun]  当日确认的缠论买点 {len(got)} 个')
        for e in got:
            print(f'  指示: {e["signal_type"]}  买 {e["entry"]:.2f}'
                  f' / 止损(失效位) {e["stop"]:.2f}')
        eps.extend(got)

    if source in ('tr_surge', 'all'):
        got = tr_surge_episodes(ticker, [d])
        print(f'\n[tr_surge]  当日新触发的TR连续放大 {len(got)} 个')
        for e in got:
            print(f'  状态: {e["signal_type"]}  收 {e["entry"]:.2f}'
                  f' (无止损, 买入持有口径)')
        eps.extend(got)

    if source in ('earnings_react', 'all'):
        got = earnings_react_episodes(ticker, [d])
        print(f'\n[earnings_react]  当日为财报反应日(E/E+1 ≥{erx.CHG_MIN:.0f}%) {len(got)} 个')
        for e in got:
            print(f'  指示: {e["signal_type"]}  买 {e["entry"]:.2f}'
                  f' / 止损(财报日-1收) {e["stop"]:.2f}')
        eps.extend(got)

    if not eps:
        print('\n当日系统没有给出可入场的指示 — 无可回测的开仓。')
        return

    print(f'\n[ 前向验证 ]  (信号日收盘入场, 系统纪律退出)')
    for e in eps:
        sim = simulate(ticker, e['date'], e['entry'], e['stop'],
                       managed=e.get('managed', True))
        e.update(sim)
        if sim['exit_reason'] is None:
            print(f'  {e["source"]}/{e["signal_type"]}: 无后续数据, 无法验证')
            continue
        print(f'  {e["source"]}/{e["signal_type"]}: '
              f'{EXIT_CN.get(sim["exit_reason"], sim["exit_reason"])} '
              f'@ {sim["exit_date"].date()} ({sim["days_held"]}日)  '
              f'ret {_fmt(sim["ret_pct"])}%  R {_fmt(sim["r_mult"], 2)}  '
              f'fwd21 {_fmt(sim.get("fwd21"))}%  fwd63 {_fmt(sim.get("fwd63"))}%  '
              f'α63 {_fmt(sim.get("alpha63"))}%'
              + ('  [trim曾触发]' if sim.get('trim_hit') else ''))


# ── sweep mode ────────────────────────────────────────────────────────────────
# fork 型 worker 上下文: 在 Pool 创建前填好, 子进程 copy-on-write 继承
# (gate memo 也一样 — 必须在 fork 前算完, 子进程只读)。
_SWEEP_DAYS: list = []
_SWEEP_SOURCE: str = 'both'


def _sweep_worker(ticker: str):
    try:
        eps = []
        if _SWEEP_SOURCE in ('tech_swing', 'both', 'all'):
            eps += tech_swing_episodes(ticker, _SWEEP_DAYS)
        if _SWEEP_SOURCE in ('key_kline', 'both', 'all'):
            eps += key_kline_episodes(ticker, _SWEEP_DAYS[0], _SWEEP_DAYS[-1])
        if _SWEEP_SOURCE in ('gap_scan', 'all'):
            eps += gap_episodes(ticker, _SWEEP_DAYS)
        if _SWEEP_SOURCE in ('chanlun', 'all'):
            eps += chanlun_episodes(ticker, _SWEEP_DAYS)
        if _SWEEP_SOURCE in ('tr_surge', 'all'):
            eps += tr_surge_episodes(ticker, _SWEEP_DAYS)
        if _SWEEP_SOURCE in ('earnings_react', 'all'):
            eps += earnings_react_episodes(ticker, _SWEEP_DAYS)
        return ticker, eps, None
    except Exception as e:
        return ticker, [], f'{type(e).__name__}: {e}'


def run_sweep(tickers: list, start: pd.Timestamp, end: pd.Timestamp,
              source: str, min_n: int, output: str | None,
              jobs: int = 1, tag: str | None = None):
    global _SWEEP_DAYS, _SWEEP_SOURCE
    days = _trading_days(start, end)
    if not days:
        logging.error('区间内无交易日')
        return
    logging.info(f'重放 {len(tickers)} 票 × {len(days)} 交易日 '
                 f'({days[0].date()} → {days[-1].date()}), source={source}, jobs={jobs}')

    # gate state 全区间先算好 (QQQ/SOXX/MAG7, 与个股无关) — 单遍, worker 只读
    for i, d in enumerate(days, 1):
        gate_state_on(d)
        if i % 100 == 0:
            logging.info(f'  market gate 预计算 {i}/{len(days)}')

    _SWEEP_DAYS, _SWEEP_SOURCE = days, source
    eps, failed = [], []
    if jobs > 1:
        with mp.Pool(jobs) as pool:
            for k, (t, got, err) in enumerate(
                    pool.imap_unordered(_sweep_worker, tickers, chunksize=4), 1):
                if err:
                    failed.append(t)
                    logging.warning(f'  [{k}/{len(tickers)}] {t}: FAILED ({err})')
                elif k % 25 == 0 or len(tickers) <= 30:
                    logging.info(f'  [{k}/{len(tickers)}] {t}: {len(got)} episode(s)')
                eps.extend(got)
    else:
        for k, t in enumerate(tickers, 1):
            t2, got, err = _sweep_worker(t)
            if err:
                failed.append(t)
                logging.warning(f'  [{k}/{len(tickers)}] {t}: FAILED ({err})')
            else:
                logging.info(f'  [{k}/{len(tickers)}] {t}: {len(got)} episode(s)')
            eps.extend(got)
    if failed:
        logging.warning(f'{len(failed)} 票失败: {" ".join(failed[:20])}'
                        + (' …' if len(failed) > 20 else ''))

    # undervalue 在父进程做: ROE 网络拉取要全局去重 + 磁盘缓存, 不适合 fork worker
    if source in ('undervalue', 'all'):
        got = undervalue_episodes(tickers, days)
        logging.info(f'  undervalue: {len(got)} episode(s)')
        eps.extend(got)

    logging.info(f'共 {len(eps)} episode(s); 前向模拟中 …')
    for e in eps:
        e.update(simulate(e['ticker'], e['date'], e['entry'], e['stop'],
                          managed=e.get('managed', True)))
    eps.sort(key=lambda e: e['date'])

    if tag is None:
        tag = tickers[0] if len(tickers) == 1 else 'universe'
    if source != 'both':
        tag += f'_{source}'
    run_report(eps, tag, min_n, days[0], days[-1], output)


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    global MAX_HOLD_D
    ap = argparse.ArgumentParser(
        description='回测系统指示: 点位重放 tech_swing / key_kline, 按系统纪律模拟结局')
    ap.add_argument('-asof', '--asof', dest='asof', default=None,
                    help='单日模式: 重现该日指示并前向验证 (需 --ticker)')
    ap.add_argument('--ticker', dest='ticker', default=None,
                    help='标的 (可逗号分隔多只); 区间模式下不传 = 全 watchlist')
    ap.add_argument('--start', dest='start', default=None,
                    help='区间模式起点 YYYY-MM-DD')
    ap.add_argument('--end', dest='end', default=None,
                    help='区间模式终点 (默认今天)')
    ap.add_argument('--source', dest='source', default='both',
                    choices=['tech_swing', 'key_kline', 'gap_scan', 'undervalue',
                             'chanlun', 'tr_surge', 'earnings_react', 'both', 'all'],
                    help='both = tech_swing+key_kline (兼容旧口径); all = 全部七源')
    ap.add_argument('--universe', dest='universe', default=None,
                    choices=['sp500', 'ndx', 'both', 'r2000ht', 'midht', 'all'],
                    help='区间模式股票池: SP500/NDX/并集/R2000医疗+科技切片/全并集 '
                         '(默认 watchlist); --ticker 优先')
    ap.add_argument('--jobs', dest='jobs', type=int, default=0,
                    help='并行进程数 (0=自动: 池>30 用全核, 否则单进程)')
    ap.add_argument('--underval-skip-roe', dest='underval_skip_roe',
                    action='store_true', default=False,
                    help='undervalue 用纯价格版 (无ROE质量门, 2023 前的远期回测必开 — '
                         'yfinance 年报回溯不到; 信号记为 UNDERVAL_P)')
    ap.add_argument('--no-mixed-gate', dest='no_mixed_gate', action='store_true',
                    default=False,
                    help='关闭 tech_swing 的 MIXED 不开新仓门控 (重放门控前的旧行为, '
                         '对照/复现历史回测用)')
    ap.add_argument('--max-hold', dest='max_hold', type=int, default=MAX_HOLD_D)
    ap.add_argument('--min-n', dest='min_n', type=int, default=5,
                    help='聚合组小于此样本数标 ⚠ (default 5)')
    ap.add_argument('--output', dest='output', default=None)
    args = ap.parse_args()

    MAX_HOLD_D = args.max_hold
    global _UNDERVAL_SKIP_ROE
    _UNDERVAL_SKIP_ROE = args.underval_skip_roe
    if args.no_mixed_gate:
        tsw.MIXED_NO_NEW_ENTRY = False
        logging.info('MIXED 门控已关 — 重放旧行为 (MIXED 也出 PULLBACK Setup)')

    tag = None
    if args.ticker:
        tickers = [t.strip().upper() for t in args.ticker.split(',')]
    elif args.universe:
        from t_us_undervalue import load_universe
        tickers = load_universe(args.universe, force=False)
        tag = args.universe
        if not tickers:
            logging.error('股票池抓取失败 — 检查网络/Wikipedia')
            return
    else:
        tickers = list(tsw.UNIVERSE)

    if args.asof:
        asof = pd.Timestamp(args.asof).normalize()
        if not args.ticker:
            logging.error('单日模式需要 --ticker')
            return
        _ensure_deep_history(
            list(dict.fromkeys(tickers + tsw.BAROMETERS + tsw.MAG7 + [BENCH])),
            asof - pd.Timedelta(days=WARMUP_D))
        for t in tickers:
            run_single(t, asof, args.source, args.min_n)
        return

    if not args.start:
        logging.error('要么给 -asof <date> --ticker X (单日), 要么给 --start (区间重放)')
        return
    start = pd.Timestamp(args.start).normalize()
    end = pd.Timestamp(args.end).normalize() if args.end else pd.Timestamp.today().normalize()
    need = start - pd.Timedelta(days=WARMUP_D)
    if len(tickers) > 60:
        # 大池: 骨干票 (barometer/MAG7/QQQ) 逐票保证深度; 个股缓存不够深时分批 bulk
        # 深拉 (远期回测如 2022), 够深则直接用 (每日 cron 维护 3y)
        _ensure_deep_history(
            list(dict.fromkeys(tsw.BAROMETERS + tsw.MAG7 + [BENCH])), need)
        _bulk_deepen(tickers, need)
    else:
        _ensure_deep_history(
            list(dict.fromkeys(tickers + tsw.BAROMETERS + tsw.MAG7 + [BENCH])), need)
    jobs = args.jobs or (min(mp.cpu_count(), 8) if len(tickers) > 30 else 1)
    run_sweep(tickers, start, end, args.source, args.min_n, args.output,
              jobs=jobs, tag=tag)


if __name__ == '__main__':
    main()
