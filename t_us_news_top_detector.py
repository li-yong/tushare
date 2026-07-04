# -*- coding: utf-8 -*-
"""
t_us_news_top_detector.py — 新闻驱动见顶 + 启动探测 (L1 事件层)

Implements docs/news_driven_top_detection.md (§1-5 top, §6 launch mirror):

TOP direction (§1-5):
  news side  — grok_lib.top_scan: structured output (4 straw classes + 主语迁移
               + 按"谁的需求假设被击穿"归因), NOT keyword matching.
  price side — t_us_key_kline.detect_exhaustion_top: §2.1 climax bar
               (跳空新高收回落、放量), the price half of the straw.
  §2.1 rule  — best_news_climax straw requires BOTH halves in the same window
               ("二者缺一不可"): Grok saw the super-beat news AND the bar printed.
  §3 rule    — 主语迁移只对加速段有分辨力: outside the parabolic precondition
               (close ≥ PARABOLIC_DIST above the 20-week proxy MA and no fresh
               climax bar) news-only risk is capped at 'low'.

LAUNCH direction (§6, the mirror — universe & funnel direction are REVERSED):
  funnel     — price FIRST (§6.2): sweep SP500∪NDX ∪ holdings/watch with
               t_us_key_kline's free entry detectors (BREAKOUT/FIRST_KISS/
               POCKET_PIVOT/EARNINGS_GAP), keep names at a fresh & alive entry
               that are NOT extended (dist to 20w proxy < PARABOLIC_DIST), rank
               by key_kline's priority, then Grok-verify only the top N
               (--launch-topn) — that is what bounds cost on a ~600-name pool.
  news side  — grok_lib.launch_scan: 4 spark classes + 主语回归 + 按"谁的需求
               假设正在建立"归因 (the straw taxonomy mirrored).
  §6.1 rule  — catalyst_breakout spark requires BOTH halves in the same window.
  §6.2 rule  — 已延伸 (parabolic) → launch_level capped at 'low' (追高陷阱,
               dip-buy-vs-pop-chase 实证); 无新鲜进场K线 → capped at 'medium'.

Discipline — GPS 不是讨债单: top action tops out at 'de-risk-pending-price-
             confirmation', launch action at 'entry-candidate-pending-price-
             system'; position moves belong to the price system (L0/1.5/2,
             关键K线 entry/stop + 三层确认).
Ledger     — signal_ledger.log_signals, one row per straw/spark hit, real
             (gated) market state; source='news_top' / 'news_launch'; FULL
             LIVE runs only.

Degradation: xAI unreachable / no credits (grok_lib.ping fails) → price-only
rows, news fields empty, the report says so loudly. --asof slices bars hard to
≤ that date; the news side only gets a prompt-level cutoff (web search cannot
be truncated server-side), so backtests are best-effort and labeled as such.

Usage:
  python t_us_news_top_detector.py                     # full weekly (top + launch)
  python t_us_news_top_detector.py --hold-only --force_run
  python t_us_news_top_detector.py --top-only          # 只跑见顶 (tagged)
  python t_us_news_top_detector.py --launch-only --launch-universe ndx
  python t_us_news_top_detector.py --ticker MU --asof 2026-06-25   # MU case, approx
  python t_us_news_top_detector.py --limit 3 --no-news # offline smoke test
Single-ticker / --limit / --asof / direction-restricted runs write tagged files
and never touch the ledger or the daily already-ran guard. --ticker and --limit
skip the launch funnel and scan the same names in both directions.
"""

import os
import sys
import argparse
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)

import t_us_tech_swing as _sw                     # loads select.yml watchlist
from t_us_tech_swing import _fetch_daily
import t_us_key_kline as _kk                      # §2.1 exhaustion-top detector
from t_us_sentiment import SKIP_ETF, load_holdings
import grok_lib
import signal_ledger
from constant import (STRAW_BEST_NEWS_CLIMAX, STRAW_REGULATORY,
                      STRAW_CUSTOMER_CONFLICT, STRAW_DOWNSTREAM_EXCESS,
                      STRAW_TYPES_ALL,
                      SPARK_CATALYST_BREAKOUT, SPARK_SUPPLY_DESTRUCTION,
                      SPARK_DEMAND_COMMITMENT, SPARK_SHORTAGE_ADMISSION,
                      SPARK_TYPES_ALL)

OUT_DIR = '/home/ryan/DATA/result/us_news_top'
DOC = 'docs/news_driven_top_detection.md'

PARABOLIC_DIST = 0.20   # §3 加速段前置条件: close ≥ 20% above the 20w-proxy MA
SMA_20W_PROXY  = 100    # ~20 weeks of daily bars (weekly fetch per name is not worth it)

# §6.1b 利好错杀背离态 (SHOCK_DIVERGENCE): the launch mirror of §2.1's divergence
# logic — 见顶=最好的新闻买不出新高, 启动=最强的新闻砸出深坑. Reference case MU
# 2026-03-30: 03-18 史诗财报+首份5年SCA, 8 bar 内 471→322 (-31.7%), close 仍在
# MA150 上 — key_kline 的顺势进场器 (首个★ 04-22 @487, 距底 +51%) 与 pullback_shock
# (距252日高≤15% 趋势门槛) 全部错过. 实证依据 bottom-entry (质量=防归零, 越早进越好,
# 放量无用 — 故无量能门槛/不等企稳) + dip-buy A类 (强势股急跌是真买点).
SHOCK_DD     = 0.20     # 距 60 日高回撤 ≥ 20% 才算深坑
SHOCK_WINDOW = 15       # 该高点距今 ≤ 15 bar (快崩是错杀, 阴跌 20% 是下降趋势)

RISK_ORDER = {'confirmed_top': 0, 'high': 1, 'medium': 2, 'low': 3,
              'none': 4, 'error': 5}
# doc §3 checklist item numbers derived deterministically from measured fields;
# item 6 (顶后反弹相对强度死) is price-confirmation and stays with the price system.
STRAW_CHECKLIST_ITEM = {STRAW_BEST_NEWS_CLIMAX: 2, STRAW_CUSTOMER_CONFLICT: 3,
                        STRAW_REGULATORY: 4, STRAW_DOWNSTREAM_EXCESS: 5}

# §6 launch mirror of the two tables above (item 1 = 主语回归).
LAUNCH_ORDER = {'confirmed_launch': 0, 'high': 1, 'medium': 2, 'low': 3,
                'none': 4, 'error': 5}
SPARK_CHECKLIST_ITEM = {SPARK_CATALYST_BREAKOUT: 2, SPARK_DEMAND_COMMITMENT: 3,
                        SPARK_SUPPLY_DESTRUCTION: 4, SPARK_SHORTAGE_ADMISSION: 5}


def load_universe(hold_only: bool, watch_only: bool) -> dict:
    """ticker → 'HOLD'/'WATCH'/'HOLD+WATCH'. Watchlist from t_us_tech_swing's
    select.yml globals; holdings live from Futu via t_us_sentiment.load_holdings
    (graceful when OpenD is down). Barometer/cash ETFs are never scanned."""
    skip = SKIP_ETF | set(_sw.BAROMETERS)
    tickers: dict[str, set] = {}
    if not hold_only:
        watch = dict.fromkeys(_sw.MAG7 + _sw.SEMIS + _sw.AI_CHAIN + _sw.HYPERSCALERS)
        for tk in watch:
            if tk not in skip:
                tickers.setdefault(tk, set()).add('WATCH')
    if not watch_only:
        for tk in load_holdings():
            if tk not in skip:
                tickers.setdefault(tk, set()).add('HOLD')
    return {tk: '+'.join(sorted(s)) for tk, s in tickers.items()}


def _shock_divergence(df: pd.DataFrame) -> 'dict | None':
    """§6.1b 背离态: 长趋势未破 (close > MA150) + 距 60 日高深回撤 (≥SHOCK_DD)
    且该高点距今 ≤ SHOCK_WINDOW bar. Returns {dd_pct, stop, risk_pct, hi_date,
    bars_since_high} or None. stop = 急跌段最低 low (跌破 = 还在自由落体, 态失效).

    Expects an indicator-attached frame (needs ma150). Shared by price_check
    and the launch funnel so both judge the state identically."""
    if len(df) < 60:
        return None
    close = float(df['close'].iloc[-1])
    ma150 = df['ma150'].iloc[-1]
    if pd.isna(ma150) or close <= float(ma150):
        return None
    win = df.iloc[-60:]
    hi_pos = int(win['high'].values.argmax())
    hi = float(win['high'].iloc[hi_pos])
    bars_since = len(win) - 1 - hi_pos
    dd = close / hi - 1
    if dd > -SHOCK_DD or bars_since > SHOCK_WINDOW:
        return None
    seg_low = float(win['low'].iloc[hi_pos:].min())
    if close <= seg_low:            # degenerate: close AT the segment low → 1R=0
        return None
    return {
        'dd_pct': round(dd * 100, 1),
        'stop': round(seg_low, 2),
        'risk_pct': round((close - seg_low) / close * 100, 1),
        'hi_date': str(win.index[hi_pos].date()),
        'bars_since_high': int(bars_since),
    }


def price_check(ticker: str, asof: 'pd.Timestamp | None',
                window_days: int) -> dict:
    """Price half of BOTH directions, point-in-time safe under --asof.

    Top half:    climax_recent = an exhaustion-top bar (§2.1 price half) within
                 the news window; parabolic = §3 acceleration precondition.
    Launch half: entry_* = the freshest entry-type key bar (§6.1 price half,
                 key_kline's compute_status: type/stop/1R); entry_fresh = the
                 ★ condition (fresh_enough & alive). shock_* = the §6.1b
                 divergence state (利好错杀) — the OTHER acceptable price half.
                 parabolic doubles as the §6.2 已延伸 cap — one measurement,
                 two mirrored readings."""
    try:
        df = _fetch_daily(ticker)
        if asof is not None and not df.empty:
            df = df[df.index <= asof]
        if df.empty or len(df) < SMA_20W_PROXY + 10:
            return {'ok': False, 'note': f'bars insufficient ({len(df)})'}
        df = _kk._attach_indicators(df.copy())
        last_date = df.index[-1]
        recent = [b for b in _kk.detect_exhaustion_top(df)
                  if (last_date - b['date']).days <= window_days]
        close = float(df['close'].iloc[-1])
        sma = float(df['close'].rolling(SMA_20W_PROXY).mean().iloc[-1])
        dist = close / sma - 1
        st = _kk.compute_status(
            df, _kk.collect_key_bars(df, ticker, fetch_earnings=False))
        fresh = st.get('fresh')
        shock = _shock_divergence(df)
        return {
            'ok': True, 'close': round(close, 2),
            'last_bar': str(last_date.date()),
            'climax_recent': bool(recent),
            'climax_dates': ','.join(str(b['date'].date()) for b in recent),
            'climax_note': recent[-1]['note'] if recent else '',
            'dist_20w_pct': round(dist * 100, 1),
            'parabolic': dist >= PARABOLIC_DIST,
            'entry_type': fresh['type'] if fresh else '',
            'entry_date': str(fresh['date'].date()) if fresh else '',
            'entry_bars_ago': st.get('bars_ago'),
            'entry_stop': (round(float(st['stop']), 2)
                           if st.get('stop') is not None else None),
            'entry_risk_pct': (round(st['risk_pct'] * 100, 1)
                               if st.get('risk_pct') is not None else None),
            'entry_fresh': bool(st.get('fresh_enough') and st.get('alive')),
            'shock_div': shock is not None,
            'shock_dd_pct': shock['dd_pct'] if shock else None,
            'shock_stop': shock['stop'] if shock else None,
            'shock_risk_pct': shock['risk_pct'] if shock else None,
            'shock_hi_date': shock['hi_date'] if shock else '',
            'note': '',
        }
    except Exception as e:
        return {'ok': False, 'note': f'price error: {e}'}


def scan_one(ticker: str, source: str, days_back: int, model: str,
             asof: 'pd.Timestamp | None', cutoff: 'str | None',
             news_enabled: bool) -> dict:
    price = price_check(ticker, asof, days_back)

    news, news_err = None, ''
    if news_enabled:
        try:
            news = grok_lib.top_scan(ticker, model=model, days_back=days_back,
                                     cutoff=cutoff)
        except Exception as e:
            news_err = str(e)

    # Straws: news-side classes straight from the structured response; the
    # §2.1 straw needs both halves — Grok saw the super-beat news AND the
    # exhaustion bar printed in the same window.
    news_straws = [s for s in (news or {}).get('straw_types', [])
                   if s in STRAW_TYPES_ALL]
    straws = [s for s in news_straws if s != STRAW_BEST_NEWS_CLIMAX]
    if STRAW_BEST_NEWS_CLIMAX in news_straws and price.get('climax_recent'):
        straws.append(STRAW_BEST_NEWS_CLIMAX)

    subject_migration = bool((news or {}).get('subject_migration'))
    checklist = sorted(({1} if subject_migration else set())
                       | {STRAW_CHECKLIST_ITEM[s] for s in straws})

    news_risk = (news or {}).get('top_risk_level', 'none')
    if news_risk not in RISK_ORDER:
        news_risk = 'none'
    risk = news_risk
    if STRAW_BEST_NEWS_CLIMAX in straws and RISK_ORDER[risk] > RISK_ORDER['high']:
        risk = 'high'                          # §2.1 both halves → at least high
    if (price.get('ok') and not price.get('parabolic')
            and not price.get('climax_recent')
            and RISK_ORDER[risk] < RISK_ORDER['low']):
        risk = 'low'                           # §3: 非加速段, 主语迁移分辨力低

    action = ('de-risk-pending-price-confirmation'
              if straws and risk in ('high', 'confirmed_top') else 'monitor')

    meta = (news or {}).get('_meta', {})
    summary = (news or {}).get('summary', '') or \
        ('新闻层不可用(仅价格侧)' + (f': {news_err}' if news_err else ''))
    return {
        'direction': 'top',
        'ticker': ticker, 'source': source,
        'top_risk_level': risk, 'news_risk': news_risk,
        'straw_types': straws, 'subject_migration': subject_migration,
        'checklist_hits': checklist,
        'last_straw': (news or {}).get('last_straw', ''),
        'demand_victim': (news or {}).get('demand_victim', ''),
        'confidence': (news or {}).get('confidence'),
        'summary': summary,
        'action': action,
        'close': price.get('close'),
        'climax_recent': bool(price.get('climax_recent')),
        'climax_dates': price.get('climax_dates', ''),
        'dist_20w_pct': price.get('dist_20w_pct'),
        'parabolic': price.get('parabolic'),
        'price_note': price.get('note', ''),
        'citations': ' '.join((meta.get('citations') or [])[:5]),
        'cost_usd_est': meta.get('cost_usd_est') or 0.0,
        'news_error': news_err,
    }


def scan_one_launch(ticker: str, source: str, days_back: int, model: str,
                    asof: 'pd.Timestamp | None', cutoff: 'str | None',
                    news_enabled: bool) -> dict:
    """§6 mirror of scan_one: sparks instead of straws; the §6.2 caps replace
    the §3 cap (已延伸 → 追高陷阱封顶 low; 无新鲜进场K线 → 封顶 medium)."""
    price = price_check(ticker, asof, days_back)

    news, news_err = None, ''
    if news_enabled:
        try:
            news = grok_lib.launch_scan(ticker, model=model, days_back=days_back,
                                        cutoff=cutoff)
        except Exception as e:
            news_err = str(e)

    # Sparks: news-side classes straight from the structured response; the
    # §6.1 spark needs both halves — Grok saw the catalyst AND the price half
    # holds: EITHER an entry-type key bar is fresh & alive (顺势, §6.1) OR the
    # §6.1b 利好错杀 divergence state (最强新闻砸出深坑, the MU 03-30 signature).
    price_half = bool(price.get('entry_fresh') or price.get('shock_div'))
    news_sparks = [s for s in (news or {}).get('spark_types', [])
                   if s in SPARK_TYPES_ALL]
    sparks = [s for s in news_sparks if s != SPARK_CATALYST_BREAKOUT]
    if SPARK_CATALYST_BREAKOUT in news_sparks and price_half:
        sparks.append(SPARK_CATALYST_BREAKOUT)

    subject_birth = bool((news or {}).get('subject_birth'))
    checklist = sorted(({1} if subject_birth else set())
                       | {SPARK_CHECKLIST_ITEM[s] for s in sparks})

    news_level = (news or {}).get('launch_level', 'none')
    if news_level not in LAUNCH_ORDER:
        news_level = 'none'
    level = news_level
    if (SPARK_CATALYST_BREAKOUT in sparks
            and LAUNCH_ORDER[level] > LAUNCH_ORDER['high']):
        level = 'high'                     # §6.1 both halves → at least high
    # §6.2 caps. The divergence state exempts the 无价格半 cap (the state IS
    # the price half) but NOT the 已延伸 cap: a -20% hole in a still-parabolic
    # name is the TOP case's first leg down, not 错杀. n=2 separation: MU
    # 03-30 (dist20w -3.5%, top=none → real launch, +178%) vs MU 07-03
    # (dist20w +54.8%, top=high ☠ the same day → post-climax crash).
    if price.get('ok'):
        if price.get('parabolic'):
            if LAUNCH_ORDER[level] < LAUNCH_ORDER['low']:
                level = 'low'              # 已延伸: cap wins, 背离态不豁免
        elif (not price_half
                and LAUNCH_ORDER[level] < LAUNCH_ORDER['medium']):
            level = 'medium'               # 火种未获价格确认

    action = ('entry-candidate-pending-price-system'
              if sparks and level in ('high', 'confirmed_launch') else 'monitor')

    # The stop the price half implies right now: 顺势半用关键K线止损, 背离半用
    # 急跌段最低 (both are WHERE the premise breaks). Report + ledger use this.
    if price.get('entry_fresh'):
        live_stop, live_risk = price.get('entry_stop'), price.get('entry_risk_pct')
    elif price.get('shock_div'):
        live_stop, live_risk = price.get('shock_stop'), price.get('shock_risk_pct')
    else:
        live_stop, live_risk = None, None

    meta = (news or {}).get('_meta', {})
    summary = (news or {}).get('summary', '') or \
        ('新闻层不可用(仅价格侧)' + (f': {news_err}' if news_err else ''))
    return {
        'direction': 'launch',
        'ticker': ticker, 'source': source,
        'launch_level': level, 'news_level': news_level,
        'spark_types': sparks, 'subject_birth': subject_birth,
        'checklist_hits': checklist,
        'first_spark': (news or {}).get('first_spark', ''),
        'demand_builder': (news or {}).get('demand_builder', ''),
        'confidence': (news or {}).get('confidence'),
        'summary': summary,
        'action': action,
        'close': price.get('close'),
        'entry_type': price.get('entry_type', ''),
        'entry_date': price.get('entry_date', ''),
        'entry_bars_ago': price.get('entry_bars_ago'),
        'entry_stop': price.get('entry_stop'),
        'entry_risk_pct': price.get('entry_risk_pct'),
        'entry_fresh': bool(price.get('entry_fresh')),
        'shock_div': bool(price.get('shock_div')),
        'shock_dd_pct': price.get('shock_dd_pct'),
        'shock_stop': price.get('shock_stop'),
        'shock_risk_pct': price.get('shock_risk_pct'),
        'live_stop': live_stop, 'live_risk_pct': live_risk,
        'dist_20w_pct': price.get('dist_20w_pct'),
        'parabolic': price.get('parabolic'),
        'price_note': price.get('note', ''),
        'citations': ' '.join((meta.get('citations') or [])[:5]),
        'cost_usd_est': meta.get('cost_usd_est') or 0.0,
        'news_error': news_err,
    }


def launch_candidates(universe: str, topn: int, extra: dict,
                      asof: 'pd.Timestamp | None') -> dict:
    """§6.2 价格先行漏斗 → {ticker: source}, at most `topn` names.

    Pool = SP500∪NDX (key_kline --scan's coarse quality gate) ∪ the top-scan
    universe (`extra`: holdings/watch outside the indices still get checked).
    Keep = EITHER a fresh & alive entry-type key bar (key_kline's ★ condition)
    AND not extended (dist to 20w proxy < PARABOLIC_DIST — 已延伸是追高不是启动),
    OR the §6.1b 利好错杀 divergence state (deep fast drawdown above MA150 —
    rare, so it outranks every 顺势 type and never gets cut by topn). Price
    side only, no Grok — this free filter is what bounds news-side cost on a
    ~600-name universe."""
    from t_us_undervalue import load_universe as _load_pool
    try:
        pool = _load_pool(universe, False)
    except Exception as e:
        logging.warning(f'launch pool load failed ({e}) — 只查 持仓/观察池')
        pool = []
    pool = list(dict.fromkeys(list(pool) + list(extra)))
    if asof is not None:
        _kk._ASOF = asof                    # _bulk_ohlcv drops bars > asof
    frames = _kk._bulk_ohlcv(pool, period='1y')
    scored = []   # (ticker, priority, above, risk_pct)
    for tk, df in frames.items():
        try:
            _kk._attach_indicators(df)
            # §6.2 已延伸 gate first, for BOTH price halves: an extended name's
            # deep hole is the top case's first leg (launch_level would cap to
            # low anyway) — don't spend a Grok slot on it; the top scan owns it.
            sma = df['close'].rolling(SMA_20W_PROXY).mean().iloc[-1]
            if pd.isna(sma) or float(df['close'].iloc[-1]) / float(sma) - 1 >= PARABOLIC_DIST:
                continue
            if _shock_divergence(df) is not None:      # §6.1b 背离态: 最高优先
                scored.append((tk, -1, 3, 0))
                continue
            st = _kk.compute_status(
                df, _kk.collect_key_bars(df, tk, fetch_earnings=False))
            if not st or st['fresh'] is None:
                continue
            if not (st['fresh_enough'] and st['alive']):
                continue
            scored.append((tk, _kk._TYPE_PRIORITY.get(st['fresh']['type'], 9),
                           st['above'], st['risk_pct'] or 9))
        except Exception as e:
            logging.debug(f'{tk}: funnel error {e}')
    scored.sort(key=lambda x: (x[1], -x[2], x[3]))
    out = {}
    for tk, *_ in scored[:topn]:
        out[tk] = f'{extra[tk]}+SCAN' if tk in extra else 'SCAN'
    logging.info(f'launch funnel: {len(frames)} 只有效行情 → {len(scored)} 只过滤后 '
                 f'→ 前 {len(out)} 只进新闻侧验证')
    return out


def write_report(top_rows: list, launch_rows: list, md_path: str,
                 csv_path: str, total_cost: float, market_state: str,
                 news_enabled: bool, asof_label: 'str | None',
                 do_top: bool, do_launch: bool) -> None:
    # One CSV for both directions ('direction' column tells them apart; columns
    # are the union, blank where the other direction has no such field).
    pd.DataFrame(top_rows + launch_rows).to_csv(csv_path, index=False)

    def _order_top(r):
        return ('HOLD' not in r.get('source', ''),
                RISK_ORDER.get(r.get('top_risk_level'), 99))

    def _order_launch(r):
        return (LAUNCH_ORDER.get(r.get('launch_level'), 99),
                not r.get('entry_fresh'))

    top_rows = sorted(top_rows, key=_order_top)
    launch_rows = sorted(launch_rows, key=_order_launch)
    flagged = [r for r in top_rows
               if r['top_risk_level'] in ('high', 'confirmed_top') and r['straw_types']]
    lit = [r for r in launch_rows
           if r['launch_level'] in ('high', 'confirmed_launch') and r['spark_types']]

    out = [
        f'# 新闻驱动顶部/启动探测 · {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}',
        '',
        f'> 方法论 {DOC}(§1-5 见顶 / §6 启动镜像)。见顶 {len(top_rows)} 只 · '
        f'启动候选 {len(launch_rows)} 只,market state={market_state},'
        f' Grok 成本 ≈ ${total_cost:.2f}。',
        '> **纪律**: 全部是状态测量(GPS),不是讨债单;仓位动作只在价格系统内执行'
        '(减仓走 L0/L1.5/L2 或相对强度死;进场走关键K线 entry/stop + 三层确认)。',
    ]
    if not news_enabled:
        out.append('> ⚠️ **新闻层不可用(xAI ping 失败)——本报告只有价格侧信息。**')
    if asof_label:
        out.append(f'> ⚠️ --asof {asof_label} 回测: 价格侧严格 point-in-time;'
                   '新闻侧仅 prompt 级截断,结果只能当 best-effort。')

    out += ['', '## ⚠️ 稻草命中 (L1: 减仓待价格确认)', '']
    if not do_top:
        out.append('_本次未跑见顶方向 (--launch-only)。_')
    elif flagged:
        for r in flagged:
            out += [
                f"### {r['ticker']} **{r['top_risk_level'].upper()}** [{r['source']}]",
                f"- 结论: {r['summary']}",
                f"- 稻草: {', '.join(r['straw_types'])} | 主语迁移: {r['subject_migration']}"
                f" | checklist: {r['checklist_hits']}",
                f"- 需求假设受害者: {r['demand_victim'] or '—'} | 最后稻草: {r['last_straw'] or '—'}",
                f"- 价格: close={r['close']} dist20w={r['dist_20w_pct']}%"
                f" parabolic={r['parabolic']} climax={r['climax_dates'] or '无'}",
                f"- 行动: {r['action']}",
                '',
            ]
    else:
        out.append('_无稻草命中。_')

    out += ['', '## 🔥 火种命中 (启动候选: 进场走价格系统)', '']
    if not do_launch:
        out.append('_本次未跑启动方向 (--top-only)。_')
    elif lit:
        for r in lit:
            entry = (f"{r['entry_type']} {r['entry_date']}"
                     f"({r['entry_bars_ago']} bar 前)"
                     if r['entry_type'] else '无')
            if r.get('shock_div'):
                entry += f" | 背离态: 距60日高{r['shock_dd_pct']}%(利好错杀)"
            ride = (f" | 若上车: 止损 ${r['live_stop']} / 1R {r['live_risk_pct']}%"
                    if (r['entry_fresh'] or r.get('shock_div'))
                    and r.get('live_stop') is not None else '')
            out += [
                f"### {r['ticker']} **{r['launch_level'].upper()}** [{r['source']}]",
                f"- 结论: {r['summary']}",
                f"- 火种: {', '.join(r['spark_types'])} | 主语回归: {r['subject_birth']}"
                f" | checklist: {r['checklist_hits']}",
                f"- 需求假设建立者: {r['demand_builder'] or '—'} | 第一根火种: {r['first_spark'] or '—'}",
                f"- 价格: close={r['close']} dist20w={r['dist_20w_pct']}%"
                f" 进场K线={entry}{ride}",
                f"- 行动: {r['action']}",
                '',
            ]
    else:
        out.append('_无火种命中。_')

    out += ['', '## 全部结果', '']
    if top_rows:
        out.append('见顶方向:')
        for r in top_rows:
            mark = '☠' if r['climax_recent'] else ' '
            out.append(f"- {r['ticker']:6} [{r['source']:10}] {r['top_risk_level']:9}"
                       f"{mark} | {str(r['summary'])[:80]}")
        out.append('')
    if launch_rows:
        out.append('启动方向 (🔥=顺势进场K线 ⚡=利好错杀背离态):')
        for r in launch_rows:
            mark = '⚡' if r.get('shock_div') else '🔥' if r['entry_fresh'] else ' '
            ptype = ('SHOCK_DIV' if r.get('shock_div') and not r['entry_fresh']
                     else r['entry_type'] or '—')
            out.append(f"- {r['ticker']:6} [{r['source']:10}] {r['launch_level']:9}"
                       f"{mark} | {ptype:12} | {str(r['summary'])[:70]}")
    out += ['', f'案例登记簿见 {DOC} §5 / §6.4(本报告不复制,以文档为准)。', '']

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(out))
    logging.info(f'Report: {md_path}')
    logging.info(f'CSV:    {csv_path}')


def main() -> int:
    ap = argparse.ArgumentParser(description='News-driven top & launch detector (L1 event layer)')
    ap.add_argument('--hold-only', action='store_true')
    ap.add_argument('--watch-only', action='store_true')
    ap.add_argument('--ticker', help='Single ticker, both directions (tagged output, no ledger)')
    ap.add_argument('--limit', type=int, default=0, help='Test: first N tickers (tagged output, no ledger)')
    ap.add_argument('--days-back', type=int, default=14)
    ap.add_argument('--model', default=grok_lib.DEFAULT_MODEL)
    ap.add_argument('--no-news', action='store_true', help='Price side only (offline)')
    ap.add_argument('--top-only', action='store_true', help='只跑见顶方向 (tagged run)')
    ap.add_argument('--launch-only', action='store_true', help='只跑启动方向 (tagged run)')
    ap.add_argument('--launch-universe', default='both', choices=['sp500', 'ndx', 'both'],
                    help='启动漏斗宇宙 (default both = SP500∪NDX)')
    ap.add_argument('--launch-topn', type=int, default=12,
                    help='漏斗后进新闻侧验证的候选上限 (default 12, ~$0.1/只)')
    ap.add_argument('--force_run', action='store_true')
    ap.add_argument('--asof', help='Backtest YYYY-MM-DD (price strict, news best-effort)')
    args = ap.parse_args()

    if args.top_only and args.launch_only:
        ap.error('--top-only 与 --launch-only 互斥')
    do_top, do_launch = not args.launch_only, not args.top_only

    asof_ts = None
    if args.asof:
        try:
            asof_ts = pd.Timestamp(args.asof).normalize()
        except ValueError:
            ap.error(f'--asof 无法解析: {args.asof}')
    date_str = (asof_ts or pd.Timestamp.today()).strftime('%Y%m%d')

    # Tagged runs (test/backtest/spot-check/direction-restricted) can never
    # clobber the weekly report nor satisfy its already-ran guard, and never
    # write the ledger.
    tag = ''
    if args.ticker:
        tag = f'_{args.ticker.upper()}'
    elif args.limit:
        tag = f'_limit{args.limit}'
    if args.top_only:
        tag += '_toponly'
    elif args.launch_only:
        tag += '_launchonly'
    if asof_ts is not None:
        tag += '_asof'
    full_live = (tag == '')

    os.makedirs(OUT_DIR, exist_ok=True)
    md_path = os.path.join(OUT_DIR, f'us_news_top_{date_str}{tag}.md')
    csv_path = os.path.join(OUT_DIR, f'us_news_top_{date_str}{tag}.csv')

    if full_live and not args.force_run and os.path.exists(csv_path):
        print(f'今日已跑,跳过 (--force_run 重跑): {csv_path}')
        print(pd.read_csv(csv_path).to_string(index=False))
        return 0

    if args.ticker:
        tickers = {args.ticker.upper(): 'SINGLE'}
    else:
        tickers = load_universe(args.hold_only, args.watch_only)
        if args.limit:
            tickers = dict(list(tickers.items())[:args.limit])
    if not tickers:
        print('No tickers to scan.')
        return 1

    # One cheap ping before fanning out N paid calls; on failure degrade to
    # price-only instead of writing N identical error rows.
    news_enabled = not args.no_news
    if news_enabled:
        try:
            grok_lib.ping(model=args.model)
        except Exception as e:
            logging.error(f'xAI unreachable ({e}) — degrading to price-only run')
            news_enabled = False

    market_state = 'UNKNOWN'
    if full_live:
        try:
            baro_state, _ = _sw.get_market_state()
            lead_frac, _ = _sw.get_leadership_breadth()
            market_state = _sw._gate_state(baro_state, lead_frac)
        except Exception as e:
            logging.warning(f'market state unavailable: {e}')

    # 启动方向的候选池: 全量跑走 §6.2 价格先行漏斗 (SP500∪NDX ∪ 持仓/观察池);
    # --ticker/--limit 测试跑跳过漏斗, 对同一批名字双向扫 (快、可离线)。
    launch_tickers: dict = {}
    if do_launch:
        if args.ticker or args.limit:
            launch_tickers = dict(tickers)
        else:
            launch_tickers = launch_candidates(args.launch_universe,
                                               args.launch_topn, tickers, asof_ts)

    print(f'News Top/Launch Detector: 见顶 {len(tickers) if do_top else 0} 只 · '
          f'启动候选 {len(launch_tickers)} 只 '
          f'(news={"on" if news_enabled else "OFF"}, asof={args.asof or "live"})')

    rows_top, rows_launch, total_cost = [], [], 0.0
    with ThreadPoolExecutor(max_workers=3) as ex:   # rate-limit Grok
        futs = {}
        if do_top:
            for tk, src in tickers.items():
                futs[ex.submit(scan_one, tk, src, args.days_back, args.model,
                               asof_ts, args.asof, news_enabled)] = 'top'
        for tk, src in launch_tickers.items():
            futs[ex.submit(scan_one_launch, tk, src, args.days_back, args.model,
                           asof_ts, args.asof, news_enabled)] = 'launch'
        for fut in as_completed(futs):
            r = fut.result()
            if futs[fut] == 'top':
                rows_top.append(r)
                mark = '☠' if r['climax_recent'] else ' '
                lvl = r['top_risk_level']
            else:
                rows_launch.append(r)
                mark = ('⚡' if r['shock_div'] else
                        '🔥' if r['entry_fresh'] else ' ')
                lvl = r['launch_level']
            total_cost += r['cost_usd_est']
            print(f"  {'顶' if futs[fut] == 'top' else '启'} {r['ticker']:6} "
                  f"{lvl:9} {mark} | {str(r['summary'])[:60]}")

    rows_all = rows_top + rows_launch
    if not rows_all or all(r['news_error'] and not r['close'] for r in rows_all):
        logging.error('every ticker failed on both news and price side — not writing report')
        return 1

    # 归因账本: 每个稻草/火种命中一行 (episode 键 = source×ticker×signal_type),
    # 真实 gated market state; live 全量跑才写 (--asof/--ticker/--limit/限向 不写)。
    # 火种行带 entry/stop (关键K线止损), 供 signal_attrib 算 stop-hit。
    if full_live and news_enabled:
        straw_rows = [{'ticker': r['ticker'], 'signal_type': straw,
                       'confidence': r['confidence'],
                       'market_state': market_state, 'close': r['close']}
                      for r in rows_top for straw in r['straw_types']]
        spark_rows = [{'ticker': r['ticker'], 'signal_type': spark,
                       'confidence': r['confidence'],
                       'market_state': market_state, 'close': r['close'],
                       'entry': r['close'], 'stop': r['live_stop']}
                      for r in rows_launch for spark in r['spark_types']]
        for src_name, lrows in (('news_top', straw_rows), ('news_launch', spark_rows)):
            if lrows:
                try:
                    signal_ledger.log_signals(lrows, source=src_name)
                except Exception as e:
                    logging.warning(f'signal ledger write failed [{src_name}]: {e}')

    write_report(rows_top, rows_launch, md_path, csv_path, total_cost,
                 market_state, news_enabled, args.asof, do_top, do_launch)
    print(f'\nDone. Report: {md_path}  (Grok cost ≈ ${total_cost:.2f})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
