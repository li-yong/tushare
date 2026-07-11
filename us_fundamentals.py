# coding: utf-8
"""
US fundamentals source layer — shared by t_us_premium.py (贵气/L2) and
t_us_delivery.py (兑现/L3).

数据源分工(用户偏好:基本面优先 Futu):
  - Futu OpenD F10 为主源:估值历史分位 / 季度财务(营收 yoy/qoq) / 分析师评级目标价。
  - yfinance 为降级源:EPS 超预期(Surprise%)、分析师上调方向(eps_revisions)——
    Futu 这两项给不了时序;以及 Futu OpenD 不可达时,估值/评级的兜底。

一次运行共享一个 OpenQuoteContext(惰性打开,atexit 关闭)。每只票的合并结果
当日缓存为 JSON,避免重复联网。任何子项取数失败 → 该项 None/[] 并记入 flags,
绝不抛断主流程(离线可跑)。

ADR-0001 约束的是【行情 K 线】唯一源(yfinance);基本面是另一类数据,
Futu/yfinance 在此并存不违反该决策。
"""

import os
import json
import atexit
import socket
import logging
import datetime

FUTU_HOST = os.environ.get('FUTU_OPEND_HOST', '127.0.0.1')
FUTU_PORT = int(os.environ.get('FUTU_OPEND_PORT', 11111))

FUNDA_CACHE_DIR = '/home/ryan/DATA/DAY_Global/US_funda'  # 一只票一份当日 JSON
REVENUE_FIELD_ID = 8001   # Futu get_financials_statements: 营业总收入(实测)


# ── Futu context (lazy singleton, shared across the run) ───────────────────────
_FUTU_CTX = None
_FUTU_TRIED = False


def _futu():
    """Return a live OpenQuoteContext, or None if OpenD is unreachable.

    Tried at most once per run; failures are cached so we don't retry the
    (slow) connect for every ticker.
    """
    global _FUTU_CTX, _FUTU_TRIED
    if _FUTU_CTX is not None:
        return _FUTU_CTX
    if _FUTU_TRIED:
        return None
    _FUTU_TRIED = True
    # Fast fail: probe the port first so an unreachable OpenD degrades in ~2s
    # instead of OpenQuoteContext's slow internal connect/retry (which can hang).
    try:
        with socket.create_connection((FUTU_HOST, FUTU_PORT), timeout=2):
            pass
    except Exception as e:
        logging.warning(f'Futu OpenD not listening on {FUTU_HOST}:{FUTU_PORT} ({e}) '
                        f'— fundamentals fall back to yfinance')
        return None
    try:
        from futu import OpenQuoteContext, RET_OK
        logging.getLogger('FTConsoleLog').setLevel(logging.WARNING)
        ctx = OpenQuoteContext(host=FUTU_HOST, port=FUTU_PORT)
        ret, _ = ctx.get_global_state()      # health check
        if ret != RET_OK:
            ctx.close()
            logging.warning('Futu OpenD reachable but get_global_state failed — using yfinance fallback')
            return None
        _FUTU_CTX = ctx
        return _FUTU_CTX
    except Exception as e:
        logging.warning(f'Futu OpenD unavailable ({e}) — fundamentals fall back to yfinance')
        return None


def close_futu():
    global _FUTU_CTX
    if _FUTU_CTX is not None:
        try:
            _FUTU_CTX.close()
        except Exception:
            pass
        _FUTU_CTX = None


atexit.register(close_futu)


# ── Cache ──────────────────────────────────────────────────────────────────────
def _cache_path(ticker: str) -> str:
    return os.path.join(FUNDA_CACHE_DIR, f'{ticker}.json')


def _cache_fresh(path: str) -> bool:
    """Fresh = written today (mirrors t_us_tech_swing._cache_is_fresh)."""
    if not os.path.exists(path):
        return False
    mtime = datetime.date.fromtimestamp(os.path.getmtime(path))
    return mtime == datetime.date.today()


# ── Futu source funcs (primary) ────────────────────────────────────────────────
def _futu_valuation(ticker: str) -> dict | None:
    ctx = _futu()
    if ctx is None:
        return None
    try:
        from futu import RET_OK
        ret, d = ctx.get_valuation_detail(f'US.{ticker}', valuation_type=1)  # 1 = PE
        if ret != RET_OK or not isinstance(d, dict):
            return None
        tr = d.get('trend') or {}
        pct = tr.get('valuation_percentile')   # Futu 单位是百分数(0.8 = 0.80%)
        return {
            'pe':          tr.get('current_value'),
            'pe_pctile':   (pct / 100.0) if pct is not None else None,  # 存成 0..1 分数
            'pe_mean':     tr.get('average_value'),
            'pe_forecast': tr.get('forward_value'),
            'fwd_pe':      tr.get('forward_value'),
            'trail_pe':    tr.get('current_value'),
            'source':      'futu',
        }
    except Exception as e:
        logging.debug(f'{ticker}: futu valuation failed ({e})')
        return None


def _futu_revenue(ticker: str) -> list | None:
    ctx = _futu()
    if ctx is None:
        return None
    try:
        from futu import RET_OK
        ret, d = ctx.get_financials_statements(
            f'US.{ticker}', statement_type=1, financial_type=9)  # 利润表, 单季报组合
        if ret != RET_OK or not isinstance(d, dict):
            return None
        out = []
        for rep in (d.get('report_list') or []):    # 新→旧
            rev = next((it for it in rep.get('item_list', [])
                        if str(it.get('field_id')) == str(REVENUE_FIELD_ID)), None)
            if rev is None:
                continue
            out.append({
                'period':  rep.get('period_text'),
                'date':    rep.get('date_time_str'),
                'revenue': rev.get('data'),
                'yoy':     rev.get('yoy'),
                'qoq':     rev.get('qoq'),
            })
        return out or None
    except Exception as e:
        logging.debug(f'{ticker}: futu revenue failed ({e})')
        return None


def _futu_consensus(ticker: str) -> dict | None:
    ctx = _futu()
    if ctx is None:
        return None
    try:
        from futu import RET_OK
        ret, d = ctx.get_research_analyst_consensus(f'US.{ticker}')
        if ret != RET_OK or not isinstance(d, dict):
            return None
        return {
            'target_mean': d.get('average'),
            'target_high': d.get('highest'),
            'target_low':  d.get('lowest'),
            'buy_pct':     d.get('buy'),
            'rating':      d.get('rating'),
            'total':       d.get('total'),
            'source':      'futu',
        }
    except Exception as e:
        logging.debug(f'{ticker}: futu consensus failed ({e})')
        return None


# ── yfinance source funcs (fallback / surprise & revisions) ────────────────────
def _yf_ticker(ticker: str):
    import yfinance as yf
    return yf.Ticker(ticker)


def _yf_valuation(ticker: str) -> dict | None:
    """Current-snapshot PE only (no history → pe_pctile stays None)."""
    try:
        info = _yf_ticker(ticker).info or {}
        trail, fwd = info.get('trailingPE'), info.get('forwardPE')
        if trail is None and fwd is None:
            return None
        return {
            'pe':          trail or fwd,
            'pe_pctile':   None,
            'pe_mean':     None,
            'pe_forecast': fwd,
            'fwd_pe':      fwd,
            'trail_pe':    trail,
            'source':      'yfinance',
        }
    except Exception as e:
        logging.debug(f'{ticker}: yf valuation failed ({e})')
        return None


def _yf_revenue(ticker: str) -> list | None:
    """Quarterly Total Revenue with self-computed YoY (4-quarter lag)."""
    try:
        q = _yf_ticker(ticker).quarterly_income_stmt
        if q is None or q.empty:
            return None
        row = next((r for r in q.index if 'Total Revenue' in r), None)
        if row is None:
            return None
        cols = list(q.columns)              # 新→旧
        rev = [float(q.loc[row, c]) for c in cols]
        out = []
        for i, c in enumerate(cols):
            yoy = None
            if i + 4 < len(rev) and rev[i + 4]:
                yoy = (rev[i] / rev[i + 4] - 1) * 100
            qoq = None
            if i + 1 < len(rev) and rev[i + 1]:
                qoq = (rev[i] / rev[i + 1] - 1) * 100
            out.append({'period': str(c.date()) if hasattr(c, 'date') else str(c),
                        'date': str(c.date()) if hasattr(c, 'date') else str(c),
                        'revenue': rev[i], 'yoy': yoy, 'qoq': qoq})
        return out or None
    except Exception as e:
        logging.debug(f'{ticker}: yf revenue failed ({e})')
        return None


def _yf_surprises(ticker: str) -> list | None:
    """EPS Surprise% per earnings, newest→oldest (reported quarters only).

    走共享财报日历磁盘缓存 (t_us_tech_swing.fetch_earnings_calendar, 3日 TTL,
    失败服务陈旧缓存) — 与 key_kline / earnings_react 同吃一份日历, 不再直打
    yfinance get_earnings_dates。"""
    try:
        from t_us_tech_swing import fetch_earnings_calendar
        cal = fetch_earnings_calendar(ticker)
        if not cal:
            return None
        # cal 升序、含未来排程日(surprise=None) → 反转成新→旧, 只留已公布的
        out = [float(r['surprise']) for r in reversed(cal) if r['surprise'] is not None]
        return out or None
    except Exception as e:
        logging.debug(f'{ticker}: yf surprises failed ({e})')
        return None


def _yf_revisions(ticker: str) -> dict | None:
    """Analyst estimate-revision direction over the last 30 days (PEAD proxy)."""
    try:
        t = _yf_ticker(ticker)
        up = down = None
        trend_up = None
        rev = getattr(t, 'eps_revisions', None)
        if rev is not None and hasattr(rev, 'loc') and not rev.empty:
            # 取最近一期(0q)的上/下调家数
            idx = '0q' if '0q' in rev.index else rev.index[0]
            up = int(rev.loc[idx].get('upLast30days')) if 'upLast30days' in rev.columns else None
            down = int(rev.loc[idx].get('downLast30days')) if 'downLast30days' in rev.columns else None
        tr = getattr(t, 'eps_trend', None)
        if tr is not None and hasattr(tr, 'loc') and not tr.empty:
            idx = '0q' if '0q' in tr.index else tr.index[0]
            cur = tr.loc[idx].get('current')
            old = tr.loc[idx].get('90daysAgo')
            if cur is not None and old is not None:
                trend_up = bool(cur > old)
        if up is None and down is None and trend_up is None:
            return None
        return {'up30': up, 'down30': down, 'trend_up': trend_up}
    except Exception as e:
        logging.debug(f'{ticker}: yf revisions failed ({e})')
        return None


# ── Public API ─────────────────────────────────────────────────────────────────
def fetch_all(ticker: str, use_cache: bool = True, use_futu: bool = True) -> dict:
    """
    Merged fundamentals for one ticker (Futu-primary, yfinance-fallback).

    Returns a dict with keys: ticker, asof, valuation, revenue, surprises,
    revisions, consensus, flags. Missing pieces are None/[] and named in
    `flags`. The Futu-primary result is cached per ticker per day.

    use_futu=False forces a yfinance-only run and neither reads nor writes the
    cache, so a deliberate offline run won't be masked by — or pollute — today's
    Futu-sourced cache.
    """
    ticker = ticker.upper()
    path = _cache_path(ticker)
    if use_futu and use_cache and _cache_fresh(path):
        try:
            with open(path) as fh:
                return json.load(fh)
        except Exception:
            pass

    flags = []

    valuation = (_futu_valuation(ticker) if use_futu else None) or _yf_valuation(ticker)
    if valuation is None:
        flags.append('noval')
    elif valuation.get('pe_pctile') is None:
        flags.append('noval_pctile')   # 有 PE 但无历史分位(yfinance 兜底)

    revenue = (_futu_revenue(ticker) if use_futu else None) or _yf_revenue(ticker) or []
    if not revenue:
        flags.append('norev')

    surprises = _yf_surprises(ticker) or []
    if not surprises:
        flags.append('nosurprise')

    revisions = _yf_revisions(ticker)
    if revisions is None:
        flags.append('norevisions')

    consensus = (_futu_consensus(ticker) if use_futu else None)
    if consensus is None:
        consensus = _yf_consensus_fallback(ticker)
    if consensus is None:
        flags.append('noconsensus')

    result = {
        'ticker':    ticker,
        'asof':      datetime.date.today().isoformat(),
        'valuation': valuation,
        'revenue':   revenue,
        'surprises': surprises,
        'revisions': revisions,
        'consensus': consensus,
        'flags':     flags,
    }

    if use_futu:   # 只持久化 Futu-优先的完整结果, 不让离线运行污染缓存
        try:
            os.makedirs(FUNDA_CACHE_DIR, exist_ok=True)
            with open(path, 'w') as fh:
                json.dump(result, fh, ensure_ascii=False, indent=1)
        except Exception as e:
            logging.debug(f'{ticker}: funda cache write failed ({e})')

    return result


def _yf_consensus_fallback(ticker: str) -> dict | None:
    try:
        info = _yf_ticker(ticker).info or {}
        tm = info.get('targetMeanPrice')
        if tm is None:
            return None
        return {
            'target_mean': tm,
            'target_high': info.get('targetHighPrice'),
            'target_low':  info.get('targetLowPrice'),
            'buy_pct':     None,
            'rating':      info.get('recommendationKey'),
            'total':       info.get('numberOfAnalystOpinions'),
            'source':      'yfinance',
        }
    except Exception:
        return None


if __name__ == '__main__':
    # 自检: python us_fundamentals.py NVDA
    import sys
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    tk = sys.argv[1] if len(sys.argv) > 1 else 'NVDA'
    data = fetch_all(tk, use_cache=False)
    print(json.dumps(data, ensure_ascii=False, indent=2)[:2500])
    close_futu()
