# coding: utf-8
"""
US Undervalued-Quality Screener — 低估优质股扫描器

实现投资研究笔记 §二 的筛选框架(docs/investment_research_20260613.md):

    市值 > $1B           (流动性门槛)
    近一年跌幅 > 30%      (可能超跌 — 好公司被错杀的入口)
    ROE 近3年年报均值 > 12%  (基本质地, 过滤周期单年波动)

目的不是给买入信号, 而是产出一份"超跌但底子好"的候选清单, 供人工按笔记
§二核心三问复核: ①它为什么便宜?(暂时性 vs 结构性) ②三年后还在吗?
③同样的钱买 QQQ 哪个更好?

实证补充(t_us_bottom_entry_backtest.py / docs §7.5-7.6, 2026-06): 本清单的
"超跌+优质"正是好入场状态, 但用法有三条钉死的纪律 ——
  · 质量(ROE门槛)的作用是【防归零/左尾保护】, 不是收益放大器: 优质组反弹
    幅度甚至略输垃圾组, 但深跌>40%率约为其一半。别期待它抬高反弹, 指望它砍尾。
  · alpha 靠右尾少数大复苏, 中位仅打平 QQQ → 这份清单要【分散持多只 + 拿得住】,
    单押一只 ≈ 打平 QQQ。
  · 入场越早越好(跌破20周线的底部就分批建), 别等"放量"或"站回均线"确认 ——
    机械放量信号不贡献 EV, 站回入场反而更差(便宜段已走完)。

数据源: yfinance only (与 ADR-0001 一致, 行情/基本面同源)。
  - 股票池 = S&P 500 ∪ Nasdaq-100 成分股 (Wikipedia, 当日缓存)。
    超跌的优质大中盘正是茅台/MU 式"好公司被杀估值"的猎场; 微盘超跌是噪音, 不在范围。
  - 计算顺序按成本递增, 贵的那步只跑幸存者:
      1) 收盘价优先复用 t_us_tech_swing 的共享缓存(每日 cron 已热身), 缺
         的才批量 yf.download 补拉 → 算跌幅 (全体, 便宜, 见 bulk_drops)
      2) fast_info 市值 → 只对已跌>30% 的票 (较快)
      3) annual financials 算 3 年 ROE 均值 → 只对市值幸存者 (慢)

输出: /home/ryan/DATA/result/us_undervalue_<date>.csv  + 终端报告。

Usage:
  python t_us_undervalue.py                      # 默认 SP500∪NDX
  python t_us_undervalue.py --universe sp500      # 仅 S&P 500
  python t_us_undervalue.py --tickers MU,INTC,PYPL  # 只扫指定票 (跳过宽源)
  python t_us_undervalue.py --min-drop 25 --min-roe 10 --min-cap 2  # 调阈值
  python t_us_undervalue.py --force               # 忽略当日缓存重拉股票池
"""

import os
import sys
import json
import logging
import datetime
import warnings
from io import StringIO
from optparse import OptionParser

warnings.filterwarnings('ignore')

import pandas as pd
import tabulate as tab_mod

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('undervalue')

RESULT_DIR = '/home/ryan/DATA/result'
CACHE_DIR  = '/home/ryan/DATA/DAY_Global/US_universe'   # 股票池清单当日缓存
UA = {'User-Agent': 'Mozilla/5.0'}

# ── 默认阈值 (笔记 §二) ──────────────────────────────────────────────────────
MIN_MKTCAP   = 1e9     # 市值 > $1B
MIN_DROP_1Y  = 30.0    # 近一年跌幅 > 30%  (即 trailing-1y return <= -30%)
MIN_ROE_AVG  = 12.0    # ROE 近3年年报均值 > 12%
ROE_YEARS    = 3
INTERVAL_5Y  = 6       # get_valuation_detail interval_type: 6=近5年 (取整个周期的估值分位)

# midht 中盘切片市值带: 下限接住 R2000 顶部, 上限盖过 SP500 准入线(~$18B)留余量
MIDHT_CAP_LO = 2e9
MIDHT_CAP_HI = 30e9


# ── 股票池 (Wikipedia, 当日缓存) ─────────────────────────────────────────────
def _wiki_table(url: str, symbol_col_candidates) -> list:
    import urllib.request
    req = urllib.request.Request(url, headers=UA)
    html = urllib.request.urlopen(req, timeout=25).read().decode('utf-8', 'ignore')
    for tbl in pd.read_html(StringIO(html)):
        col = next((c for c in symbol_col_candidates if c in tbl.columns), None)
        if col is not None:
            # BRK.B → BRK-B (yfinance 用连字符)
            return (tbl[col].astype(str).str.upper()
                    .str.replace('.', '-', regex=False).str.strip().tolist())
    raise ValueError(f'no symbol column in {url}')


def _fetch_sp500() -> list:
    return _wiki_table('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
                       ['Symbol', 'Ticker symbol', 'Ticker'])


def _fetch_ndx() -> list:
    # 2026-07 起成分表从 Nasdaq-100 主条目挪到了独立列表页; 两个 URL 都试
    for url in ('https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies',
                'https://en.wikipedia.org/wiki/Nasdaq-100'):
        try:
            return _wiki_table(url, ['Ticker', 'Symbol'])
        except Exception:
            continue
    raise ValueError('NDX constituents table not found on Wikipedia')


def _fetch_r2000_healthtech() -> list:
    """Russell 2000 中 Health Care + Technology 成分。

    成分清单: Vanguard VTWO (Russell 2000 ETF) 持仓 API, 500/页分页 (~1940 只,
    Wikipedia 没有 R2000 成分表; iShares IWM 的 CSV 下载端点 2026 改版后已失效)。
    行业口径: 纳斯达克官方 screener API 一次请求全交易所 (与 ndx_predictor 同源),
    sector ∈ {Health Care, Technology}。少数 VTWO 持仓在 screener 里查不到
    (~25 只, 多为多股类别符号) — 直接丢弃, 不影响池子成色。
    """
    import urllib.request
    hdr = dict(UA, Accept='application/json')
    tickers, start = [], 1
    while True:
        url = ('https://investor.vanguard.com/investment-products/etfs/profile/api/'
               f'VTWO/portfolio-holding/stock?start={start}&count=500')
        req = urllib.request.Request(url, headers=hdr)
        d = json.loads(urllib.request.urlopen(req, timeout=30).read())
        ents = d.get('fund', {}).get('entity', [])
        tickers += [(e.get('ticker') or '').strip().upper() for e in ents]
        if len(ents) < 500:
            break
        start += 500
    if not tickers:
        raise ValueError('VTWO holdings API returned no tickers')
    req = urllib.request.Request(
        'https://api.nasdaq.com/api/screener/stocks?download=true', headers=hdr)
    rows = json.loads(urllib.request.urlopen(req, timeout=60).read())['data']['rows']
    sector = {r['symbol'].strip().upper(): r['sector'] for r in rows}
    keep = {'Health Care', 'Technology'}
    # BRK.B → BRK-B (yfinance 用连字符, 与 _wiki_table 同约定)
    return [t.replace('.', '-') for t in tickers if sector.get(t) in keep]


def _fetch_midcap_healthtech() -> list:
    """中盘 Health Care + Technology 切片 — 补 SP500/NDX/R2000 三块拼图的缝。

    SP500 有市值门槛+注册地规则, NDX 只收 Nasdaq-100, R2000 是小盘 — 市值落在
    R2000 上限与 SP500 门槛之间的中盘科技/医疗名是三者共同的盲区 (2026-07 FROG
    $11B 财报强反应缺席暴露)。S&P MidCap 400 也补不上: 同样的注册地规则,
    FROG(以色列) 就不在其中。
    口径: 纳斯达克官方 screener 一次请求全交易所 (与 r2000ht 的行业口径同源),
    sector ∈ {Health Care, Technology} 且市值 $2B–$30B。与其他池的重叠在
    load_universe 的 set-union 里自然去重。
    """
    import urllib.request
    hdr = dict(UA, Accept='application/json')
    req = urllib.request.Request(
        'https://api.nasdaq.com/api/screener/stocks?download=true', headers=hdr)
    rows = json.loads(urllib.request.urlopen(req, timeout=60).read())['data']['rows']
    keep = {'Health Care', 'Technology'}
    out = []
    for r in rows:
        try:
            cap = float(r.get('marketCap') or 0)
        except (TypeError, ValueError):
            continue
        if r.get('sector') in keep and MIDHT_CAP_LO <= cap <= MIDHT_CAP_HI:
            out.append(r['symbol'].strip().upper().replace('.', '-'))
    if not out:
        raise ValueError('Nasdaq screener returned no mid-cap HC/Tech names')
    return out


def load_universe(which: str, force: bool) -> list:
    """which ∈ {sp500, ndx, both, r2000ht, midht, all}. 当日缓存到 CACHE_DIR/<which>.json。

    both = SP500 ∪ NDX-100 (质量池, huice 的 α 结论都在这个口径上);
    r2000ht = Russell 2000 的 Health Care + Technology 切片 (~590 只小盘);
    midht = 中盘 $2B–$30B 的 Health Care + Technology 切片 (screener, ~400 只);
    all = both ∪ r2000ht ∪ midht。
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f'{which}.json')
    if not force and os.path.exists(path):
        mtime = datetime.date.fromtimestamp(os.path.getmtime(path))
        if mtime == datetime.date.today():
            try:
                with open(path) as fh:
                    syms = json.load(fh)
                log.info(f'股票池 {which}: {len(syms)} 只 (当日缓存)')
                return syms
            except Exception:
                pass
    parts = []
    try:
        if which in ('sp500', 'both', 'all'):
            parts += _fetch_sp500()
        if which in ('ndx', 'both', 'all'):
            parts += _fetch_ndx()
        if which in ('r2000ht', 'all'):
            parts += _fetch_r2000_healthtech()
        if which in ('midht', 'all'):
            parts += _fetch_midcap_healthtech()
    except Exception as e:
        log.error(f'股票池抓取失败 ({e})')
        if os.path.exists(path):                     # 陈旧缓存兜底 (ADR-0001 stale-cache 思路)
            with open(path) as fh:
                syms = json.load(fh)
            log.warning(f'降级使用陈旧缓存 {which}: {len(syms)} 只')
            return syms
        return []
    syms = sorted(set(s for s in parts if s and s.isascii() and 1 <= len(s) <= 6))
    try:
        with open(path, 'w') as fh:
            json.dump(syms, fh)
    except Exception:
        pass
    log.info(f'股票池 {which}: {len(syms)} 只 (新抓取)')
    return syms


# ── 计算层 ───────────────────────────────────────────────────────────────────
def bulk_drops(tickers: list) -> dict:
    """1 年收盘 → {ticker: {price, ret_1y, dist_low, dist_high}}。

    Cache-first (t_us_tech_swing's shared per-ticker disk cache, ADR-0001): the
    daily cron already warms this cache for the SP500∪NDX universe every weekday
    (gap_scan/pullback_shock/bottom_entry/key_kline --scan), so by Sunday almost
    every ticker is a cache hit and costs zero network calls. Only misses (new
    listings, or a cold cache) fall back to one batched yf.download — this used
    to be an unconditional full-universe download every single week regardless
    of what the daily runs had already fetched."""
    import t_us_tech_swing as sw

    out = {}
    misses = []
    for t in tickers:
        df = sw._fetch_daily(t)
        if df.empty:
            misses.append(t)
        else:
            out[t] = df['close'].tail(252)

    if misses:
        import yfinance as yf
        log.info(f'{len(misses)}/{len(tickers)} 只无缓存 — 批量补拉 1 年收盘 ...')
        data = yf.download(misses, period='1y', auto_adjust=True,
                           progress=False, group_by='ticker', threads=True)
        multi = len(misses) > 1
        for t in misses:
            try:
                close = data[t]['Close'].dropna() if multi else data['Close'].dropna()
            except Exception:
                continue
            if not close.empty:
                out[t] = close

    result = {}
    for t, close in out.items():
        if len(close) < 60:                          # 上市不足/数据太少, 跳过
            continue
        first, last = float(close.iloc[0]), float(close.iloc[-1])
        hi, lo = float(close.max()), float(close.min())
        if first <= 0:
            continue
        result[t] = {
            'price':     last,
            'ret_1y':    (last / first - 1) * 100,           # 负=下跌
            'dist_low':  (last / lo - 1) * 100 if lo > 0 else None,   # 高于52周低点%
            'dist_high': (last / hi - 1) * 100 if hi > 0 else None,   # 距52周高点%(负)
        }
    log.info(f'有效价格数据 {len(result)} 只')
    return result


def market_cap(ticker) -> float | None:
    """fast_info 优先 (快), 退回 info。"""
    try:
        fi = ticker.fast_info
        mc = fi.get('market_cap') if hasattr(fi, 'get') else getattr(fi, 'market_cap', None)
        if mc:
            return float(mc)
    except Exception:
        pass
    try:
        return float((ticker.info or {}).get('marketCap') or 0) or None
    except Exception:
        return None


def roe_history(ticker, years: int) -> dict:
    """近 N 年年报 ROE = 净利润 / 股东权益。返回 {avg, ttm, per_year, n}。"""
    out = {'avg': None, 'ttm': None, 'per_year': [], 'n': 0}
    try:
        info = ticker.info or {}
        if info.get('returnOnEquity') is not None:
            out['ttm'] = float(info['returnOnEquity']) * 100
    except Exception:
        pass
    try:
        fin, bs = ticker.financials, ticker.balance_sheet
        if fin is None or bs is None or fin.empty or bs.empty:
            return out

        def _row(df, *names):
            for n in names:
                m = [i for i in df.index if n.lower() in str(i).lower()]
                if m:
                    return df.loc[m[0]]
            return None

        ni = _row(fin, 'Net Income')
        eq = _row(bs, 'Stockholders Equity', 'Common Stock Equity', 'Total Equity Gross')
        if ni is None or eq is None:
            return out
        roes = []
        for c in fin.columns:                        # 新→旧
            n = ni.get(c)
            e = eq.get(c) if c in eq.index else None
            if n is not None and e and e != 0 and pd.notna(n) and pd.notna(e):
                roes.append(round(n / e * 100, 1))
            if len(roes) >= years:
                break
        out['per_year'] = roes
        out['n'] = len(roes)
        if roes:
            out['avg'] = round(sum(roes) / len(roes), 1)
    except Exception as e:
        log.debug(f'roe_history failed ({e})')
    return out


# ── Futu 富化 (只对幸存者; OpenD 不可达 → 全 None, 不挡主流程) ────────────────
# yfinance 给不了的三样, 正好回答笔记 §二"真便宜吗 / 还在吗":
#   PE/PB 历史分位 — 低分位才是真便宜 (raw 单位已是 0..100)
#   晨星星级       — 1=高估 … 5=低估
#   晨星公允价值   — price/fair_value < 1 = 低于公允价值
def open_futu():
    """复用 us_fundamentals 的惰性 OpenQuoteContext (含端口探测/降级)。"""
    try:
        import us_fundamentals as uf
        return uf, uf._futu()
    except Exception as e:
        log.warning(f'Futu 富化不可用 ({e}) — 仅 yfinance 列')
        return None, None


def futu_enrich(ctx, ticker: str) -> dict:
    out = {'pe_pctile': None, 'pb_pctile': None, 'ms_star': None, 'ms_fv': None}
    if ctx is None:
        return out
    try:
        from futu import RET_OK
    except Exception:
        return out
    code = f'US.{ticker}'
    for vt, key in ((1, 'pe_pctile'), (2, 'pb_pctile')):
        try:
            ret, d = ctx.get_valuation_detail(code, valuation_type=vt, interval_type=INTERVAL_5Y)
            if ret == RET_OK and isinstance(d, dict):
                p = (d.get('trend') or {}).get('valuation_percentile')
                if p is not None:
                    out[key] = round(float(p))
        except Exception:
            pass
    try:
        ret, d = ctx.get_research_morningstar_report(code)
        if ret == RET_OK and isinstance(d, dict):
            sr, fv = d.get('star_rating'), d.get('fair_value')
            out['ms_star'] = int(sr) if sr else None
            out['ms_fv']   = round(float(fv), 1) if fv else None
    except Exception:
        pass
    return out


# ── 主流程 ───────────────────────────────────────────────────────────────────
def screen(tickers, min_cap, min_drop, min_roe, roe_years, use_futu=True):
    import yfinance as yf

    drops = bulk_drops(tickers)
    # 1) 跌幅过滤 (免费, 全体)
    dropped = {t: d for t, d in drops.items() if d['ret_1y'] <= -min_drop}
    log.info(f'跌幅≥{min_drop:.0f}%: {len(dropped)} 只')
    if not dropped:
        return []

    uf, fctx = (open_futu() if use_futu else (None, None))

    rows = []
    for i, (t, d) in enumerate(sorted(dropped.items(), key=lambda kv: kv[1]['ret_1y']), 1):
        tk = yf.Ticker(t)
        # 2) 市值过滤 (只对已跌的票)
        cap = market_cap(tk)
        if cap is None or cap < min_cap:
            continue
        # 3) ROE 过滤 (只对市值幸存者 — 最慢的一步)
        roe = roe_history(tk, roe_years)
        flags = []
        if roe['n'] < roe_years:
            flags.append(f'roe_only_{roe["n"]}y')
        if roe['avg'] is None or roe['avg'] < min_roe:
            continue
        # PE / sector (尽力而为, 失败不挡)
        pe = sector = None
        try:
            info = tk.info or {}
            pe = info.get('trailingPE')
            sector = info.get('sector')
        except Exception:
            pass
        if roe['avg'] is not None and roe['avg'] > 80:
            # ROE 畸高几乎都是回购把股东权益打到很小/为负所致, 不是真质地。
            # 笔记 §二 要的是经营质地, 这类应人工警惕 (看 ROIC/净利率更可靠)。
            flags.append('ROE畸高(权益被回购侵蚀?)')
        elif roe['ttm'] is not None and roe['avg'] is not None and roe['ttm'] > roe['avg'] * 2:
            flags.append('ttm>>avg(或处周期高)')      # 提示: 当前 ROE 远高于历史均值

        # Futu 富化 — 判断"真便宜还是只是跌了"
        fz = futu_enrich(fctx, t)
        px2fv = round(d['price'] / fz['ms_fv'], 2) if fz['ms_fv'] else None
        have_futu = any(v is not None for v in (fz['pe_pctile'], fz['ms_star'], px2fv))
        if fz['pe_pctile'] is not None and fz['pe_pctile'] >= 70:
            flags.append('PE分位高')
        if px2fv is not None and px2fv > 1:
            flags.append('高于公允价值')
        if fz['ms_star'] is not None and fz['ms_star'] <= 2:
            flags.append('晨星高估')
        # 真低估 = 有 Futu 数据佐证, 且无任一"其实不便宜"的反向信号
        genuine = (have_futu
                   and (fz['pe_pctile'] is None or fz['pe_pctile'] <= 50)
                   and (px2fv is None or px2fv <= 1.0)
                   and (fz['ms_star'] is None or fz['ms_star'] >= 3))

        rows.append({
            'ticker':        t,
            'sector':        sector or '',
            'price':         round(d['price'], 2),
            'mktcap_b':      round(cap / 1e9, 1),
            'ret_1y_pct':    round(d['ret_1y'], 1),
            'dist_low_pct':  round(d['dist_low'], 1) if d['dist_low'] is not None else None,
            'dist_high_pct': round(d['dist_high'], 1) if d['dist_high'] is not None else None,
            'roe_avg3_pct':  roe['avg'],
            'roe_ttm_pct':   round(roe['ttm'], 1) if roe['ttm'] is not None else None,
            'roe_years':     '/'.join(str(x) for x in roe['per_year']),
            'pe':            round(pe, 1) if isinstance(pe, (int, float)) else None,
            'pe_pctile':     fz['pe_pctile'],
            'pb_pctile':     fz['pb_pctile'],
            'ms_star':       fz['ms_star'],
            'fair_value':    fz['ms_fv'],
            'px_to_fv':      px2fv,
            'genuine':       genuine,
            'flags':         ' '.join(flags),
        })
        gmark = ' ◎真低估' if genuine else ''
        log.info(f'  ✓ {t}  跌{d["ret_1y"]:.0f}%  cap${cap/1e9:.0f}B  ROE3y{roe["avg"]}%'
                 f'  PE分位{fz["pe_pctile"]}  ★{fz["ms_star"]}  P/FV{px2fv}{gmark}')

    if uf is not None:
        try:
            uf.close_futu()
        except Exception:
            pass

    # 排名: 真低估优先 → 质地 (ROE 高) → 同档比谁更超跌
    rows.sort(key=lambda r: (not r['genuine'], -r['roe_avg3_pct'], r['ret_1y_pct']))
    return rows


def render(date_str, rows, params):
    out = []

    def p(*a):
        line = ' '.join(str(x) for x in a)
        out.append(line); print(line)

    p()
    p('=' * 84)
    p(f'  US 低估优质股扫描  —  {date_str}   (笔记 §二: 超跌×优质质地)')
    p('=' * 84)
    p(f'  阈值: 市值≥${params["min_cap"]/1e9:.0f}B · 近1年跌幅≥{params["min_drop"]:.0f}%'
      f' · ROE近{params["roe_years"]}年均值≥{params["min_roe"]:.0f}%')
    p(f'  股票池: {params["universe_desc"]}  |  命中 {len(rows)} 只')
    p()
    if rows:
        n_genuine = sum(1 for r in rows if r['genuine'])
        table = tab_mod.tabulate(
            [[('◎ ' if r['genuine'] else '') + r['ticker'], r['sector'][:12], r['price'],
              r['mktcap_b'], r['ret_1y_pct'], r['roe_avg3_pct'],
              r['pe_pctile'], r['ms_star'], r['px_to_fv'], r['flags']] for r in rows],
            headers=['Ticker', 'Sector', 'Price', 'Cap$B', '1y%', 'ROE3y',
                     'PE分位', '晨星★', 'P/FV', 'flags'],
            tablefmt='simple', floatfmt='.1f')
        p(table)
        p()
        p(f'  ◎真低估 {n_genuine} 只: Futu 佐证(PE分位≤50 且不高于公允价值 且晨星★≥3) — 真便宜, 非单纯下跌。')
        p('  ⚠ 候选清单, 非买入信号。逐只问笔记 §二核心三问:')
        p('     ① 它为什么便宜?(暂时性 vs 结构性)  ② 三年后还在/更强吗?  ③ 买 QQQ 哪个更好?')
        p('  列义: PE分位=PE在自身近5年区间百分位(低=真便宜) · 晨星★(1高估…5低估) · P/FV=价格/晨星公允价值(>1偏贵)')
        p('  flags: PE分位高/高于公允价值/晨星高估 = "看着跌了其实不便宜"(MU 式周期陷阱); ROE畸高=权益被回购侵蚀。')
    else:
        p('  本期无命中。可放宽 --min-drop / --min-roe / --min-cap 再试。')
    p()
    return '\n'.join(out)


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--universe', default='both', help='sp500 | ndx | both (默认 both)')
    parser.add_option('--tickers', default=None, help='逗号分隔, 指定则跳过宽源')
    parser.add_option('--min-cap',  type='float', default=MIN_MKTCAP / 1e9, help='最小市值($B), 默认 1')
    parser.add_option('--min-drop', type='float', default=MIN_DROP_1Y, help='最小1年跌幅(%%), 默认 30')
    parser.add_option('--min-roe',  type='float', default=MIN_ROE_AVG, help='最小ROE均值(%%), 默认 12')
    parser.add_option('--roe-years', type='int', default=ROE_YEARS, help='ROE 平均年数, 默认 3')
    parser.add_option('--force', action='store_true', help='忽略当日股票池缓存')
    parser.add_option('--no-futu', action='store_true', help='跳过 Futu 富化(仅 yfinance 列)')
    parser.add_option('--output', default=None, help='报告输出路径')
    opts, _ = parser.parse_args()

    date_str = datetime.datetime.now().strftime('%Y%m%d')

    if opts.tickers:
        tickers = sorted(set(t.strip().upper() for t in opts.tickers.split(',') if t.strip()))
        universe_desc = f'指定 {len(tickers)} 只'
    else:
        tickers = load_universe(opts.universe, opts.force)
        universe_desc = f'{opts.universe} ({len(tickers)} 只)'
    if not tickers:
        log.error('股票池为空, 退出。')
        sys.exit(1)

    params = {'min_cap': opts.min_cap * 1e9, 'min_drop': opts.min_drop,
              'min_roe': opts.min_roe, 'roe_years': opts.roe_years,
              'universe_desc': universe_desc}

    rows = screen(tickers, params['min_cap'], params['min_drop'],
                  params['min_roe'], params['roe_years'], use_futu=not opts.no_futu)

    out_dir = os.path.join(RESULT_DIR, 'us_undervalue')
    if os.path.isdir(RESULT_DIR):
        os.makedirs(out_dir, exist_ok=True)

    # CSV
    if rows and os.path.isdir(RESULT_DIR):
        csv_path = os.path.join(out_dir, f'us_undervalue_{date_str}.csv')
        pd.DataFrame(rows).to_csv(csv_path, index=False)
        log.info(f'候选清单 → {csv_path}')

    text = render(date_str, rows, params)
    out_file = opts.output or (os.path.join(out_dir, f'us_undervalue_{date_str}.txt')
                               if os.path.isdir(RESULT_DIR) else None)
    if out_file:
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(text + '\n')
        log.info(f'报告 → {out_file}')


if __name__ == '__main__':
    main()
