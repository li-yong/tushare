# coding: utf-8
"""
US Cashflow-Quality Screener — 现金流质地白/黑名单

基于经营现金流(OCF)与自由现金流(FCF)把股票池分成三档:

  白名单 (现金流优质):
    近 N 财年(默认3) OCF 每年 > 0  且  FCF 每年 > 0
    TTM OCF > 0 且 TTM FCF > 0        (最新没有恶化)
    现金含量: 最近财年净利润>0 时, OCF/净利润 ≥ 0.8   (利润有现金背书, 非白条)
    → 按 FCF yield (TTM FCF / 市值) 降序排名

  黑名单 (现金流有问题, 任一命中, reason 用 constant.py 字符串):
    NAG_CASHFLOW              最近财年 OCF < 0
    OCF_TTM_TURNED_NEG        年报 OCF 为正但 TTM 已转负 (刚恶化)
    NAG_FCF_MOST_YEARS        近3财年 FCF ≥2 年为负 (烧钱是常态)
    N_CASHFLOW_ACT_LT_PROFIT  净利润>0 但 OCF < 0.5×净利润 (白条利润, 镜像
                              finlib._remove_garbage_n_cashflow_act_less_profit)

  灰名单: 两头都不沾 (数据不足 / 介于之间), 只进 CSV 不上报告。
  口径不适用: 金融/地产 (银行保险的 OCF 由放贷节奏驱动, REIT 折旧口径特殊),
    不做黑白判断, 单列。

数据源: yfinance (ADR-0001 行情同源; Futu 无需参与)。每票三次调用:
  - Ticker.info               → marketCap / totalRevenue / netIncomeToCommon /
                                sector / financialCurrency
  - Ticker.cashflow           → 近 N 财年 Operating Cash Flow / Free Cash Flow /
                                Net Income (FCF 行缺失时用 OCF + CapEx 兜底)
  - Ticker.quarterly_cashflow → TTM = 最近4季求和 (info 的 freeCashflow 是
                                levered 口径且实测常大于自家 OCF, 只作兜底)
ADR 财报币种≠USD (PDD 之类) → FCF yield 不算 (分子分母币种不匹配), 打 fx 标记;
margin/现金含量/黑白判定是同币种比值或符号判断, 不受影响。
财报季度才变 → 每票 JSON 缓存 7 天 (US_cashflow/), --refresh 强拉。

输出 (per-script subfolder 约定):
  /home/ryan/DATA/result/us_cashflow_quality/us_cashflow_quality_<date>.csv  全池明细
  /home/ryan/DATA/result/us_cashflow_quality/us_cashflow_quality_<date>.txt  白/黑名单报告

Usage:
  python t_us_cashflow_quality.py                          # 默认 SP500∪NDX
  python t_us_cashflow_quality.py --universe all           # 加 r2000ht+midht 大池
  python t_us_cashflow_quality.py --tickers NVDA,BA,SNAP   # 指定票
  python t_us_cashflow_quality.py --min-cap 2 --years 4    # 调阈值
  python t_us_cashflow_quality.py --refresh                # 忽略每票缓存重拉
"""

import os
import sys
import json
import logging
import datetime
import warnings
from optparse import OptionParser
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

import pandas as pd
import tabulate as tab_mod

import constant
from t_us_undervalue import load_universe, RESULT_DIR

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('cashflow')

CACHE_DIR = '/home/ryan/DATA/DAY_Global/US_cashflow'   # 每票现金流 JSON
CACHE_TTL_DAYS = 7                                     # 财报季度才变, 7天足够新
CACHE_VER = 2                                          # 字段结构变更时 +1 使旧缓存失效

# ── 默认阈值 ─────────────────────────────────────────────────────────────────
YEARS       = 3      # 追溯财年数
MIN_MKTCAP  = 1e9    # 市值门槛 $1B (池外小票噪音多)
MIN_OCF_NI  = 0.8    # 白名单现金含量: 最近财年 OCF/净利润 下限
BAITIAO_OCF_NI = 0.5 # 黑名单白条线 (镜像 finlib 0.5×profit 规则)

# 金融的 OCF 由放贷/存款节奏驱动, 地产(REIT)折旧口径特殊 — 黑白判断不适用
NA_SECTORS = {'Financial Services', 'Real Estate'}


# ── 取数层 (cache-first) ─────────────────────────────────────────────────────
def _cache_path(ticker: str) -> str:
    return os.path.join(CACHE_DIR, f'{ticker}.json')


def _cache_fresh(path: str) -> bool:
    if not os.path.exists(path):
        return False
    age = datetime.date.today() - datetime.date.fromtimestamp(os.path.getmtime(path))
    return age.days < CACHE_TTL_DAYS


def _stmt_row(df, *names):
    """cashflow 报表按行名模糊取一行 (完全匹配优先, 其次包含)。"""
    idx = [str(i) for i in df.index]
    for n in names:
        for i, s in enumerate(idx):
            if s.lower() == n.lower():
                return df.iloc[i]
    for n in names:
        for i, s in enumerate(idx):
            if n.lower() in s.lower():
                return df.iloc[i]
    return None


def fetch_cashflow(ticker: str, years: int, refresh: bool = False) -> dict:
    """一只票的现金流画像 (缓存 7 天)。任何缺项 → None, 不抛。

    返回: {ticker, sector, cap, revenue_ttm, ni_ttm, ocf_ttm, fcf_ttm,
           ocf_years, fcf_years, ni_years (新→旧, 长度≤years), flags}
    """
    path = _cache_path(ticker)
    if not refresh and _cache_fresh(path):
        try:
            with open(path) as fh:
                cached = json.load(fh)
            if cached.get('v') == CACHE_VER:
                return cached
        except Exception:
            pass

    import yfinance as yf
    out = {'v': CACHE_VER, 'ticker': ticker, 'sector': None, 'cap': None,
           'fin_currency': None, 'revenue_ttm': None, 'ni_ttm': None,
           'ocf_ttm': None, 'fcf_ttm': None,
           'ocf_years': [], 'fcf_years': [], 'ni_years': [],
           'flags': []}
    tk = yf.Ticker(ticker)

    info_ocf = info_fcf = None
    try:
        info = tk.info or {}
        out['sector']       = info.get('sector')
        out['cap']          = info.get('marketCap')
        out['fin_currency'] = info.get('financialCurrency')
        out['revenue_ttm']  = info.get('totalRevenue')
        out['ni_ttm']       = info.get('netIncomeToCommon')
        info_ocf = info.get('operatingCashflow')
        info_fcf = info.get('freeCashflow')
    except Exception as e:
        out['flags'].append('noinfo')
        log.debug(f'{ticker}: info failed ({e})')

    try:
        cf = tk.cashflow
        if cf is not None and not cf.empty:
            ocf = _stmt_row(cf, 'Operating Cash Flow',
                            'Cash Flow From Continuing Operating Activities')
            fcf = _stmt_row(cf, 'Free Cash Flow')
            capex = _stmt_row(cf, 'Capital Expenditure')
            ni = _stmt_row(cf, 'Net Income From Continuing Operations', 'Net Income')
            for c in list(cf.columns)[:years]:          # 新→旧
                o = ocf.get(c) if ocf is not None else None
                f = fcf.get(c) if fcf is not None else None
                if f is None or pd.isna(f):             # FCF 行缺 → OCF+CapEx 兜底
                    cx = capex.get(c) if capex is not None else None
                    if o is not None and cx is not None and pd.notna(o) and pd.notna(cx):
                        f = o + cx                      # capex 本身为负
                n = ni.get(c) if ni is not None else None
                out['ocf_years'].append(float(o) if o is not None and pd.notna(o) else None)
                out['fcf_years'].append(float(f) if f is not None and pd.notna(f) else None)
                out['ni_years'].append(float(n) if n is not None and pd.notna(n) else None)
        else:
            out['flags'].append('nostmt')
    except Exception as e:
        out['flags'].append('nostmt')
        log.debug(f'{ticker}: cashflow stmt failed ({e})')

    # TTM: 最近4季求和, 与年报同口径; 不足4季或取数失败 → 退回 info 值
    try:
        qcf = tk.quarterly_cashflow
        if qcf is not None and not qcf.empty and len(qcf.columns) >= 4:
            ocf = _stmt_row(qcf, 'Operating Cash Flow',
                            'Cash Flow From Continuing Operating Activities')
            fcf = _stmt_row(qcf, 'Free Cash Flow')
            capex = _stmt_row(qcf, 'Capital Expenditure')
            o_q, f_q = [], []
            for c in list(qcf.columns)[:4]:             # 新→旧
                o = ocf.get(c) if ocf is not None else None
                f = fcf.get(c) if fcf is not None else None
                if f is None or pd.isna(f):
                    cx = capex.get(c) if capex is not None else None
                    if o is not None and cx is not None and pd.notna(o) and pd.notna(cx):
                        f = o + cx
                if o is not None and pd.notna(o):
                    o_q.append(float(o))
                if f is not None and pd.notna(f):
                    f_q.append(float(f))
            if len(o_q) == 4:
                out['ocf_ttm'] = sum(o_q)
            if len(f_q) == 4:
                out['fcf_ttm'] = sum(f_q)
    except Exception as e:
        log.debug(f'{ticker}: quarterly cashflow failed ({e})')
    if out['ocf_ttm'] is None and info_ocf is not None:
        out['ocf_ttm'] = info_ocf
        out['flags'].append('ttm_from_info')
    if out['fcf_ttm'] is None and info_fcf is not None:
        out['fcf_ttm'] = info_fcf
        if 'ttm_from_info' not in out['flags']:
            out['flags'].append('ttm_from_info')

    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(path, 'w') as fh:
            json.dump(out, fh)
    except Exception:
        pass
    return out


# ── 判定层 ───────────────────────────────────────────────────────────────────
def classify(m: dict, years: int, min_ocf_ni: float) -> tuple:
    """→ (类别, reasons)。类别 ∈ {'WHITE','BLACK','GREY','NA'}。"""
    if m['sector'] in NA_SECTORS:
        return 'NA', ['金融/地产 OCF 口径不适用']

    ocf_y, fcf_y, ni_y = m['ocf_years'], m['fcf_years'], m['ni_years']
    ocf_latest = ocf_y[0] if ocf_y else None
    ni_latest  = ni_y[0] if ni_y else None
    if not ocf_y and m['ocf_ttm'] is None:
        return 'GREY', ['无现金流数据']

    # ── 黑名单: 任一命中 ──
    reasons = []
    if ocf_latest is not None and ocf_latest < 0:
        reasons.append(constant.NAG_CASHFLOW)
    elif m['ocf_ttm'] is not None and m['ocf_ttm'] < 0:
        reasons.append(constant.OCF_TTM_TURNED_NEG)
    fcf_known = [f for f in fcf_y if f is not None]
    if len(fcf_known) >= 2 and sum(1 for f in fcf_known if f < 0) >= 2:
        reasons.append(constant.NAG_FCF_MOST_YEARS)
    if (ni_latest is not None and ni_latest > 0
            and ocf_latest is not None and ocf_latest < BAITIAO_OCF_NI * ni_latest):
        reasons.append(constant.N_CASHFLOW_ACT_LT_PROFIT)
    if reasons:
        return 'BLACK', reasons

    # ── 白名单: 全部满足 ──
    ocf_known = [o for o in ocf_y if o is not None]
    full_history = len(ocf_known) >= years and len(fcf_known) >= years
    all_pos = (full_history
               and all(o > 0 for o in ocf_known)
               and all(f > 0 for f in fcf_known))
    ttm_ok = ((m['ocf_ttm'] is None or m['ocf_ttm'] > 0)
              and (m['fcf_ttm'] is None or m['fcf_ttm'] > 0))
    accrual_ok = True
    if ni_latest is not None and ni_latest > 0 and ocf_latest is not None:
        accrual_ok = ocf_latest >= min_ocf_ni * ni_latest
    if all_pos and ttm_ok and accrual_ok:
        notes = []
        if ni_latest is not None and ni_latest <= 0:
            notes.append('净利润为负但现金流全正')
        return 'WHITE', notes
    return 'GREY', []


def build_row(m: dict, cls: str, reasons: list) -> dict:
    cap = m['cap']
    flags = list(m['flags'])
    cur = m.get('fin_currency')
    fx_mismatch = cur is not None and cur != 'USD'
    if fx_mismatch:
        flags.append(f'fx:{cur}')      # 财报币种≠USD → 不与美元市值做比值
    fcf_yield = (m['fcf_ttm'] / cap * 100) \
        if (m['fcf_ttm'] and cap and not fx_mismatch) else None
    fcf_margin = (m['fcf_ttm'] / m['revenue_ttm'] * 100) \
        if (m['fcf_ttm'] and m['revenue_ttm']) else None
    ocf_ni_latest = None
    if (m['ni_years'] and m['ocf_years'] and m['ni_years'][0]
            and m['ocf_years'][0] is not None and m['ni_years'][0] > 0):
        ocf_ni_latest = round(m['ocf_years'][0] / m['ni_years'][0], 2)
    ocf_ni_ttm = None
    if m['ni_ttm'] and m['ni_ttm'] > 0 and m['ocf_ttm'] is not None:
        ocf_ni_ttm = round(m['ocf_ttm'] / m['ni_ttm'], 2)

    def _b(vals):     # $B 串, 新→旧
        return '/'.join('-' if v is None else f'{v/1e9:.1f}' for v in vals)

    return {
        'ticker':         m['ticker'],
        'class':          cls,
        'sector':         m['sector'] or '',
        'mktcap_b':       round(cap / 1e9, 1) if cap else None,
        'ocf_ttm_b':      round(m['ocf_ttm'] / 1e9, 2) if m['ocf_ttm'] is not None else None,
        'fcf_ttm_b':      round(m['fcf_ttm'] / 1e9, 2) if m['fcf_ttm'] is not None else None,
        'fcf_yield_pct':  round(fcf_yield, 1) if fcf_yield is not None else None,
        'fcf_margin_pct': round(fcf_margin, 1) if fcf_margin is not None else None,
        'ocf_ni':         ocf_ni_latest,
        'ocf_ni_ttm':     ocf_ni_ttm,
        'ocf_years_b':    _b(m['ocf_years']),
        'fcf_years_b':    _b(m['fcf_years']),
        'reasons':        '; '.join(reasons),
        'flags':          ' '.join(flags),
    }


# ── 扫描 ─────────────────────────────────────────────────────────────────────
def screen(tickers, years, min_cap, min_ocf_ni, jobs, refresh):
    rows = []
    done = 0
    with ThreadPoolExecutor(max_workers=jobs) as ex:
        futs = {ex.submit(fetch_cashflow, t, years, refresh): t for t in tickers}
        for fut in as_completed(futs):
            t = futs[fut]
            done += 1
            if done % 50 == 0:
                log.info(f'  ... {done}/{len(tickers)}')
            try:
                m = fut.result()
            except Exception as e:
                log.debug(f'{t}: fetch failed ({e})')
                continue
            if m['cap'] is not None and m['cap'] < min_cap:
                continue
            cls, reasons = classify(m, years, min_ocf_ni)
            rows.append(build_row(m, cls, reasons))

    # 白名单按 FCF yield 降序; 黑名单按市值降序 (大票的问题更值得看)
    order = {'WHITE': 0, 'BLACK': 1, 'GREY': 2, 'NA': 3}
    rows.sort(key=lambda r: (order[r['class']],
                             -(r['fcf_yield_pct'] or -999) if r['class'] == 'WHITE'
                             else -(r['mktcap_b'] or 0)))
    return rows


def render(date_str, rows, params):
    out = []

    def p(*a):
        line = ' '.join(str(x) for x in a)
        out.append(line); print(line)

    white = [r for r in rows if r['class'] == 'WHITE']
    black = [r for r in rows if r['class'] == 'BLACK']
    grey  = [r for r in rows if r['class'] == 'GREY']
    na    = [r for r in rows if r['class'] == 'NA']

    p()
    p('=' * 92)
    p(f'  US 现金流质地 白/黑名单  —  {date_str}   (OCF × FCF)')
    p('=' * 92)
    p(f'  股票池: {params["universe_desc"]} · 市值≥${params["min_cap"]/1e9:.0f}B'
      f' · 追溯{params["years"]}财年 · 白名单现金含量 OCF/NI≥{params["min_ocf_ni"]}')
    p(f'  白 {len(white)} / 黑 {len(black)} / 灰 {len(grey)} / 口径不适用(金融地产) {len(na)}')
    p()

    p(f'  ── 白名单 (现金流优质, 按 FCF yield 降序, 共 {len(white)} 只) ──')
    if white:
        table = tab_mod.tabulate(
            [[r['ticker'], r['sector'][:14], r['mktcap_b'], r['fcf_yield_pct'],
              r['fcf_margin_pct'], r['ocf_ni'], r['fcf_years_b'],
              ' '.join(x for x in (r['reasons'],) + tuple(
                  f for f in r['flags'].split() if f.startswith('fx:')) if x)]
             for r in white],
            headers=['Ticker', 'Sector', 'Cap$B', 'FCFyield%', 'FCF利润率%',
                     'OCF/NI', f'FCF近{params["years"]}年$B(新→旧)', '备注'],
            tablefmt='simple', floatfmt='.1f')
        p(table)
    else:
        p('  (无)')
    p()

    p(f'  ── 黑名单 (现金流有问题, 按市值降序, 共 {len(black)} 只) ──')
    if black:
        table = tab_mod.tabulate(
            [[r['ticker'], r['sector'][:14], r['mktcap_b'], r['ocf_ttm_b'],
              r['fcf_ttm_b'], r['ocf_ni'], r['fcf_years_b'], r['reasons']]
             for r in black],
            headers=['Ticker', 'Sector', 'Cap$B', 'OCFttm$B', 'FCFttm$B',
                     'OCF/NI', f'FCF近{params["years"]}年$B(新→旧)', 'reasons'],
            tablefmt='simple', floatfmt='.1f')
        p(table)
    else:
        p('  (无)')
    p()
    p('  判据:')
    p('    白 = 近N财年 OCF、FCF 每年>0 + TTM 未恶化 + 现金含量 OCF/NI≥0.8 (利润有现金背书)')
    p(f'    黑 = 任一: "{constant.NAG_CASHFLOW}"(最近财年OCF<0) /'
      f' "{constant.OCF_TTM_TURNED_NEG}"(年报正但TTM转负) /')
    p(f'         "{constant.NAG_FCF_MOST_YEARS}"(近3年FCF≥2年负) /'
      f' "{constant.N_CASHFLOW_ACT_LT_PROFIT}"(白条利润)')
    p('    FCFyield = TTM FCF/市值 (高=现金便宜) · OCF/NI = 最近财年经营现金流/净利润')
    p('  ⚠ 白名单是质地池非买入信号 (入场看技术层); 黑名单叠灰名单可当持仓/候选的否决线。')
    p('    金融/地产不判黑白: 银行 OCF 由放贷节奏驱动, REIT 折旧口径特殊。')
    p('    公用事业几乎整板块黑: 重资本受监管, FCF 负是结构性(靠发债滚 capex), 与烧钱成长股的负 FCF 性质不同, 自行区分。')
    p()
    return '\n'.join(out)


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--universe', default='both',
                      help='sp500 | ndx | both | r2000ht | midht | all (默认 both)')
    parser.add_option('--tickers', default=None, help='逗号分隔, 指定则跳过宽源')
    parser.add_option('--years', type='int', default=YEARS, help='追溯财年数, 默认 3')
    parser.add_option('--min-cap', type='float', default=MIN_MKTCAP / 1e9,
                      help='最小市值($B), 默认 1')
    parser.add_option('--min-ocf-ni', type='float', default=MIN_OCF_NI,
                      help='白名单现金含量下限 OCF/NI, 默认 0.8')
    parser.add_option('--jobs', type='int', default=8, help='并发取数线程, 默认 8')
    parser.add_option('--force', action='store_true', help='忽略当日股票池缓存')
    parser.add_option('--refresh', action='store_true', help='忽略每票现金流缓存(7天)重拉')
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

    params = {'universe_desc': universe_desc, 'years': opts.years,
              'min_cap': opts.min_cap * 1e9, 'min_ocf_ni': opts.min_ocf_ni}

    rows = screen(tickers, opts.years, params['min_cap'],
                  opts.min_ocf_ni, opts.jobs, opts.refresh)

    out_dir = os.path.join(RESULT_DIR, 'us_cashflow_quality')
    if os.path.isdir(RESULT_DIR):
        os.makedirs(out_dir, exist_ok=True)

    if rows and os.path.isdir(RESULT_DIR):
        csv_path = os.path.join(out_dir, f'us_cashflow_quality_{date_str}.csv')
        pd.DataFrame(rows).to_csv(csv_path, index=False)
        log.info(f'全池明细 → {csv_path}')

    text = render(date_str, rows, params)
    out_file = opts.output or (os.path.join(out_dir, f'us_cashflow_quality_{date_str}.txt')
                               if os.path.isdir(RESULT_DIR) else None)
    if out_file:
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(text + '\n')
        log.info(f'报告 → {out_file}')


if __name__ == '__main__':
    main()
