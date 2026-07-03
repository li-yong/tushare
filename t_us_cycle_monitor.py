# coding: utf-8
"""
US Memory-Cycle Monitor — DRAM/AI 周期监测器 (笔记 §八)

实现投资研究笔记 §八 的三个周期监测指标(docs/investment_research_20260613.md),
数据源 = SEC EDGAR XBRL (data.sec.gov, 免费/结构化/最权威)。目的:在周期上行中
盯住"扩产倒计时", 在管理层喊话之前用财报硬数据捕捉拐点。

三个指标(全部从同一份 XBRL 季度序列算出):

  指标1 价格趋势(代理)= 毛利率环比
     笔记原指标是"现货/合同价格", 结构化数据里没有, 但毛利率是它的天然代理:
     涨价→毛利扩张, 跌价→毛利收缩。报警: 连续两季毛利率环比下跌。
     ⚠ 真·价格信号仍需读财报电话会 Prepared Remarks(管理层会说"价格环比±X%"),
       本指标是硬数据兜底, 不替代。

  指标2 库存周数 = InventoryNet ÷ (单季COGS / 13周)
     报警: >12周(84天)警戒, >20周(140天)清仓。
     ⚠ 笔记阈值是"行业渠道库存"口径; 这里是公司资产负债表库存, 趋势(环比升=囤货/
       需求转弱)比绝对值更可靠。

  指标3 CapEx 扩产 = 单季资本开支 / TTM / 同比
     报警: TTM CapEx 显著同比加速 + capex/营收比上升 → 12–18 个月后供给过剩。
     (CapEx 现金流量表只报 YTD 累计, 脚本按财年差分还原单季。)

单季还原: XBRL duration facts 按 start 分组, 含≥2个 end 的链 = YTD 累计链,
逐期差分得单季(自然涵盖 10-K 的 Q4); InventoryNet 为时点值, 直接取季末。

Usage:
  python t_us_cycle_monitor.py                 # 默认 MU
  python t_us_cycle_monitor.py --ticker MU
  python t_us_cycle_monitor.py --quarters 10   # 展示最近 N 季 (默认 8)
  python t_us_cycle_monitor.py --json
"""

import os
import sys
import json
import logging
import datetime
import urllib.request
from io import StringIO
from collections import defaultdict
from datetime import date
from optparse import OptionParser

import tabulate as tab_mod

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stderr)],   # 日志走 stderr, stdout 留给报告/JSON
)
log = logging.getLogger('cycle')

RESULT_DIR = '/home/ryan/DATA/result'
CACHE_DIR  = '/home/ryan/DATA/DAY_Global/US_edgar'
# SEC 要求带可联系的 User-Agent, 否则 403
UA = {'User-Agent': 'tushare-ryan research sunraise2005@gmail.com'}

# 各指标用到的 us-gaap concept(按优先级, 取第一个有数据的)
CONCEPTS = {
    'cogs':    ['CostOfGoodsAndServicesSold', 'CostOfRevenue', 'CostOfGoodsSold'],
    'revenue': ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues',
                'RevenueFromContractWithCustomerIncludingAssessedTax', 'SalesRevenueNet'],
    'capex':   ['PaymentsToAcquirePropertyPlantAndEquipment',
                'PaymentsToAcquireProductiveAssets'],
    'inventory': ['InventoryNet', 'InventoryFinishedGoodsNetOfReserves'],
}


# ── EDGAR 取数 ───────────────────────────────────────────────────────────────
def _get(url: str):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=30).read())


def resolve_cik(ticker: str) -> str | None:
    """ticker → 10 位 CIK (company_tickers.json, 当日缓存)。"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, 'company_tickers.json')
    fresh = (os.path.exists(path) and
             date.fromtimestamp(os.path.getmtime(path)) == date.today())
    try:
        if fresh:
            with open(path) as fh:
                m = json.load(fh)
        else:
            m = _get('https://www.sec.gov/files/company_tickers.json')
            with open(path, 'w') as fh:
                json.dump(m, fh)
    except Exception as e:
        log.error(f'company_tickers 获取失败 ({e})')
        if os.path.exists(path):
            with open(path) as fh:
                m = json.load(fh)
        else:
            return None
    tk = ticker.upper()
    for v in m.values():
        if v.get('ticker', '').upper() == tk:
            return str(v['cik_str']).zfill(10)
    return None


def concept_facts(cik: str, names: list) -> list | None:
    """取第一个有数据的 concept 的 USD facts。"""
    for c in names:
        try:
            d = _get(f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{c}.json')
            units = d.get('units', {})
            usd = units.get('USD')
            if usd:
                return usd
        except Exception:
            continue
    return None


# ── 单季还原 ─────────────────────────────────────────────────────────────────
def quarterly_flow(facts: list) -> dict:
    """duration facts → {end_date(str): 单季值}, 时间升序。

    XBRL 现金流/利润表项报 YTD 累计。按 start 分组: 同一 start 的多个 end
    构成一条财年 YTD 链(Q1 90d, Q2 181d, Q3 272d, FY 363d), 逐期差分得单季,
    第一期(Q1)直接用。再补入未被覆盖的独立 ~90d 单季 fact。
    """
    byse = {}
    for f in facts:
        if 'start' not in f or 'end' not in f:
            continue
        byse[(f['start'], f['end'])] = f['val']     # 去重(同区间跨 fy 标签重复)

    groups = defaultdict(list)
    for (s, e), v in byse.items():
        groups[s].append((e, v))

    q = {}
    for s, lst in groups.items():
        if len(lst) < 2:
            continue                                # 非 YTD 链, 留到下方独立补入
        lst.sort()
        prev = 0.0
        for e, v in lst:
            q[e] = v - prev
            prev = v
    # 补入独立的单季(~90天)fact: 一条 FY 只报了 Q1 时也能覆盖到
    for (s, e), v in byse.items():
        span = (date.fromisoformat(e) - date.fromisoformat(s)).days
        if 80 <= span <= 100 and e not in q:
            q[e] = v
    return dict(sorted(q.items()))


def instant_series(facts: list) -> dict:
    """instant facts(如库存)→ {end_date: 值}, 升序。同 end 取 10-K/10-Q 最新。"""
    out = {}
    for f in facts:
        e = f.get('end')
        if e is not None:
            out[e] = f['val']                       # facts 已按时间升序, 后者覆盖前者
    return dict(sorted(out.items()))


# ── 指标计算 ─────────────────────────────────────────────────────────────────
def build_quarters(cik: str) -> list:
    """返回按季升序的 list[dict]: end, revenue, cogs, gross_margin, capex,
    inventory, inv_weeks。缺失项为 None。"""
    raw = {}
    for k, names in CONCEPTS.items():
        f = concept_facts(cik, names)
        if f is None:
            log.warning(f'{k}: 无 XBRL 数据 (尝试过 {names[0]} 等)')
            raw[k] = {}
        else:
            raw[k] = instant_series(f) if k == 'inventory' else quarterly_flow(f)

    ends = sorted(set(raw['revenue']) | set(raw['cogs']) | set(raw['capex']))
    rows = []
    for e in ends:
        rev = raw['revenue'].get(e)
        cogs = raw['cogs'].get(e)
        capex = raw['capex'].get(e)
        inv = raw['inventory'].get(e)
        gm = ((rev - cogs) / rev * 100) if (rev and cogs is not None and rev != 0) else None
        inv_wk = (inv / (cogs / 13.0)) if (inv and cogs and cogs != 0) else None
        rows.append({'end': e, 'revenue': rev, 'cogs': cogs, 'gross_margin': gm,
                     'capex': capex, 'inventory': inv, 'inv_weeks': inv_wk})
    return rows


def _ttm(rows, key, idx):
    """rows[idx-3..idx] 的 key 之和(需 4 季齐全), 否则 None。"""
    if idx < 3:
        return None
    vals = [rows[i].get(key) for i in range(idx - 3, idx + 1)]
    return sum(vals) if all(v is not None for v in vals) else None


def assess(rows: list) -> dict:
    """三指标信号牌(🟢/🟡/🔴)+ 文字结论。"""
    sig = {}
    n = len(rows)

    # 指标1: 毛利率环比(价格代理)
    gms = [(r['end'], r['gross_margin']) for r in rows if r['gross_margin'] is not None]
    if len(gms) >= 3:
        last3 = gms[-3:]
        d1 = last3[-1][1] - last3[-2][1]
        d2 = last3[-2][1] - last3[-3][1]
        if d1 < 0 and d2 < 0:
            sig['price'] = ('🔴', f'毛利率连续两季环比下跌 ({last3[-3][1]:.0f}%→'
                                  f'{last3[-2][1]:.0f}%→{last3[-1][1]:.0f}%) — 价格见顶信号')
        elif d1 < 0:
            sig['price'] = ('🟡', f'毛利率环比转跌 ({last3[-2][1]:.0f}%→{last3[-1][1]:.0f}%), 观察')
        else:
            sig['price'] = ('🟢', f'毛利率环比上行 ({last3[-2][1]:.0f}%→{last3[-1][1]:.0f}%) — 涨价周期')
    else:
        sig['price'] = ('⚪', '毛利率数据不足')

    # 指标2: 库存周数
    iws = [(r['end'], r['inv_weeks']) for r in rows if r['inv_weeks'] is not None]
    if iws:
        end, wk = iws[-1]
        trend = ''
        if len(iws) >= 2:
            dwk = wk - iws[-2][1]
            trend = f', 环比{"+" if dwk>=0 else ""}{dwk:.1f}周'
        if wk > 20:
            sig['inventory'] = ('🔴', f'库存 {wk:.1f}周 (>{20}周清仓线){trend}')
        elif wk > 12:
            sig['inventory'] = ('🟡', f'库存 {wk:.1f}周 (>{12}周警戒){trend}')
        else:
            sig['inventory'] = ('🟢', f'库存 {wk:.1f}周 (<12周安全){trend}')
    else:
        sig['inventory'] = ('⚪', '库存周数数据不足')

    # 指标3: CapEx 扩产(TTM 同比 + 强度)
    ttm_capex = ttm_capex_prev = ttm_rev = None
    for i in range(n - 1, -1, -1):
        if _ttm(rows, 'capex', i) is not None:
            ttm_capex = _ttm(rows, 'capex', i)
            ttm_rev = _ttm(rows, 'revenue', i)
            ttm_capex_prev = _ttm(rows, 'capex', i - 4) if i - 4 >= 3 else None
            break
    if ttm_capex is not None:
        yoy = ((ttm_capex / ttm_capex_prev - 1) * 100) if ttm_capex_prev else None
        intensity = (ttm_capex / ttm_rev * 100) if ttm_rev else None
        msg = f'TTM CapEx ${ttm_capex/1e9:.1f}B'
        if yoy is not None:
            msg += f', 同比{"+" if yoy>=0 else ""}{yoy:.0f}%'
        if intensity is not None:
            msg += f', 占营收{intensity:.0f}%'
        if yoy is not None and yoy >= 40:
            sig['capex'] = ('🟡', msg + ' — 扩产加速, 风险窗口 12–18 个月后')
        elif yoy is not None and yoy <= -15:
            sig['capex'] = ('🟢', msg + ' — 收缩产能, 利好后续供需')
        else:
            sig['capex'] = ('🟢', msg)
    else:
        sig['capex'] = ('⚪', 'CapEx TTM 数据不足')

    return sig


# ── 报告 ─────────────────────────────────────────────────────────────────────
def render(ticker, cik, rows, sig, n_show):
    out = []

    def p(*a):
        line = ' '.join(str(x) for x in a)
        out.append(line); print(line)

    p()
    p('=' * 84)
    p(f'  US 周期监测  {ticker}  (CIK {cik})  —  DRAM/AI 内存周期 (笔记 §八)')
    p('=' * 84)
    show = rows[-n_show:]
    table = tab_mod.tabulate(
        [[r['end'],
          f"{r['revenue']/1e9:.2f}" if r['revenue'] else '—',
          f"{r['gross_margin']:.1f}" if r['gross_margin'] is not None else '—',
          f"{r['inv_weeks']:.1f}" if r['inv_weeks'] is not None else '—',
          f"{r['capex']/1e9:.2f}" if r['capex'] else '—'] for r in show],
        headers=['季末', '营收$B', '毛利%', '库存周', 'CapEx$B'],
        tablefmt='simple')
    p(table)
    p()
    p('  ── 三指标信号牌 ──')
    p(f"  指标1 价格趋势(毛利代理)  {sig['price'][0]}  {sig['price'][1]}")
    p(f"  指标2 库存周数            {sig['inventory'][0]}  {sig['inventory'][1]}")
    p(f"  指标3 CapEx 扩产          {sig['capex'][0]}  {sig['capex'][1]}")
    p()
    reds = [k for k, v in sig.items() if v[0] == '🔴']
    yels = [k for k, v in sig.items() if v[0] == '🟡']
    if reds:
        p(f'  ⚠ 综合: {len(reds)} 项红灯 — 周期见顶信号出现, 对照笔记 §十 减仓纪律。')
    elif yels:
        p(f'  综合: 周期仍上行, 但 {len(yels)} 项黄灯(扩产倒计时已开始)。')
    else:
        p('  综合: 三指标健康, 周期上行。')
    p('  注: 指标1 仅毛利代理; 真·价格信号读财报电话会 Prepared Remarks(管理层口径)。')
    p(f'  下次财报后重跑即可。EDGAR 季度数据通常在 10-Q 提交后 1–2 日内可得。')
    p()
    return '\n'.join(out)


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--ticker', default='MU', help='标的, 默认 MU')
    parser.add_option('--quarters', type='int', default=8, help='展示最近 N 季, 默认 8')
    parser.add_option('--json', action='store_true', dest='as_json', help='输出 JSON')
    parser.add_option('--output', default=None, help='报告输出路径')
    opts, _ = parser.parse_args()

    ticker = opts.ticker.upper()
    cik = resolve_cik(ticker)
    if cik is None:
        log.error(f'找不到 {ticker} 的 CIK (非美股 SEC 注册?)')
        sys.exit(1)
    log.info(f'{ticker} → CIK {cik}, 拉取 EDGAR XBRL ...')

    rows = build_quarters(cik)
    if not rows:
        log.error('无可用季度数据。')
        sys.exit(1)
    sig = assess(rows)

    if opts.as_json:
        print(json.dumps({'ticker': ticker, 'cik': cik,
                          'quarters': rows[-opts.quarters:],
                          'signals': {k: {'light': v[0], 'note': v[1]} for k, v in sig.items()}},
                         ensure_ascii=False, indent=1))
        return

    date_str = datetime.datetime.now().strftime('%Y%m%d')
    text = render(ticker, cik, rows, sig, opts.quarters)
    out_dir = os.path.join(RESULT_DIR, 'us_cycle_monitor')
    if os.path.isdir(RESULT_DIR):
        os.makedirs(out_dir, exist_ok=True)
    out_file = opts.output or (os.path.join(out_dir, f'us_cycle_{ticker}_{date_str}.txt')
                               if os.path.isdir(RESULT_DIR) else None)
    if out_file:
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(text + '\n')
        log.info(f'报告 → {out_file}')


if __name__ == '__main__':
    main()
