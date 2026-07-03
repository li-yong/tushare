# coding: utf-8
"""
CN 科创板 Swing Trade Scanner — A-share STAR-50 port of t_us_tech_swing.py

WHAT THIS IS
    A migration of the US Nasdaq swing system to China A-shares, focused on the
    科创50 (STAR-50) universe. The signal/exit/sizing logic is NOT reimplemented —
    it is the *same* calibrated engine from t_us_tech_swing.py, retargeted to the
    CN universe. Only the data target, watchlist, barometers, lot size, and the
    A-share-specific 涨跌停 handling are new here.

WHAT MIGRATED CLEANLY (reused verbatim from t_us_tech_swing)
    · Layer 1 市场状态 : two barometer ETFs vs 20-week MA → STRONG/MIXED/WEAK,
                         demoted when leadership breadth of the generals breaks.
    · Layer 2 入场     : breakout (STRONG) / pullback-to-MA (MIXED·WEAK) + key-level
                         stop + R:R ≥ 2:1.
    · Layer 4 退出     : weekly close < 20-week MA (Layer 2 of the trend system);
                         fast Layer 0/1 crash/event check on daily bars.
    · R 仓位           : shares = 1% equity / (entry-stop), capped 25%/name.
    Data path: yfinance with .SS/.SZ suffixes (ADR-0001, single bar source) — STAR
    names are 688xxx.SS; ETFs serve full history, the raw index does not.

WHAT CHANGED FOR A-SHARES (the structural deltas, see docs assessment)
    ⚠ ±20% 涨跌停 + 连续跌停: the fast hard-stop (Layer 0/1 "act now, don't wait for
      Friday") ASSUMES exit liquidity. In A-shares a crash locks 跌停 with no fill —
      you cannot sell. So the crash layer here is DEMOTED from "exit faster" to
      "de-risk EARLIER": it is surfaced as a warning, and the real defense is to
      have trimmed before the limit-down chain. The 涨跌停 state of each name is
      flagged (you also cannot BUY at 涨停).
    · 一手 = 100 股: share counts are floored to whole lots.
    · T+1: a name bought today cannot be sold today — the weekly exit rule is
      unaffected, only same-day round-trips are.
    · Earnings (PEAD/财报跳空) and Futu holdings sync are NOT ported in v1: yfinance
      carries no reliable A-share earnings calendar, and CN positions use SH./SZ.
      Futu prefixes. The migrated core is regime + entry + weekly exit + fast-risk
      surfacing — run manually, the user trades by hand (CONTEXT.md).

Usage:
  python t_cn_star_swing.py                 # full scan of the CN STAR watchlist
  python t_cn_star_swing.py --ticker 688981.SS   # single name (中芯国际)
  python t_cn_star_swing.py --asof 2025-09-01     # point-in-time backtest report
  python t_cn_star_swing.py --output /tmp/x.txt
"""

import os
import sys
import logging
import datetime
from optparse import OptionParser

import pandas as pd
import tabulate as tab_mod

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# The shared engine. We import it as a module and RETARGET its data layer +
# universe to the CN STAR pool, so every calibrated rule (signals, stops, R:R,
# crash/event layers, regime gate) is reused, not reimplemented.
import t_us_tech_swing as tsw

WATCHLIST_FILE = '/home/ryan/tushare_ryan/select.yml'
CN_BAR_CACHE_DIR = '/home/ryan/DATA/DAY_Global/CN_yf'   # CN bars, separate from US_yf
LOT_SIZE = 100                                          # 一手 = 100 股
STAR_LIMIT_PCT = 0.20                                   # 科创板 ±20% 日涨跌停

# Defaults double as the fallback if select.yml is missing/unreadable.
GENERALS   = ['688981.SS', '688041.SS', '688256.SS', '688012.SS', '688008.SS']
WATCH      = ['688126.SS', '688396.SS', '688516.SS', '688271.SS', '688169.SS',
              '688111.SS', '688036.SS', '688187.SS', '688303.SS', '688599.SS']
BAROMETERS = ['588000.SS', '512480.SS']
NAMES: dict = {}            # ticker -> 中文名 (display only)
EQUITY = 0.0


def _load_watchlist():
    """Load the CN STAR watchlist + names + equity from select.yml; fall back to
    the built-in defaults. Entries are `TICKER: 中文名` maps (or bare tickers)."""
    global GENERALS, WATCH, BAROMETERS, NAMES, EQUITY
    try:
        import yaml
        with open(WATCHLIST_FILE) as fh:
            cfg = yaml.safe_load(fh) or {}

        def _rows(key, default):
            rows = cfg.get(key)
            if not rows:
                return default
            out = []
            for row in rows:
                if isinstance(row, dict):
                    tkr = next(iter(row))
                    NAMES[str(tkr).upper()] = str(row[tkr])
                    out.append(tkr)
                else:
                    out.append(row)
            return [str(t).upper() for t in out]

        GENERALS   = _rows('CN_STAR_GENERALS', GENERALS)
        WATCH      = _rows('CN_STAR_WATCH', WATCH)
        BAROMETERS = _rows('CN_STAR_BAROMETERS', BAROMETERS)
        EQUITY     = float(cfg.get('CN_STAR_EQUITY', 0) or 0)
    except Exception as e:
        logging.warning(f'watchlist load failed ({e}) — using built-in defaults')


def _retarget_engine():
    """Point the shared scanner's data layer + barometers at the CN universe.

    The signal logic in t_us_tech_swing is ticker-agnostic (pure DataFrame math);
    only the cache directory and the barometer list are US-specific globals. We
    swap those two and reuse everything else."""
    tsw.BAR_CACHE_DIR = CN_BAR_CACHE_DIR
    tsw.BAROMETERS = BAROMETERS


def _name(ticker: str) -> str:
    return NAMES.get(ticker, '')


def _limit_flag(daily: 'pd.DataFrame | None') -> str:
    """涨停 / 跌停 flag for the latest bar (科创板 ±20%).

    A name at 涨停 cannot be bought; a name at 跌停 cannot be sold — both override
    any signal/stop the engine produced. Uses a 1% tolerance off the exact cap so
    a close pinned at the limit registers regardless of rounding."""
    if daily is None or len(daily) < 2:
        return ''
    prev = float(daily['close'].iloc[-2])
    cur = float(daily['close'].iloc[-1])
    if prev <= 0:
        return ''
    chg = cur / prev - 1
    if chg >= STAR_LIMIT_PCT - 0.01:
        return '涨停'
    if chg <= -(STAR_LIMIT_PCT - 0.01):
        return '跌停'
    return ''


def _cn_position_size(entry, stop, equity):
    """R-based size, floored to whole 100-share lots (一手)."""
    shares, r_amt, _cap = tsw._position_size(entry, stop, equity)
    lots = shares // LOT_SIZE
    return int(lots * LOT_SIZE), r_amt


def scan_name(ticker: str, market_state: str) -> dict:
    """Reuse the engine's per-ticker scan, then attach A-share-specific flags."""
    r = tsw.scan_stock(ticker, market_state)
    r['name'] = _name(ticker)
    daily = tsw._history(ticker, period='1y', interval='1d')
    r['limit'] = _limit_flag(daily)
    layer, det = tsw._crash_event_check(daily)
    r['risk_layer'] = layer
    r['risk_detail'] = det
    return r


# ── Report ──────────────────────────────────────────────────────────────────────
def _fmt(val, d=2):
    return '—' if val is None else f'{val:.{d}f}'


def print_report(market_state, baro_info, results, baro_state, lead_frac,
                 lead_detail, equity=0.0, output_file=None):
    lines = []

    def p(*a):
        line = ' '.join(str(x) for x in a)
        lines.append(line)
        print(line)

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    p()
    p('=' * 76)
    p(f'  科创板 SWING SCANNER (STAR-50)  —  {now_str}')
    p('=' * 76)

    # ── 市场状态 ──────────────────────────────────────────────────────────────
    p()
    p(f'[ 市场状态 MARKET STATE: {market_state} ]')
    baro_rows = []
    for t, d in baro_info.items():
        if d:
            baro_rows.append([f'{t} {_name(t)}'.strip(), _fmt(d['close']),
                              _fmt(d['ma20w']), f"{d['pct_vs_ma']:+.1f}%",
                              '在上 ✓' if d['above'] else '在下 ✗'])
    p(tab_mod.tabulate(baro_rows,
                       headers=['ETF', '收盘', '20周线', 'vs MA', '状态'],
                       tablefmt='simple'))
    if lead_detail:
        below = sorted(t for t, b in lead_detail.items() if b)
        names_below = ', '.join(f'{t}{_name(t)}' for t in below) if below else 'none'
        p(f'  领导股广度 (龙头 < 20周线): {lead_frac:.0%}  [{names_below}]')
        if baro_state is not None and market_state != baro_state:
            p(f'  ⚠ GATE: 领导股走弱 (≥{tsw.LEADERSHIP_BREACH_FRAC:.0%}) '
              f'→ 扫描降档 {baro_state} → {market_state}')
    mode_msg = {
        'STRONG': '  → 扫描模式: 突破 BREAKOUT  (周收盘破10周区间高 + 量≥1.5×)',
        'MIXED':  '  → 扫描模式: 回踩 PULLBACK   (触20/50日线 + 缩量 + 反转阳)',
        'WEAK':   '  → 扫描模式: 谨慎 CAUTION    (仅回踩, 降低仓位)',
        'ERROR':  '  → 扫描模式: 未知 (行情数据异常)',
    }
    p(mode_msg.get(market_state, ''))

    # ── 入场信号 ──────────────────────────────────────────────────────────────
    p()
    if equity and equity > 0:
        p(f'[ 入场信号 ENTRY ]  (本金 ¥{equity:,.0f} · 1R=¥{equity*tsw.RISK_PCT:,.0f} · '
          f'单票≤{tsw.MAX_POSITION_PCT:.0%} · 一手={LOT_SIZE}股)')
    else:
        p('[ 入场信号 ENTRY ]  (在 select.yml 设 CN_STAR_EQUITY 以显示买入手数)')

    sig_rows, watch_rows = [], []
    for r in results:
        tkr, nm = r['ticker'], r.get('name', '')
        label = f'{tkr} {nm}'.strip()
        close = _fmt(r['close'])
        if r['error']:
            watch_rows.append([label, close, '—', '—', '—', r['error']])
            continue

        flags = []
        if r.get('limit'):
            flags.append(r['limit'])
        if r.get('risk_layer') == 0:
            flags.append(f"🛑L0崩塌·{r['risk_detail'].get('reason','')}")
        elif r.get('risk_layer') == 1:
            flags.append(f"⚠L1事件·{r['risk_detail'].get('reason','')}")
        flag_note = ' '.join(flags)

        s = r['signal']
        if s:
            vol_note = ''
            if s['type'] == 'BREAKOUT':
                basis = 'key' if s.get('stop_basis') == 'key-level' else 'rng'
                vol_note = f"vol×{s['vol_ratio']:.1f}{'✓' if s['high_vol'] else '✗'} stop:{basis}"
            elif s['type'] == 'PULLBACK':
                fl = []
                if s.get('declining_vol'): fl.append('vol↓')
                if s.get('reversal'):      fl.append('阳')
                vol_note = ' '.join(fl)
            rr_str = f"{s['rr']:.1f}:1 {'✓' if s['rr_ok'] else '✗'}"
            lots, _r = _cn_position_size(s['entry'], s['stop'], equity)
            sh = f'{lots}' if lots else '—'
            buy_block = ' ⛔涨停勿追' if r.get('limit') == '涨停' else ''
            notes = f"{s['type']}[{s['confidence']}] {vol_note} {flag_note}{buy_block}".strip()
            sig_rows.append([label, close, _fmt(s['entry']), _fmt(s['stop']),
                             _fmt(s['target']), rr_str, sh, notes])
        else:
            exit_flag = '⚠退出(破20周线)' if r['exit_signal'] else ''
            notes = ' | '.join(filter(None, [exit_flag, flag_note]))
            watch_rows.append([label, close, _fmt(r['ma20d']), _fmt(r['ma50d']),
                               _fmt(r['ma20w']), notes])

    if sig_rows:
        p(tab_mod.tabulate(sig_rows,
                           headers=['标的', '收盘', '入场', '止损', '目标', 'R:R', '手数', '备注'],
                           tablefmt='simple'))
    else:
        p('  本周无入场信号。')

    if watch_rows:
        p()
        p('[ 观察区 — 暂无信号 ]')
        p(tab_mod.tabulate(watch_rows,
                           headers=['标的', '收盘', 'MA20日', 'MA50日', 'MA20周', '备注'],
                           tablefmt='simple'))

    # ── A股结构性提醒 ──────────────────────────────────────────────────────────
    p()
    p('[ ⚠ A股结构性提醒 (与美股的关键差异) ]')
    p(f'  · 涨跌停 ±{STAR_LIMIT_PCT:.0%}: 快速硬止损(L0/L1)在 连续跌停 时【可能根本卖不掉】。')
    p('    → 美股的"暴跌就立刻跑"在这里失效, 改为【提前减仓】: 周线/事件层要更早动手,')
    p('      因为你无法更快地动手。涨停的票【买不进】, 别追挂单。')
    p('  · T+1: 当日买入次日才能卖 (周线退出规则不受影响, 仅当日回转受限)。')
    p('  · 财报跳空(PEAD)/Futu 持仓同步 v1 未迁移 (yfinance 无 A 股财报历; 持仓走 SH./SZ.)。')

    # ── 下单前检查 ──────────────────────────────────────────────────────────────
    p()
    p('[ 下单前检查 — 收盘后决策, 不盘中拍脑袋 ]')
    for item in [
        '催化剂确认? (业绩/产业周期, 不靠 yfinance, 需人工核)',
        '入场 = 上一根收盘价 (收盘后定, 非盘中)',
        '止损 = 写死的技术位 (非固定%)  →  ______',
        '目标已写, R:R ≥ 2:1  →  ______',
        '退出条件已定 (什么会证伪逻辑)?',
        '手数取自表格 (1R=1%本金, 单票≤25%, floor 到整手)?',
        '⚠ 该票今日是否涨停(买不进)/跌停(卖不掉)?',
        '并发持仓 ≤ 5?',
    ]:
        p(f'  □  {item}')

    # ── 图例 ────────────────────────────────────────────────────────────────────
    p()
    p('[ 字段说明 / LEGEND ]')
    p(f'  市场状态: STRONG=两个barometer均在{tsw.MA_WEEKLY}周线上 / MIXED=一上一下 / WEAK=均在下')
    p(f'    领导股广度: 龙头({len(GENERALS)}只)跌破各自{tsw.MA_WEEKLY}周线的比例; ≥{tsw.LEADERSHIP_BREACH_FRAC:.0%}触发降档')
    p('  入场信号: 入场/止损/目标; R:R=(目标-入场)/(入场-止损), ✓=≥2:1; 手数=建议买入(整手, 非持仓)')
    p('  备注: vol×N=量比 · stop:key=关键支撑/rng=区间低 · 涨停/跌停=今日触及限价')
    p(f'    🛑L0崩塌=近{tsw.CRASH_WINDOW_D}日峰→现≤-{tsw.HARD_STOP_PCT:.0f}% · ⚠L1事件=≤-{tsw.EVENT_DROP_PCT:.0f}%或异常波动')
    p('  退出: 周收盘跌破20周线 → 清 (优先于止盈); 进场敏感(信关键K), 离场迟钝(只信周线)')
    p('  产业链(人工背景): 设备(中微/北方华创)→大硅片(沪硅)→代工(中芯)→设计(海光/寒武纪)')
    p()

    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        logging.info(f'Report saved → {output_file}')


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--ticker', dest='ticker', default=None,
                      help='Scan a single ticker (e.g. 688981.SS) instead of the watchlist')
    parser.add_option('--output', dest='output', default=None,
                      help='Save report to this file path')
    parser.add_option('--asof', dest='asof', default=None,
                      help='Backtest as-of YYYY-MM-DD: use only bars ≤ that day')
    opts, _ = parser.parse_args()

    _load_watchlist()
    _retarget_engine()

    if opts.asof:
        try:
            tsw._ASOF = pd.Timestamp(opts.asof).normalize()
        except ValueError:
            parser.error(f'--asof 无法解析: {opts.asof}')
        logging.info(f'AS-OF backtest mode: anchoring to {tsw._ASOF.date()}')

    logging.info('CN STAR Swing Scanner starting')

    # Layer 1: regime (barometers + leadership breadth gate)
    baro_state, baro_info = tsw.get_market_state()
    lead_frac, lead_detail = tsw.get_leadership_breadth(GENERALS)
    market_state = tsw._gate_state(baro_state, lead_frac)
    logging.info(f'Market state: {market_state} (barometer {baro_state})')

    # Layer 2/3/4: per-name scan
    universe = [opts.ticker.upper()] if opts.ticker else (GENERALS + WATCH)
    results = []
    for tkr in universe:
        logging.info(f'Scanning {tkr} {_name(tkr)} …')
        results.append(scan_name(tkr, market_state))

    date_str = (tsw._ASOF.strftime('%Y%m%d') if tsw._ASOF is not None
                else datetime.datetime.now().strftime('%Y%m%d'))
    out_file = opts.output
    if out_file is None:
        res_root = '/home/ryan/DATA/result'
        if os.path.isdir(res_root):
            out_dir = os.path.join(res_root, 'cn_star_swing')
            os.makedirs(out_dir, exist_ok=True)
            out_file = f'{out_dir}/cn_star_swing_{date_str}.txt'

    print_report(market_state, baro_info, results, baro_state, lead_frac,
                 lead_detail, equity=EQUITY, output_file=out_file)


if __name__ == '__main__':
    main()
