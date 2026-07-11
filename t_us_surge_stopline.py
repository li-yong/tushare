# coding: utf-8
"""
US Surge-Stopline Monitor — 持仓·关键K线(surge日)收盘失守警报

规则(用户 2026-07-11 澄清版):
  某日 TR/前收 ≥ --thr (默认 7%) 且为上涨日(收>前收; --any-direction 关掉方向要求)
  → 该日【收盘价】成为 stopline; 之后任一日收盘 < stopline → ALERT;
  新 surge 日的收盘【永远替换】旧线(不做只上移 ratchet)。
  语义 = "最后一根 surge 日的成果全部回吐 → 假突破/顶部第一声裂响"。

实证(CSCO 1995-2003 / NVDA 2023-26, 本轮对话):
  CSCO 2000 顶: 该规则在顶后 2 天、距顶 -5.0% (2000-03-29) 即报警 —— 所有测过
  规则里最早(现行 L2 20周线是 -28.8%); 但牛市段假警报 36 次(仅上涨日版)、
  中位买回溢价 +3.2%, NVDA melt-up 三年半 37 次 —— 当全仓机械止损复利成本远超
  顶部所省。故本脚本定位 = 【持仓警报层/人工复核触发器】, NOT a stop engine:
  退出政策归 swing scanner 的分层止损 (ADR-0002); 它管"响亮的顶"的第一声,
  温水煮青蛙式的安静顶归 20周线 (互补, 见 memory boiling-frog-slow-grind)。

持仓池: select.yml US_SWING_STOPS keys ∪ US_HOLD_EXTRA (同 t_us_intraday_internals)。
数据: 复用 t_us_tech_swing 的 yfinance 缓存 (_fetch_daily, ADR-0001), 无 Futu 依赖。

Usage:
  python t_us_surge_stopline.py                       # 持仓池巡检 (cron 模式)
  python t_us_surge_stopline.py --ticker CSCO --asof 2000-05-01   # 单票诊断/回放
  python t_us_surge_stopline.py --thr 10 --any-direction
"""

import os
import sys
import logging
import datetime

import numpy as np
import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

from t_us_tech_swing import _fetch_daily

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR = '/home/ryan/DATA/result'
SELECT_YML = '/home/ryan/tushare_ryan/select.yml'

THR_PCT  = 7.0    # surge 日门槛: TR/前收 ≥ 此值(%)
UP_ONLY  = True   # 默认只用上涨 surge 日取线(实证: 假警报少 36% 且逃生点相同)
MIN_BARS = 30


def load_holdings() -> list:
    import yaml
    with open(SELECT_YML) as fh:
        cfg = yaml.safe_load(fh) or {}
    stops = [str(t).upper() for t in (cfg.get('US_SWING_STOPS') or {})]
    extra = [str(t).upper() for t in (cfg.get('US_HOLD_EXTRA') or [])]
    return list(dict.fromkeys(stops + extra))


def annotate(df: pd.DataFrame, thr: float, up_only: bool) -> pd.DataFrame:
    """标 surge 日与 stopline。stopline 列 = 当日收盘后生效的线(含当日 surge);
    警报判定用【前一日生效的线】(surge 日自身不因换线而自报警)。"""
    d = df.copy()
    prev = d['close'].shift(1)
    tr = pd.concat([d['high'] - d['low'],
                    (d['high'] - prev).abs(),
                    (d['low'] - prev).abs()], axis=1).max(axis=1)
    d['tr_pct'] = tr / prev * 100.0
    d['surge'] = (d['tr_pct'] >= thr) & ((d['close'] > prev) if up_only else True)
    d['stopline'] = d['close'].where(d['surge']).ffill()
    d['alert'] = d['close'] < d['stopline'].shift(1)
    return d


def _status(d: pd.DataFrame) -> dict:
    """最新一天的状态摘要 (d 已截断到 asof)。"""
    last = d.iloc[-1]
    if pd.isna(last['stopline']):
        return {'armed': False}
    kb_pos = int(np.where(d['surge'].values)[0][-1])
    kb = d.iloc[kb_pos]
    age = len(d) - 1 - kb_pos
    # 自本线生效以来的连续/首次破线
    seg = d.iloc[kb_pos + 1:]
    breaches = seg.index[seg['close'] < float(kb['close'])]
    run = 0
    for c in reversed(seg['close'].values):
        if c < float(kb['close']):
            run += 1
        else:
            break
    return {
        'armed': True, 'kb_date': d.index[kb_pos], 'age': age,
        'stopline': float(kb['close']), 'kb_tr': float(kb['tr_pct']),
        'close': float(last['close']),
        'dist': (float(last['close']) / float(kb['close']) - 1.0) * 100.0,
        'alert': bool(last['alert']),
        'first_breach': breaches[0] if len(breaches) else None,
        'run': run,
        'fresh_kb': age == 0,
    }


# ── 模式一: 持仓池巡检 (cron) ─────────────────────────────────────────────────
def run_hold(thr, up_only, asof=None):
    pool = load_holdings()
    if not pool:
        logging.error('持仓池为空 (US_SWING_STOPS / US_HOLD_EXTRA)')
        return
    asof_txt = f' (as-of {asof.date()})' if asof is not None else ''
    rows, n_alert = [], 0
    for t in pool:
        try:
            df = _fetch_daily(t)
            if df.empty:
                continue
            d = annotate(df, thr, up_only)
            if asof is not None:
                d = d[d.index <= asof]
            if len(d) < MIN_BARS:
                continue
            s = _status(d)
            if not s['armed']:
                rows.append({'ticker': t, 'surge日': '—', 'age': '—', 'TR%': '—',
                             'stopline': '—', 'close': round(float(d['close'].iloc[-1]), 2),
                             'dist%': '—', '状态': '未武装(近史无surge日)'})
                continue
            if s['alert']:
                n_alert += 1
                state = (f"⚠ ALERT 连续{s['run']}日"
                         + (f" (首破 {s['first_breach'].date()})" if s['run'] > 1 else ''))
            elif s['fresh_kb']:
                state = '★ 今日新线'
            else:
                state = 'OK'
            rows.append({
                'ticker': t, 'surge日': s['kb_date'].date(), 'age': s['age'],
                'TR%': round(s['kb_tr'], 1), 'stopline': round(s['stopline'], 2),
                'close': round(s['close'], 2), 'dist%': round(s['dist'], 1),
                '状态': state,
            })
        except Exception as e:
            logging.warning(f'{t}: {e}')

    out = pd.DataFrame(rows)
    print(f"\n持仓·Surge日收盘失守警报{asof_txt}  "
          f"(TR≥{thr}%{'·仅上涨日' if up_only else '·任意方向'}, "
          f"池 {len(pool)} 只, ALERT {n_alert})")
    print(tab_mod.tabulate(out, headers='keys', tablefmt='github', showindex=False))
    print(f"\n列义: surge日=最近一根 TR≥{thr}% {'阳线' if up_only else ''}关键日 · "
          "age=距今交易日 · stopline=该日收盘(最新替换旧线) · "
          "dist%=现价距线(负=已破线) · 状态: ALERT=收盘失守(surge日成果全部回吐)。")
    print("解读: ALERT=响亮上攻被完全否定, 是复核/减仓评估的触发器, 不是卖出指令 — "
          "退出纪律归分层止损(ADR-0002)。age 越小线越新鲜; 陈年老线(age>60)参考意义弱。"
          "实证: CSCO 2000 顶后2日(-5%)即报警, 但牛市中假警报常见(NVDA melt-up ~37次/3.5年), "
          "单独动作不划算, 结合高位TR_DOWN/新闻稻草等旁证用。")

    tag = (asof or pd.Timestamp(datetime.date.today())).strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_surge_stopline')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_surge_stopline_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


# ── 模式二: 单票诊断/历史回放 ─────────────────────────────────────────────────
def run_ticker(ticker, thr, up_only, asof=None, history=250):
    df = _fetch_daily(ticker)
    if df.empty:
        logging.error(f'{ticker}: 无数据'); return
    d = annotate(df, thr, up_only)
    if asof is not None:
        d = d[d.index <= asof]
    if len(d) < MIN_BARS:
        logging.error(f'{ticker}: 数据不足'); return
    s = _status(d)
    asof_txt = f' (as-of {d.index[-1].date()})' if asof is not None else ''
    print(f"\n{ticker} Surge-Stopline 诊断{asof_txt}  收 ${d['close'].iloc[-1]:.2f}  "
          f"(TR≥{thr}%{'·仅上涨日' if up_only else ''})")
    if not s['armed']:
        print('  未武装: 历史上无 surge 日。')
        return
    print(f"  当前线: {s['kb_date'].date()} 收盘 ${s['stopline']:.2f} "
          f"(TR {s['kb_tr']:.1f}%, {s['age']}日前) · 现价距线 {s['dist']:+.1f}% → "
          f"{'⚠ ALERT 收盘失守' if s['alert'] else 'OK 线上'}")

    win = d.tail(history)
    kbs = win[win['surge']]
    print(f"\n近{history}交易日 surge 日({len(kbs)} 根)及其线的结局:")
    rows = []
    kb_pos_all = np.where(d['surge'].values)[0]
    for pos in kb_pos_all:
        ts = d.index[pos]
        if ts not in win.index:
            continue
        nxt = kb_pos_all[kb_pos_all > pos]
        seg_end = int(nxt[0]) if len(nxt) else len(d) - 1
        seg = d.iloc[pos + 1: seg_end + 1]
        line = float(d['close'].iloc[pos])
        br = seg.index[seg['close'] < line]
        rows.append({
            'surge日': ts.date(), 'TR%': round(float(d['tr_pct'].iloc[pos]), 1),
            '收盘(线)': round(line, 2),
            '结局': (f'破线 {br[0].date()} ({d.index.get_loc(br[0]) - pos}日后)'
                    if len(br) else ('被新线替换' if len(nxt) else '存续中')),
        })
    print(tab_mod.tabulate(pd.DataFrame(rows), headers='keys',
                           tablefmt='github', showindex=False))
    print("\n解读: '破线'=该 surge 日成果曾被完全回吐(警报); '被新线替换'=趋势以新 surge "
          "日延续(健康)。破线密集=上攻屡屡被否定, 高位区出现时警惕派发。")


#### MAIN ####
def main():
    parser = OptionParser()
    parser.add_option('--ticker', dest='ticker', help='单票诊断, 如 CSCO')
    parser.add_option('--thr', dest='thr', type='float', default=THR_PCT,
                      help=f'surge 日门槛 TR/前收 ≥ 此值%% (默认 {THR_PCT})')
    parser.add_option('--any-direction', action='store_true', default=False,
                      dest='any_dir', help='surge 日不要求上涨(默认仅阳线取线)')
    parser.add_option('--history', dest='history', type='int', default=250,
                      help='单票模式回看交易日数 (默认 250)')
    parser.add_option('--asof', dest='asof', default=None,
                      help='回测某历史日 YYYY-MM-DD: 只用到那天为止的数据')
    (opt, _) = parser.parse_args()

    asof = None
    if opt.asof:
        try:
            asof = pd.Timestamp(opt.asof).normalize()
        except ValueError:
            print(f'--asof 日期无法解析: {opt.asof}'); sys.exit(1)

    up_only = not opt.any_dir
    if opt.ticker:
        run_ticker(opt.ticker.upper(), opt.thr, up_only, asof, opt.history)
    else:
        run_hold(opt.thr, up_only, asof)


if __name__ == '__main__':
    main()
