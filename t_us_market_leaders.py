# coding: utf-8
"""
US Market Leaders — 市场领跑股 (个股层面涨幅榜持续性, 板块轮动的个股配对)

t_us_sector_rotation 回答"钱往哪个板块流", 但个股层面一直没人管——每天涨幅榜前列
一堆名字, 到底哪些是真在【持续领跑】, 哪些只是"高波动乱窜"? 单日涨幅榜测不出区
别: 2025年11月 MU/SNDK 曾在 SP500∪NDX 池子里反复冲进 top-10, 但同一周也反复跌进
bottom-10(SNDK 11-13→11-20 五个交易日里三天在 bottom-3、两天在 top-3), 三周后
两票都遭遇单日 -10%~-20% 闪崩——纯涨幅榜会把这种两边打脸的震荡态误判成"领跑"。

本脚本的确认方法(2026-06 实证, 见对话/scratchpad): 用【净排名持续性】而非单日榜单——
trailing WINDOW 个交易日内, 在 SP500∪NDX 池子里按当日涨幅排名, 统计:
    top_n  = 挤进 TAIL_RANK 名(涨幅前列) 的天数
    bot_n  = 跌进 TAIL_RANK 名(跌幅前列) 的天数
触发 = bot_n ≤ BOT_TOLERANCE(允许 1 天噪声) 且 top_n ≥ TOP_MIN。用固定名次(非池子
百分位)——池子规模稳定在 500 出头, 不必为百分位再引入一层换算。

用 MU/SNDK 12月-1月数据验证: 该组合能把 10-11月的两边打脸期和12月起的真实领跑段
分开(MU 12-19→01-12 连续 15 个交易日触发, 紧跟 12-18 的 +10.2% 突破日次日确认)。
已知假阳性: 5日窗口仍短到会被两周内的孤立暴涨骗到——11月那两段(MU 11-05→11-11,
SNDK 11-03→11-17)都触发过, 随后就是闪崩。这是当前参数下的已知代价, 不是 bug;
量能门槛(领跑日 vol_ratio≥1.3× 可挡掉大半假阳性)是下一步的候选加固, 暂未启用。

指标测当下状态, 不赌方向(守 docs/indicator_design_state_vs_debt): 只回答"现在谁在
持续领跑", 不承诺趋势延续, 也不判断买卖点——环境/确认读, 不接 us_daily_report 的
BUY/SELL 归并(同 sector_rotation / breadth_diffusion 的定位), 只作原文附录。

数据: 复用 t_us_tech_swing 的 yfinance 缓存(_fetch_daily, ADR-0001)。yfinance-only,
不依赖 Futu/OpenD。

两个模式(对齐 t_us_gap_scan / t_us_pullback_shock):
  --scan [--universe both|sp500|ndx] [--top N]
      全市场扫当前处于"触发态"的领跑股, 按 top5d 排序。
  --ticker SYM
      单票诊断: 近期排名轨迹 + top/bot 计数 + 当前连续触发天数。

  --asof YYYY-MM-DD  (两模式通用)
      回测开关: 只用到那天为止的数据。

Usage:
  python t_us_market_leaders.py --scan --universe both
  python t_us_market_leaders.py --ticker MU
  python t_us_market_leaders.py --scan --asof 2026-01-10
"""

import os
import sys
import json
import logging
import datetime

import numpy as np
import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

import t_us_tech_swing as _sw

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR   = '/home/ryan/DATA/result'
UNIVERSE_DIR = '/home/ryan/DATA/DAY_Global/US_universe'

# ── 门槛(2026-06 讨论定案) ────────────────────────────────────────────────────
WINDOW       = 5     # trailing 窗口(交易日) —— 选"更及时"
TAIL_RANK    = 20    # 涨幅/跌幅前列的固定名次(非百分位) —— 选"更简单"
BOT_TOLERANCE = 1    # 窗口内允许的 bottom 命中噪声次数
TOP_MIN      = 2     # 窗口内 top 命中次数下限(触发门槛, 未单独讨论, 按回测经验定)
LOOKBACK_ROWS = 90   # 拉取的原始收盘价行数(≈4个月), 够算窗口+连续触发天数的缓冲


def _load_universe(name: str) -> list:
    path = os.path.join(UNIVERSE_DIR, f'{name}.json')
    with open(path) as f:
        return json.load(f)


def _build_rank_matrix(tickers, asof):
    """拉全池收盘价, 对齐成矩阵, 逐日横截面排名; 返回 (top_flag, bot_flag, ret, close) 四个同形 DataFrame。"""
    prev = _sw._ASOF
    try:
        if asof is not None:
            _sw._ASOF = asof
        closes = {}
        for t in tickers:
            try:
                df = _sw._fetch_daily(t)
            except Exception as e:
                logging.warning(f'{t}: 拉取失败 ({e})')
                continue
            if df.empty:
                continue
            closes[t] = df['close'].tail(LOOKBACK_ROWS)
    finally:
        _sw._ASOF = prev

    mat = pd.DataFrame(closes)
    ret = mat.pct_change(fill_method=None) * 100.0
    ranks = ret.rank(axis=1, ascending=False, method='min')
    n = ret.count(axis=1)
    top_flag = ranks.le(TAIL_RANK)
    bot_flag = ranks.ge(n.sub(TAIL_RANK - 1), axis=0)
    return top_flag, bot_flag, ret, mat, ranks


def _streak(trig: pd.Series) -> int:
    """连续触发天数(从最新一天往回数, 遇到 False 即停)。"""
    s = 0
    for v in reversed(trig.tolist()):
        if bool(v):
            s += 1
        else:
            break
    return s


# ── 模式一: 全市场扫当前触发的领跑股 ──────────────────────────────────────────
def run_scan(universe: str, top: int, asof=None):
    tickers = _load_universe(universe)
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    logging.info(f'scan universe={universe} ({len(tickers)} 只), '
                 f'window={WINDOW}日/固定名次{TAIL_RANK}/允许噪声{BOT_TOLERANCE}次{asof_txt}')

    top_flag, bot_flag, ret, mat, ranks = _build_rank_matrix(tickers, asof)
    if top_flag.empty or len(top_flag) < WINDOW + 1:
        print('数据不足, 无法计算。')
        return

    top5d = top_flag.rolling(WINDOW).sum()
    bot5d = bot_flag.rolling(WINDOW).sum()
    trig = (bot5d <= BOT_TOLERANCE) & (top5d >= TOP_MIN)

    last_date = mat.index[-1]
    rows = []
    for t in mat.columns:
        if not bool(trig[t].iloc[-1]):
            continue
        r = ranks[t].loc[last_date]
        rows.append({
            'ticker':  t,
            'close':   round(float(mat[t].iloc[-1]), 2),
            'ret%':    round(float(ret[t].iloc[-1]), 2) if pd.notna(ret[t].iloc[-1]) else np.nan,
            'rank':    int(r) if pd.notna(r) else np.nan,
            'top5d':   int(top5d[t].iloc[-1]),
            'bot5d':   int(bot5d[t].iloc[-1]),
            'streak':  _streak(trig[t]),
        })

    if not rows:
        print(f'无命中: 当前无票满足 trailing{WINDOW}日 top{TAIL_RANK}≥{TOP_MIN} '
              f'且 bot{TAIL_RANK}≤{BOT_TOLERANCE} 次。')
        return

    out = pd.DataFrame(rows).sort_values(
        ['top5d', 'streak', 'bot5d'], ascending=[False, False, True]
    ).reset_index(drop=True)
    show = out.head(top)
    print(f"\n市场领跑股 · 净排名持续性  (universe={universe}{asof_txt}, "
          f"池 {len(mat.columns)} 只, 命中 {len(out)})")
    print(tab_mod.tabulate(show, headers='keys', tablefmt='github', showindex=False))
    print(f"\n定义: trailing {WINDOW} 日内, 在全池(SP500∪NDX)按当日涨幅排名固定第 "
          f"{TAIL_RANK} 名以内记 top、倒数第 {TAIL_RANK} 名以内记 bot; 触发 = "
          f"top≥{TOP_MIN} 且 bot≤{BOT_TOLERANCE}。streak=连续触发交易日数。")
    print("已知假阳性: 5日窗口仍会被两周内的孤立暴涨骗到(11月 MU/SNDK 案例, 触发后"
          "即闪崩) —— 测的是当下有没有持续挤进涨幅榜前列, 不承诺趋势延续, 也不是"
          "买卖点; 与 sector_rotation/breadth_diffusion 同属环境确认读。")

    tag = asof.strftime('%Y%m%d') if asof is not None else datetime.date.today().strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_market_leaders')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_market_leaders_{universe}_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


# ── 模式二: 单票诊断 ──────────────────────────────────────────────────────────
def run_ticker(ticker: str, universe: str, asof=None):
    tickers = _load_universe(universe)
    if ticker not in tickers:
        tickers = tickers + [ticker]
    top_flag, bot_flag, ret, mat, ranks = _build_rank_matrix(tickers, asof)
    if ticker not in mat.columns:
        logging.error(f'{ticker}: 无数据'); return

    top5d = top_flag.rolling(WINDOW).sum()
    bot5d = bot_flag.rolling(WINDOW).sum()
    trig = (bot5d <= BOT_TOLERANCE) & (top5d >= TOP_MIN)

    asof_txt = f' (as-of {mat.index[-1].date()})' if asof is not None else ''
    print(f"\n{ticker} 领跑轨迹{asof_txt}  收 ${mat[ticker].iloc[-1]:.2f}  "
          f"池 {len(mat.columns)} 只(universe={universe})")
    is_trig = bool(trig[ticker].iloc[-1])
    print(f"  当前: {'★ 触发(持续领跑)' if is_trig else '未触发'} · "
          f"连续 {_streak(trig[ticker])} 个交易日 · "
          f"trailing{WINDOW}日 top{TAIL_RANK}={int(top5d[ticker].iloc[-1])} 次 · "
          f"bot{TAIL_RANK}={int(bot5d[ticker].iloc[-1])} 次")

    tail_n = min(20, len(mat) - 1)
    rows = []
    for d in mat.index[-tail_n:]:
        r = ranks[ticker].loc[d]
        rows.append({
            'date':  d.date(),
            'ret%':  round(float(ret[ticker].loc[d]), 2) if pd.notna(ret[ticker].loc[d]) else np.nan,
            'rank':  int(r) if pd.notna(r) else np.nan,
            'top':   '✓' if bool(top_flag[ticker].loc[d]) else '',
            'bot':   '✓' if bool(bot_flag[ticker].loc[d]) else '',
            'top5d': int(top5d[ticker].loc[d]) if pd.notna(top5d[ticker].loc[d]) else np.nan,
            'bot5d': int(bot5d[ticker].loc[d]) if pd.notna(bot5d[ticker].loc[d]) else np.nan,
        })
    print(f"\n近 {tail_n} 个交易日排名轨迹:")
    print(tab_mod.tabulate(pd.DataFrame(rows), headers='keys', tablefmt='github', showindex=False))


#### MAIN ####
def main():
    parser = OptionParser()
    parser.add_option('--scan', action='store_true', default=False, dest='scan',
                      help='全市场扫当前处于"持续领跑"触发态的票')
    parser.add_option('--ticker', dest='ticker', help='单票诊断, 如 MU')
    parser.add_option('--universe', dest='universe', default='both',
                      help='both|sp500|ndx (默认 both)')
    parser.add_option('--top', dest='top', type='int', default=40,
                      help='显示前 N(配 --scan, 默认 40)')
    parser.add_option('--asof', dest='asof', default=None,
                      help='回测某历史日 YYYY-MM-DD: 只用到那天为止的数据(两模式通用)')
    (opt, _) = parser.parse_args()

    asof = None
    if opt.asof:
        try:
            asof = pd.Timestamp(opt.asof).normalize()
        except ValueError:
            print(f'--asof 日期无法解析: {opt.asof}'); sys.exit(1)

    if opt.universe not in ('both', 'sp500', 'ndx'):
        print('--universe 仅支持 both|sp500|ndx'); sys.exit(1)

    if opt.ticker:
        run_ticker(opt.ticker.upper(), opt.universe, asof)
    elif opt.scan:
        run_scan(opt.universe, opt.top, asof)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
