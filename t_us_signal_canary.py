# coding: utf-8
"""
t_us_signal_canary.py — 信号金丝雀: 系统自身信号的滚动止损率 = 池内体制传感器

原始假说 (huice 2022 回测): 体制转变最先反映在"自己的单子开始连续挨止损"上,
金丝雀可做池内【早警】。

★ 验证结论 (2026-07-04, --validate 合成历史 2021-22 + 2024-26; 全文
docs/huice_backtest_findings.md §2.7): **早警假说被证伪**。按"结局揭晓日"的
实时视角, 金丝雀在 2021-11~12 (Nasdaq/SPX 顶) 读数反而是全年最低 (10%/32%) —
melt-up 让顶前的信号全是赢的; 飙升发生在下跌展开之后, 与 regime WATCH 同期。
任何"追踪自身信号近期结局"的指标都会被 melt-up 顶结构性欺骗 — 又一次印证
"没有万能早警" (docs/bull_to_bear_2021_2022.md)。
降级后的定位: 【同步的池内健康度确认】— 高读数=池子正处于消化/回撤中
(2022 全年持续高、2025-H2 平静期持续低、2025春/2026-03 回撤如实标高),
用来回答"最近的信号环境是不是变差了", 不用来提前减仓。

口径 (变体扫描后选定, 原始口径 10日止损率 噪声过大已弃):
  每个 episode: 信号日收盘入场, 21 个交易日后收盘仍低于入场 = 一次"亏损结局"
  (与止损松紧无关)。读数 = 最近 WINDOW_EP=40 个已走满 episode 的亏损率。
  分级: NORMAL <0.50 · ELEVATED ≥0.50 · ALARM ≥0.70 (平静牛里 ≥0.70 只在
  真实回撤段出现)。窗口未满的 episode 记 pending, 不进分母。

live:      读真实 ledger (us_signal_ledger.csv)。账本年轻时如实报"样本不足" —
           合成历史绝不混进 live 读数。live 是唯一无 watchlist 事后偏差的版本。
--validate: 用 huice 点位重放合成 episode 流复现上述验证 (数分钟)。

Usage:
  python t_us_signal_canary.py                     # live 读数 (cron 接在 tech_swing 后)
  python t_us_signal_canary.py --validate          # 合成历史验证 (复现证伪过程)
  python t_us_signal_canary.py --window 60 --horizon 21
"""

import os
import sys
import logging
import datetime

import numpy as np
import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

import t_us_tech_swing as tsw
from signal_ledger import LEDGER_PATH

OUT_DIR    = '/home/ryan/DATA/result/us_signal_log'
WINDOW_EP  = 40     # 滚动窗口: 最近 N 个已走满的 episode (变体扫描: 40-60 最稳)
HORIZON_D  = 21     # 结局观察窗: 信号日后 N 个交易日的收盘 vs 入场 (亏损率口径)
THR_ELEV   = 0.50   # ≥ → ELEVATED
THR_ALARM  = 0.70   # ≥ → ALARM (平静牛里只在真实回撤段出现)

# --validate 的参照事件 (docs/bull_to_bear_2021_2022.md §1 + regime --backtest)
REF_EVENTS = [
    ('2021-11-19', 'Nasdaq 综指顶'),
    ('2022-01-03', 'SPX 收盘历史顶'),
    ('2022-01-26', 'regime WATCH 首触发 (live 工具)'),
    ('2022-05-11', 'regime DEFEND 段起点'),
    ('2025-02-19', '2025 春回撤起点附近'),
    ('2025-04-07', '2025 春低点'),
]


def loss_outcome(ticker: str, date: pd.Timestamp, stop,
                 horizon: int) -> 'bool | None':
    """信号日收盘入场, horizon 个交易日后收盘是否低于入场 (亏损结局)。
    与止损松紧无关 — 变体扫描显示止损率口径被 stop 紧度主导, 判别力差。
    窗口未走满 → None (pending)。stop 参数保留签名兼容, 不参与判定。"""
    daily = tsw._fetch_daily_full(ticker)
    if daily.empty:
        return None
    pos = daily.index.searchsorted(pd.Timestamp(date))
    if pos >= len(daily) or pos + horizon >= len(daily):
        return None
    return bool(float(daily['close'].iloc[pos + horizon])
                < float(daily['close'].iloc[pos]))


def canary_frame(episodes: pd.DataFrame, window: int, horizon: int) -> pd.DataFrame:
    """episodes: 列 date/ticker/stop (+任意)。返回按 date 排序的帧, 追加
    hit (True/False/NaN) 与 canary (最近 window 个已走满 episode 的止损率)。"""
    df = episodes.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    df['hit'] = [loss_outcome(r['ticker'], r['date'], r.get('stop'), horizon)
                 for _, r in df.iterrows()]
    known = df['hit'].map(lambda v: np.nan if v is None else float(v))
    # 只在已走满的 episode 上滚动; pending 行沿用上一读数 (读数只基于已知结局)
    df['canary'] = (known.dropna().rolling(window, min_periods=window).mean()
                    .reindex(df.index).ffill())
    return df


def _level(v) -> str:
    if pd.isna(v):
        return '—'
    if v >= THR_ALARM:
        return 'ALARM'
    if v >= THR_ELEV:
        return 'ELEVATED'
    return 'NORMAL'


# ── live 模式 ─────────────────────────────────────────────────────────────────
def run_live(window: int, horizon: int):
    if not os.path.exists(LEDGER_PATH):
        print('无 ledger — 金丝雀等 live 扫描积累 episode 后才有读数。')
        return
    led = pd.read_csv(LEDGER_PATH)
    if led.empty:
        print('ledger 为空 — 无读数。')
        return
    led['stop'] = pd.to_numeric(led['stop'], errors='coerce')
    eps = led.rename(columns={'first_seen': 'date'})[
        ['date', 'ticker', 'stop', 'source', 'signal_type']]
    df = canary_frame(eps, window, horizon)
    n_done = int(df['hit'].map(lambda v: v is not None).sum())
    n_pend = len(df) - n_done
    latest = df['canary'].iloc[-1] if len(df) else np.nan

    lines = []
    P = lines.append
    P('=' * 70)
    P(f'信号金丝雀 (ledger 滚动亏损率 · 同步池内健康度)   {datetime.date.today()}')
    P('=' * 70)
    P(f'  口径: 最近 {window} 个已走满 episode 中, {horizon} 个交易日后收盘仍低于入场的比例')
    P(f'  episodes: {len(df)} 总 · {n_done} 已走满 · {n_pend} pending (窗口未满)')
    if n_done < window:
        P(f'  读数: 样本不足 (已走满 {n_done} < 窗口 {window}) — 账本还年轻, 如实等。')
        if n_done >= 5:
            part = df['hit'].dropna() if df['hit'].dtype != object else \
                df['hit'].map(lambda v: np.nan if v is None else float(v)).dropna()
            P(f'  参考 (全部 {n_done} 个已走满): 止损率 {part.mean():.0%} — 仅参考, 别当读数')
    else:
        P(f'  读数: {latest:.0%}  →  【{_level(latest)}】'
          f'  (NORMAL <{THR_ELEV:.0%} · ELEVATED ≥{THR_ELEV:.0%} · ALARM ≥{THR_ALARM:.0%})')
        P(f'  含义: 池内健康度的【同步】确认 — 高读数=近期信号环境已在恶化中。')
        P(f'  ⚠ 不是早警: melt-up 顶前读数反而最低 (验证已证伪早警假说, 见脚本头注)')
    P('=' * 70)
    out = '\n'.join(lines)
    print(out)
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR,
                        f'us_signal_canary_{datetime.date.today().strftime("%Y%m%d")}.txt')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(out + '\n')
    logging.info(f'金丝雀报告 → {path}')


# ── --validate: huice 合成历史 ────────────────────────────────────────────────
def _synthesize(start: str, end: str, jobs: int = 8) -> pd.DataFrame:
    """用 huice 点位重放 watchlist, 还原 tech_swing+key_kline 的 episode 流。
    关闭 MIXED 门控 (还原门控前系统会发出的信号)。"""
    import multiprocessing as mp
    import huice
    tsw.MIXED_NO_NEW_ENTRY = False
    tickers = list(tsw.UNIVERSE)
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    huice._ensure_deep_history(
        list(dict.fromkeys(tickers + tsw.BAROMETERS + tsw.MAG7 + ['QQQ'])),
        s - pd.Timedelta(days=huice.WARMUP_D))
    days = huice._trading_days(s, e)
    logging.info(f'合成 {start}→{end}: {len(tickers)} 票 × {len(days)} 日, '
                 f'gate 预计算 …')
    for d in days:
        huice.gate_state_on(d)
    huice._SWEEP_DAYS, huice._SWEEP_SOURCE = days, 'both'
    eps = []
    with mp.Pool(jobs) as pool:
        for t, got, err in pool.imap_unordered(huice._sweep_worker, tickers, chunksize=2):
            if err:
                logging.warning(f'{t}: {err}')
            eps.extend(got)
    tsw._ASOF = None
    logging.info(f'  {len(eps)} episode(s)')
    return pd.DataFrame([{'date': e_['date'], 'ticker': e_['ticker'],
                          'stop': e_['stop'], 'source': e_['source']} for e_ in eps])


def run_validate(window: int, horizon: int):
    lines = []
    P = lines.append
    P('=' * 78)
    P(f'信号金丝雀 · 合成历史验证   window={window} ep · horizon={horizon}d (亏损率口径)')
    P(f'  合成流 = huice 重放 watchlist (tech_swing+key_kline, MIXED 门控关);')
    P(f'  时间轴按【结局揭晓日】(信号日+~{horizon}交易日) — 实时视角, 无回看粉饰')
    P(f'  ⚠ watchlist 是今天的名单 (事后偏差), 但金丝雀本来就测自家池子')
    P('=' * 78)

    for tag, start, end in [('2021-22 牛转熊', '2021-01-01', '2022-12-31'),
                            ('2024-26 平静牛+春震', '2024-07-01', '2026-06-30')]:
        eps = _synthesize(start, end)
        if eps.empty:
            P(f'\n[{tag}] 无 episode'); continue
        df = canary_frame(eps, window, horizon)
        # 实时视角: 该 episode 的结局在 ~horizon 个交易日后才可知
        df['known'] = df['date'] + pd.Timedelta(days=int(horizon * 1.45))
        P('')
        P(f'[ {tag} ]  episodes {len(df)}')
        m = df.dropna(subset=['canary']).set_index('known')
        monthly = m['canary'].resample('ME').agg(['last', 'max', 'count'])
        rows = [[i.strftime('%Y-%m'), f'{r["last"]:.0%}', f'{r["max"]:.0%}',
                 int(r['count']), _level(r['last'])]
                for i, r in monthly.iterrows() if r['count'] > 0]
        P(tab_mod.tabulate(rows, headers=['month(揭晓)', '月末读数', '月内峰值', 'ep数', '级别'],
                           tablefmt='simple'))
        for thr in (THR_ELEV, THR_ALARM):
            above = m['canary'][m['canary'] >= thr]
            P(f'  首次 ≥{thr:.0%}: '
              + (str(above.index[0].date()) if len(above) else '从未'))
        P(f'  读数分布: p50 {m["canary"].median():.0%} · p80 '
          f'{m["canary"].quantile(.8):.0%} · p95 {m["canary"].quantile(.95):.0%}')

    P('')
    P('[ 参照事件 ]')
    for d, name in REF_EVENTS:
        P(f'  {d}  {name}')
    P('')
    P('[ 验证结论 (2026-07-04) ]')
    P('  · 早警假说【证伪】: 2021-11~12 (指数顶) 读数为全年最低 — melt-up 让顶前')
    P('    信号全是赢的, 任何追踪自身信号近期结局的指标都被这种顶结构性欺骗。')
    P('  · 有效用途 = 同步的池内健康度确认: 2022 全年持续高、平静牛持续低、')
    P('    真实回撤段 (2025春/2026-03) 如实标高。别据此提前减仓。')
    P('  · 原始口径 (10日止损率, W=20) 噪声更大已弃 — 被止损紧度主导, 平静牛')
    P('    月月假 ALARM。变体扫描: 亏损率 21d/W40-60 判别力最好 (Δ≈+35~40pp)。')
    P('=' * 78)
    out = '\n'.join(lines)
    print(out)
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, 'us_signal_canary_validate.txt')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(out + '\n')
    logging.info(f'验证报告 → {path}')


def main():
    parser = OptionParser()
    parser.add_option('--validate', dest='validate', action='store_true', default=False)
    parser.add_option('--window', dest='window', type='int', default=WINDOW_EP)
    parser.add_option('--horizon', dest='horizon', type='int', default=HORIZON_D)
    opt, _ = parser.parse_args()
    if opt.validate:
        run_validate(opt.window, opt.horizon)
    else:
        run_live(opt.window, opt.horizon)


if __name__ == '__main__':
    main()
