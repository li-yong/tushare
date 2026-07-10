# coding: utf-8
"""
US Intraday Internals — 持仓·分钟线派发体检 (收盘判)

实证动机 (MU 2026-06-25 衰竭日事件研究, 全文 docs/mu_1m_decline_study.md):
  MU 6/25 隔夜跳空 +17.6% 冲新高 1255 后七个交易日 -29%。分钟线在 6/25 当天
  收盘就能确认派发: 内部指标 (SVR 资金流 / VWAP 下方时间 / 早盘30分钟) 集体
  翻负 — 按收盘 1213.5 退出距顶仅 3.3%, 躲过随后 -22.6%。
  但 n=5 证伪: 日线"放量跳空衰竭"四条件在 MU 三年里触发 5 次, 2 跌 / 2 大涨
  起点 (2025-12-18 之后 21 天 +56.6%) / 1 平 — 日线 pattern 本身无方向性,
  方向由内部质地决定; 而内部指标无历史 1m 可回放 (Yahoo 仅 30 天), 是 n=1
  假说, 靠本脚本接 cron 逐日落盘 CSV 前向积累样本来检验。

两个 flag (都是收盘后的状态读数, GPS 不是讨债单):
  EXHAUSTION: 隔夜 gap ≥ +8% & 日内收跌 & 量 ≥ 1.4×10日均 & 处于20日高位
      → 双向波动事件·复核触发器 (非卖出信号!): 当晚人工复核内部质地 —
      svr/flow 翻负 = 6/25 型派发; 内部尚可 = 可能是 12/18 型 melt-up 起点。
  INTERNALS_WEAK: svr3 < 0 且 belowVWAP3 > 55% → 资金流恶化;
      反弹期若该 flag 不消失 = 无买盘诱多 (MU 6/29-30 案例)。

明确的不做 (负面实证, 见 doc):
  - 不做盘中止损: 6/25 盘中"缺口回吐50%"触发点比收盘退出差 6%。
  - 不预测隔夜跳空: 下跌大头在隔夜, 分钟线看不见; 它只识别派发状态。
  - 退出政策归 scanner (ADR-0002); 本脚本是收盘复核触发器/参考层。

数据: Futu OpenD 1m (RTH; yfinance 1m 额度不够用, 2026-07-09 切换), 每票增量
  缓存到 /home/ryan/DATA/DAY_Global/US_yf_1m/{SYM}_1m.csv, 保留 60 天。
  Futu K线 time_key 是 bar 结束时间 (09:31..16:00), 入缓存前统一 -1 分钟对齐
  yfinance 的开始时间约定 (09:30..15:59), 与旧缓存无缝衔接。OpenD 挂了 →
  用旧缓存 + 大声告警, 不换源 (ADR-0001 精神)。历史K线额度按"30天内去重股票数"
  计, 持仓级 ~18 只远在额度内; 限频 60次/30s → 每请求 sleep 0.5s。
  日线口径特征 (gap/日内/量比/20日高) 仍用 t_us_tech_swing 的 yfinance 日线
  缓存 (ADR-0001, tech_swing 主扫描已刷新, 不新增额度), 分钟缓存只算内部指标。
  无 --asof。

持仓池: select.yml US_SWING_STOPS keys ∪ US_HOLD_EXTRA (浮盈≥30% 未登记
  init 止损的那批)。--tickers 覆盖为任意名单。

Usage:
  python t_us_intraday_internals.py                 # 全持仓体检 (cron 用)
  python t_us_intraday_internals.py --tickers MU,NVDA
"""

import os
import sys
import logging
import datetime

import time

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

SELECT_YML = '/home/ryan/tushare_ryan/select.yml'
CACHE_DIR  = '/home/ryan/DATA/DAY_Global/US_yf_1m'
RESULT_DIR = '/home/ryan/DATA/result/us_intraday_internals'
TZ         = 'America/New_York'
KEEP_DAYS  = 60      # 1m 缓存保留的日历天数
FUTU_HOST, FUTU_PORT = '127.0.0.1', 11111

# ── flag 门槛 (MU 案例事后拟合, n=1 — 见 doc 局限节) ─────────────────────────
GAP_MIN      = 8.0    # EXHAUSTION: 隔夜跳空 ≥ +8%
VOL_MULT_MIN = 1.4    # EXHAUSTION: 量 ≥ 1.4×10日均 (不含当日)
NEAR_HI      = 0.99   # EXHAUSTION: 当日最高 ≥ 0.99×20日高
SVR3_NEG     = 0.0    # INTERNALS_WEAK: 3日 SVR < 0
BELOWV3_HI   = 55.0   # INTERNALS_WEAK: 3日 VWAP下方时间占比 > 55%
SVR3_REPAIR  = 5.0    # 修复线: 上涨段 svr3 典型下沿 (4-6月中位≈+6; 急跌后 1~5 天
                      # 修回此线上方=健康回调, 6/25 顶后 >10 天未修复=派发)


def load_holdings() -> list:
    import yaml
    with open(SELECT_YML) as fh:
        cfg = yaml.safe_load(fh) or {}
    stops = [str(t).upper() for t in (cfg.get('US_SWING_STOPS') or {})]
    extra = [str(t).upper() for t in (cfg.get('US_HOLD_EXTRA') or [])]
    return sorted(set(stops) | set(extra))


# ── 1m 缓存: Futu OpenD 增量拉取 + 合并 + 滚动裁剪 ───────────────────────────
def open_futu_ctx():
    """OpenD 连接; 挂了返回 None (所有票走旧缓存, 优雅降级)。
    注意: OpenQuoteContext 对不可达端口会无限重连挂死, 必须先 socket 预检。"""
    import socket
    try:
        with socket.create_connection((FUTU_HOST, FUTU_PORT), timeout=3):
            pass
        from futu import OpenQuoteContext
        return OpenQuoteContext(host=FUTU_HOST, port=FUTU_PORT)
    except Exception as e:
        logging.error(f'OpenD 连不上 ({e}) — 全部用旧 1m 缓存')
        return None


def _futu_fetch_1m(ctx, ticker: str, start: str, end: str) -> pd.DataFrame:
    """RTH 1m bars, 翻页取全。futu time_key 是 bar 结束时间 → 统一 -1min
    对齐 yfinance 开始时间约定, 与旧缓存无缝合并。限频: 每请求 sleep 0.5s。"""
    from futu import RET_OK, KLType
    frames, page_key = [], None
    while True:
        time.sleep(0.5)                       # 60次/30s 限频
        ret, data, page_key = ctx.request_history_kline(
            f'US.{ticker}', ktype=KLType.K_1M, start=start, end=end,
            extended_time=False, max_count=1000, page_req_key=page_key)
        if ret != RET_OK:
            raise RuntimeError(f'request_history_kline: {data}')
        frames.append(data)
        if page_key is None:
            break
    raw = pd.concat(frames, ignore_index=True)
    if raw.empty:
        raise ValueError('empty 1m frame')
    df = raw.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                             'close': 'Close', 'volume': 'Volume'})
    df.index = (pd.to_datetime(df['time_key']).dt.tz_localize(TZ)
                - pd.Timedelta(minutes=1))
    return df[['Open', 'High', 'Low', 'Close', 'Volume']]


def update_1m_cache(ticker: str, ctx) -> pd.DataFrame:
    """返回该票 1m 缓存 (ET 时区索引)。拉取失败 → 用旧缓存并大声告警 (ADR-0001 精神)。"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f'{ticker}_1m.csv')
    old = pd.DataFrame()
    if os.path.exists(path):
        old = pd.read_csv(path, index_col=0)
        old.index = pd.to_datetime(old.index, utc=True).tz_convert(TZ)

    today = pd.Timestamp.now(tz=TZ).strftime('%Y-%m-%d')
    if not old.empty:
        start = old.index.max().strftime('%Y-%m-%d')   # 重拉最新一天补全
    else:
        start = (pd.Timestamp.now(tz=TZ) - pd.Timedelta(days=KEEP_DAYS)).strftime('%Y-%m-%d')

    try:
        if ctx is None:
            raise RuntimeError('OpenD down')
        new = _futu_fetch_1m(ctx, ticker, start, today)
    except Exception as e:
        if old.empty:
            logging.error(f'{ticker}: 1m 拉取失败且无缓存 ({e}) — 跳过')
            return pd.DataFrame()
        logging.warning(f'{ticker}: 1m 拉取失败 ({e}) — 用旧缓存 (最新 {old.index.max()})')
        return old

    df = pd.concat([old, new])
    df = df[~df.index.duplicated(keep='last')].sort_index()
    cutoff = pd.Timestamp.now(tz=TZ) - pd.Timedelta(days=KEEP_DAYS)
    df = df[df.index >= cutoff]
    df.index.name = 'Datetime'
    df.to_csv(path)
    return df


# ── 逐日内部指标 (来自 1m) ────────────────────────────────────────────────────
def daily_internals(m1: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for d, g in m1.groupby(m1.index.date):
        g = g.sort_index()
        if len(g) < 60 or g['Volume'].sum() == 0:   # 半天/坏数据日跳过
            continue
        px, vol = g['Close'], g['Volume']
        dpx = px.diff()
        upv, dnv = vol[dpx > 0].sum(), vol[dpx < 0].sum()
        svr = (upv - dnv) / (upv + dnv) * 100 if (upv + dnv) > 0 else np.nan
        vwap = (px * vol).cumsum() / vol.cumsum()
        n30 = min(30, len(g) - 1)
        rows.append({
            'date':   pd.Timestamp(d),
            'svr':    svr,                                        # 带符号量比 %
            'belowV': (px < vwap).mean() * 100,                   # VWAP下方时间 %
            'f30':    (px.iloc[n30] / g['Open'].iloc[0] - 1) * 100,   # 早盘30min %
            'l30':    (px.iloc[-1] / px.iloc[-min(31, len(g))] - 1) * 100,  # 尾盘30min %
            'flow_B': (np.sign(dpx) * px * vol).sum() / 1e9,      # 净美元流 B$
        })
    r = pd.DataFrame(rows).set_index('date')
    if not r.empty:
        r['svr3'] = r['svr'].rolling(3).mean()
        r['belowV3'] = r['belowV'].rolling(3).mean()
    return r


# ── 单票体检: 日线口径特征 + 内部指标 + flags ─────────────────────────────────
def examine(ticker: str, ctx) -> dict | None:
    m1 = update_1m_cache(ticker, ctx)
    if m1.empty:
        return None
    internals = daily_internals(m1)
    if internals.empty:
        return None

    d = _fetch_daily(ticker)          # 复权日线 (含当日 bar, ADR-0001 缓存)
    if d.empty or len(d) < 21:
        logging.warning(f'{ticker}: 日线不足 — 跳过')
        return None
    last = d.iloc[-1]
    gap   = (last['open'] / d['close'].iloc[-2] - 1) * 100
    intra = (last['close'] / last['open'] - 1) * 100
    vol10 = d['volume'].iloc[-11:-1].mean()
    vmult = last['volume'] / vol10 if vol10 > 0 else np.nan
    hi20  = d['high'].iloc[-21:-1].max()

    it = internals.iloc[-1]
    m1_date, d_date = internals.index[-1].date(), d.index[-1].date()
    if m1_date != d_date:
        logging.warning(f'{ticker}: 1m 最新日 {m1_date} ≠ 日线最新日 {d_date} — 内部指标是旧的')

    flags = []
    if gap >= GAP_MIN and intra < 0 and vmult >= VOL_MULT_MIN and last['high'] >= NEAR_HI * hi20:
        flags.append('EXHAUSTION')
    if pd.notna(it.get('svr3')) and it['svr3'] < SVR3_NEG and it['belowV3'] > BELOWV3_HI:
        flags.append('INTERNALS_WEAK')

    # svr3 连续未修复天数: 距上一次 svr3 ≥ 修复线的交易日数 (0=已修复)。
    # 修复速度是健康回调 vs 派发的判别器 (doc 上涨段检验节); 遇 NaN(缓存头部)停数。
    norepair = 0
    for v in internals['svr3'][::-1]:
        if pd.isna(v) or v >= SVR3_REPAIR:
            break
        norepair += 1

    return {
        'ticker': ticker, 'close': round(float(last['close']), 2),
        'gap%': round(gap, 1), 'intra%': round(intra, 1),
        'vol×': round(float(vmult), 2) if pd.notna(vmult) else np.nan,
        'svr': round(float(it['svr']), 1), 'svr3': round(float(it['svr3']), 1) if pd.notna(it['svr3']) else np.nan,
        '未修复d': norepair,
        'belowV3': round(float(it['belowV3']), 0) if pd.notna(it['belowV3']) else np.nan,
        'f30%': round(float(it['f30']), 1), 'l30%': round(float(it['l30']), 1),
        'flow_B': round(float(it['flow_B']), 2),
        'flags': '+'.join(flags), '1m日': str(m1_date),
    }


def main():
    parser = OptionParser(usage=__doc__)
    parser.add_option('--tickers', dest='tickers', default=None,
                      help='逗号分隔名单, 覆盖默认持仓池')
    (opt, _) = parser.parse_args()

    tickers = ([t.strip().upper() for t in opt.tickers.split(',') if t.strip()]
               if opt.tickers else load_holdings())
    if not tickers:
        logging.error('持仓池为空 (US_SWING_STOPS / US_HOLD_EXTRA)')
        return 1
    logging.info(f'体检 {len(tickers)} 只: {" ".join(tickers)}')

    ctx = open_futu_ctx()
    rows = []
    try:
        for t in tickers:
            try:
                r = examine(t, ctx)
                if r:
                    rows.append(r)
            except Exception as e:
                logging.error(f'{t}: 体检失败 — {e}')
    finally:
        if ctx is not None:
            ctx.close()
    if not rows:
        logging.error('无任何结果')
        return 1

    df = pd.DataFrame(rows)
    df = df.sort_values('flags', ascending=False)     # 有 flag 的排前面

    today = datetime.date.today().strftime('%Y%m%d')
    os.makedirs(RESULT_DIR, exist_ok=True)
    csv_f = os.path.join(RESULT_DIR, f'us_intraday_internals_{today}.csv')
    df.to_csv(csv_f, index=False)

    print(f'\n== 持仓·分钟线派发体检 (收盘判) {today} ==')
    print('EXHAUSTION=放量跳空衰竭(双向波动事件, 当晚复核内部质地; 非卖出信号)')
    print('INTERNALS_WEAK=资金流恶化(反弹不追)')
    print('svr=带符号量比% belowV3=VWAP下方时间%(3日) f30/l30=早盘/尾盘30min% flow=净美元流B$')
    print(f'未修复d=svr3连续<+{SVR3_REPAIR:.0f}的交易日数(健康回调1~5天修回, MU 6/25顶后>10天未修复)\n')
    print(tab_mod.tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    flagged = df[df['flags'] != '']
    if not flagged.empty:
        print(f'\n⚠ 有 flag: {" ".join(flagged["ticker"])} — 见 docs/mu_1m_decline_study.md')
    else:
        print('\n全部持仓内部指标无恙。')
    logging.info(f'saved {csv_f}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
