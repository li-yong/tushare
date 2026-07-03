# coding: utf-8
"""
US Regime Monitor — 牛转熊择时监控 (市场体制, 非选股)

回答一个问题: 现在该不该减仓/防守? 它给【市场状态】一个判据, 不挑个股
(选股交给 tech_swing / key_kline / gap_scan)。心法与三周期回测见
docs/bull_to_bear_2021_2022.md。

把 2021-22 牛转熊拆解 + 用 2018(渐进顶)/2020(外生闪崩) 回测后, 结论很硬:
  · "既早、又稳、又全天候"的单一牛转熊信号【不存在】。
  · 领先内部恶化(宽度/信用/领导力背离)在 2022 型【慢顶】里能提前约 1 个月,
    但对 2018 型快跌【太晚】、对 2020 型外生闪崩【完全无效】, 阈值调低又会在
    2017/2018/2019 反复假报警 —— 它过拟合 2022。
  · 真正稳健的脊梁是最朴素的 200 日均线破位 + 掉头: 三段崩盘全部先于低点触发
    (+56 / +12 / +109 天)、平静年几乎无真假信号; 代价是【永远晚于精确的顶】。

故本监控给【分层】判据, 不给单一神奇信号 (守 docs/indicator_design_state_vs_debt:
指标测当下状态, 不赌反向):

  主防守层 (全天候, 非选项)  DEFEND
      SPY 收盘跌破 200 日线 且 200 日线掉头向下 → 降敞口 / 对冲。
      稳健但滞后, 三种 regime 都管用。

  早减仓层 (仅慢顶有效, 加分项)  WATCH
      领先共振 ≥4 类: 宽度背离 · 净新高转负 · 信用走弱 · 防御抢筹 ·
      成长/半导体跑输 · VIX 体制上移。在 2022 型慢熊里多抢 ~1 个月;
      明确接受它对快跌/外生崩盘无效、偶有假信号 —— 只用来"减", 绝不单独触发。

  留意层  CAUTION
      仅满足其一(已破 200 线 或 共振 ≥3) —— 提高警惕, 暂不动手。

  否则  BULL。

数据: 守 ADR-0001 纯 yfinance, 不依赖 Futu/OpenD。宽度用 S&P 500 ∪ Nasdaq-100 全体。
  · live(不带 --asof): 复用 t_us_tech_swing 的共享 3 年缓存(_fetch_daily, US_yf),
    每日 cron 已填好 → 快, 零额外成本。
  · --asof 回测: 自动改用【深历史缓存】(US_yf_deep, 自 2013), 能复盘 2018/2020/2022。
    历史 bar 不变, 一次拉取后长期复用; 首次回测会拉 ~515 只 × 12 年, 需数分钟。
    注: 宽度篮子是【当下】的成分股, 回测早年有幸存者偏差(详见 docs 局限)。

Usage:
  python t_us_regime_monitor.py                      # 当日体制快照 → 报告
  python t_us_regime_monitor.py --asof 2022-01-03    # 历史复盘(深缓存, 复盘牛转熊)
  python t_us_regime_monitor.py --asof 2018-12-24    # 复盘 2018 Q4 底
  python t_us_regime_monitor.py --history 30         # 末尾附 N 日体制轨迹
"""

import os
import sys
import logging
import datetime

import numpy as np
import pandas as pd
import tabulate as tab_mod
from optparse import OptionParser

import yfinance as yf

import t_us_tech_swing as _sw
from t_us_tech_swing import _fetch_daily

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR   = '/home/ryan/DATA/result'
UNIVERSE_DIR = '/home/ryan/DATA/DAY_Global/US_universe'

# 深历史缓存: 独立于 live 的 3 年 US_yf 缓存, 专给 --asof 回测 (能复盘 2018/2022)。
# live(不带 --asof)仍走共享 3 年缓存, 不增加每日 cron 成本。历史 bar 不变, 一次
# 拉取后长期复用; 仅当缓存未覆盖到 asof 时才重拉。首次回测会拉 ~515 只 × 12 年,
# 需数分钟。DEEP_START 取 2013, 给 2018 留足 200 日均线热身, 并覆盖 2020/2022。
DEEP_CACHE_DIR = '/home/ryan/DATA/DAY_Global/US_yf_deep'
DEEP_START     = '2013-01-01'

# ── 监控用到的非个股标的 (晴雨表/宏观/行业) ──────────────────────────────────
BAROMETER = 'SPY'                       # 主趋势载体
LEADERS   = ['QQQ', 'SOXX']            # 成长/半导体相对强弱
MACRO     = ['^VIX', 'HYG', 'LQD']     # 波动率 / 高收益信用 / 投资级信用
SEC_DEF   = ['XLU', 'XLP', 'XLV']      # 防御板块
SEC_CYC   = ['XLK', 'XLY', 'XLC']      # 周期/成长板块

# ── 判据门槛 (都做成常量, 与回测 regime_lib 一致) ─────────────────────────────
CONF_WATCH    = 4     # 早减仓层: 领先共振 ≥ 此值
CONF_CAUTION  = 3     # 留意层: 共振 ≥ 此值 (或已破 200 线)
SUSTAIN       = 3     # "持续"定义: 连续 N 个交易日成立 (压住单日抖动)
DIVERG_DROP   = 12    # 宽度背离: %>200日 较 60 日前下滑超过此点数
NEAR_HIGH     = 0.98  # 宽度背离: SPY ≥ 60 日高 × 此值 视作"仍在高位"
VIX_REGIME    = 20    # VIX 20 日均上穿此值且抬头 = 波动率体制上移


def _load_universe(name: str) -> list:
    import json
    path = os.path.join(UNIVERSE_DIR, f'{name}.json')
    with open(path, encoding='UTF-8') as f:
        return json.load(f)


def _sma(s, n):
    return s.rolling(n).mean()


def _deep_fetch(ticker: str, asof: 'pd.Timestamp | None' = None) -> pd.DataFrame:
    """深历史日线 (yfinance, 自 DEEP_START), 缓存在 US_yf_deep。历史 bar 不变 →
    缓存存在且已覆盖到 asof 即复用, 否则(缺失或未覆盖)整段重拉。失败返回空表。"""
    path = os.path.join(DEEP_CACHE_DIR, f'{ticker}.csv')
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, parse_dates=['date']).set_index('date')
            if not df.empty and (asof is None or df.index.max() >= asof):
                return df
        except Exception:
            pass
    try:
        raw = yf.Ticker(ticker).history(start=DEEP_START, auto_adjust=True)
        if raw.empty:
            return pd.DataFrame()
        df = raw.rename(columns={
            'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume',
        })[['open', 'high', 'low', 'close', 'volume']].copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = 'date'
        df = df.dropna(subset=['close'])
        os.makedirs(DEEP_CACHE_DIR, exist_ok=True)
        df.reset_index().to_csv(path, index=False)
        return df
    except Exception as e:
        logging.warning(f'{ticker}: deep fetch 失败 ({e})')
        return pd.DataFrame()


def _close(ticker: str, cal: pd.Index, fetch) -> pd.Series:
    """对齐到 SPY 交易日历的收盘序列 (缺失 ffill)。失败返回全 NaN。"""
    try:
        d = fetch(ticker)
        if d is None or d.empty:
            return pd.Series(np.nan, index=cal)
        return d['close'].reindex(cal).ffill()
    except Exception as e:
        logging.warning(f'{ticker}: 取数失败 ({e}) — 跳过')
        return pd.Series(np.nan, index=cal)


def build_panel(asof: 'pd.Timestamp | None' = None, deep: bool = False) -> pd.DataFrame:
    """构造体制指标面板, 以 SPY 交易日历为索引。
    live 走共享 3 年缓存(_fetch_daily); deep=True(--asof 回测)走 US_yf_deep 深缓存,
    并把日历截到 ≤ asof, 杜绝未来数据泄漏。"""
    fetch = (lambda t: _deep_fetch(t, asof)) if deep else _fetch_daily
    spy_df = fetch(BAROMETER)
    if spy_df is None or spy_df.empty:
        raise RuntimeError(f'{BAROMETER} 无数据, 无法判定体制')
    cal = spy_df.index
    if asof is not None:                    # point-in-time: 只保留 asof 当天及以前
        cal = cal[cal <= asof]
    spc = spy_df['close'].reindex(cal)

    # 行业/宏观/晴雨表
    qqc = _close('QQQ', cal, fetch)
    soxx = _close('SOXX', cal, fetch)
    vix = _close('^VIX', cal, fetch)
    hyg = _close('HYG', cal, fetch)
    lqd = _close('LQD', cal, fetch)
    defn = pd.concat([_close(s, cal, fetch) for s in SEC_DEF], axis=1).mean(axis=1)
    cyc  = pd.concat([_close(s, cal, fetch) for s in SEC_CYC], axis=1).mean(axis=1)

    # 宽度: S&P 500 ∪ Nasdaq-100 全体 (复用共享缓存)
    try:
        uni = _load_universe('both')
    except Exception:
        uni = []
    closes = {}
    if deep:
        logging.info(f'深缓存宽度取数: {len(uni)} 只 (首次回测会拉数分钟, 之后复用)')
    for i, t in enumerate(uni, 1):
        s = _close(t, cal, fetch)
        if s.notna().sum() >= 200:
            closes[t] = s
        if deep and i % 100 == 0:
            logging.info(f'  ...宽度取数 {i}/{len(uni)}')
    BC = pd.DataFrame(closes) if closes else pd.DataFrame(index=cal)
    n_breadth = BC.shape[1]

    ind = pd.DataFrame(index=cal)

    # ── 趋势层 (防守脊梁) ──
    s200, s50 = _sma(spc, 200), _sma(spc, 50)
    ind['below_200dma']   = spc < s200
    ind['200dma_falling'] = s200 < s200.shift(20)
    ind['death_cross']    = s50 < s200
    ind['trend_break']    = ind['below_200dma'] & ind['200dma_falling']

    # ── 领先共振层 (早减仓, 6 类) ──
    if n_breadth:
        a200 = (BC > BC.rolling(200).mean()).mean(axis=1) * 100
        hi = BC.rolling(252).max(); lo = BC.rolling(252).min()
        nnh = ((BC >= hi * 0.999).mean(axis=1) - (BC <= lo * 1.001).mean(axis=1)) * 100
        near_hi = spc >= spc.rolling(60).max() * NEAR_HIGH
        c_div = near_hi & ((a200.shift(60) - a200) > DIVERG_DROP)
        c_nnh = nnh.rolling(5).mean() < 0
    else:
        a200 = pd.Series(np.nan, index=cal)
        c_div = pd.Series(False, index=cal)
        c_nnh = pd.Series(False, index=cal)
    ind['pct_above_200'] = a200

    hl = hyg / lqd
    c_credit = ((hl < _sma(hl, 50)) & (hl < hl.shift(20))) | (hyg < _sma(hyg, 200))
    dc_ratio = defn / cyc
    c_defn = (dc_ratio > _sma(dc_ratio, 50)) & (dc_ratio > dc_ratio.shift(20))
    sr = soxx / spc; qr = qqc / spc
    c_lead = (((sr < _sma(sr, 50)) & (sr < sr.shift(20))) |
              ((qr < _sma(qr, 50)) & (qr < qr.shift(20))))
    c_vix = (_sma(vix, 20) > VIX_REGIME) & (_sma(vix, 20) > _sma(vix, 20).shift(10))

    ind['cat_breadth_divergence'] = c_div.fillna(False)
    ind['cat_net_new_highs_neg']  = c_nnh.fillna(False)
    ind['cat_credit_weak']        = c_credit.fillna(False)
    ind['cat_defensive_bid']      = c_defn.fillna(False)
    ind['cat_growth_semis_lag']   = c_lead.fillna(False)
    ind['cat_vix_regime']         = c_vix.fillna(False)
    cats = ['cat_breadth_divergence', 'cat_net_new_highs_neg', 'cat_credit_weak',
            'cat_defensive_bid', 'cat_growth_semis_lag', 'cat_vix_regime']
    ind['confluence'] = ind[cats].sum(axis=1)

    ind['SPY'] = spc
    ind['VIX'] = vix
    ind.attrs['n_breadth'] = n_breadth
    ind.attrs['cats'] = cats
    return ind


def _sustained(bool_series: pd.Series, asof_loc: int, n: int) -> bool:
    """asof_loc 当日(含)往回连续 n 日是否都为真。"""
    if asof_loc + 1 < n:
        return False
    window = bool_series.iloc[asof_loc - n + 1: asof_loc + 1]
    return bool(window.all())


def classify(ind: pd.DataFrame, asof_loc: int):
    """返回 (state, reasons[]) —— 分层判据。"""
    row = ind.iloc[asof_loc]
    conf = int(row['confluence'])
    below = bool(row['below_200dma'])
    defend = _sustained(ind['trend_break'], asof_loc, SUSTAIN)
    watch  = _sustained(ind['confluence'] >= CONF_WATCH, asof_loc, SUSTAIN)
    reasons = []
    if defend:
        state = 'DEFEND'
        reasons.append(f'SPY 收盘跌破 200 日线 且 200 日线掉头 (持续≥{SUSTAIN}日) — 全天候防守触发')
    elif watch:
        state = 'WATCH'
        reasons.append(f'领先共振 = {conf}/6 ≥ {CONF_WATCH} (持续≥{SUSTAIN}日) — 慢顶早减仓信号(对快跌/外生崩盘无效)')
    elif below or conf >= CONF_CAUTION:
        state = 'CAUTION'
        if below:    reasons.append('SPY 已在 200 日线下 (尚未确认掉头)')
        if conf >= CONF_CAUTION: reasons.append(f'领先共振 = {conf}/6 ≥ {CONF_CAUTION} (尚未到减仓阈值)')
    else:
        state = 'BULL'
        reasons.append('趋势完好, 内部恶化未达共振阈值')
    return state, reasons


CAT_CN = {
    'cat_breadth_divergence': '宽度背离(指数高位/参与度下滑)',
    'cat_net_new_highs_neg':  '净新高转负(新高股<新低股)',
    'cat_credit_weak':        '信用走弱(HYG/LQD 或 HYG<200线)',
    'cat_defensive_bid':      '防御抢筹(公用/必需/医疗 跑赢)',
    'cat_growth_semis_lag':   '成长/半导体跑输 SPY',
    'cat_vix_regime':         'VIX 体制上移(20 日均>20 且抬头)',
}
STATE_CN = {'DEFEND': '防守(降敞口/对冲)', 'WATCH': '早减仓',
            'CAUTION': '留意', 'BULL': '多头'}


def run(asof, history):
    deep = asof is not None          # --asof 回测走深历史缓存, live 走共享 3 年缓存
    if asof is not None:
        _sw._ASOF = asof
    ind = build_panel(asof=asof, deep=deep)
    cal = ind.index
    anchor = asof if asof is not None else cal[-1]
    # 锚到 ≤ anchor 的最后一根 bar
    locs = np.where(cal <= anchor)[0]
    if len(locs) == 0:
        print(f'无 {anchor.date()} 及之前的数据'); return
    loc = locs[-1]
    row = ind.iloc[loc]
    state, reasons = classify(ind, loc)

    asof_txt = f'  (as-of {anchor.date()})' if asof is not None else ''
    lines = []
    P = lines.append
    P('=' * 70)
    P(f'US 牛转熊体制监控{asof_txt}    数据日 {cal[loc].date()}')
    P('=' * 70)
    P(f'  市场状态: 【{state}】 {STATE_CN[state]}')
    for r in reasons:
        P(f'    · {r}')
    P('')
    P(f'  SPY {row["SPY"]:.2f}   VIX {row["VIX"]:.1f}   '
      f'宽度(%>200日均线) {row["pct_above_200"]:.0f}%   宽度样本 {ind.attrs["n_breadth"]} 只')
    P('')
    P('  趋势层 (防守脊梁, 全天候):')
    P(f'    {"✗" if row["below_200dma"] else "✓"} 收盘 vs 200 日线: '
      f'{"在下方" if row["below_200dma"] else "在上方"}')
    P(f'    {"✗" if row["200dma_falling"] else "✓"} 200 日线斜率: '
      f'{"掉头向下" if row["200dma_falling"] else "向上"}')
    P(f'    {"✗" if row["death_cross"] else "✓"} 50/200 死叉: '
      f'{"已死叉" if row["death_cross"] else "未死叉"}')
    P('')
    P(f'  领先共振层 (早减仓, 仅慢顶有效): {int(row["confluence"])}/6')
    for c in ind.attrs['cats']:
        P(f'    {"🔴" if row[c] else "·  "} {CAT_CN[c]}')

    if history and history > 1:
        P('')
        P(f'  近 {history} 日体制轨迹:')
        seg = ind.iloc[max(0, loc - history + 1): loc + 1]
        tbl = []
        for d, r in seg.iterrows():
            st, _ = classify(ind, cal.get_loc(d))
            tbl.append([d.date(), f'{r["SPY"]:.1f}', f'{r["VIX"]:.0f}',
                        f'{r["pct_above_200"]:.0f}%', int(r['confluence']),
                        'break' if r['trend_break'] else
                        ('<200' if r['below_200dma'] else 'ok'), st])
        P(tab_mod.tabulate(
            tbl, headers=['date', 'SPY', 'VIX', '%>200', 'conf', 'trend', 'state'],
            tablefmt='simple'))

    P('')
    P('  判据(经 2018/2020/2022 三周期回测): DEFEND=全天候稳健底线(滞后);')
    P('  WATCH=慢顶提前减仓(对外生闪崩无效, 只减不清); 详见 docs/bull_to_bear_2021_2022.md')
    P('=' * 70)

    out = '\n'.join(lines)
    print(out)
    tag = anchor.strftime('%Y%m%d')
    out_dir = os.path.join(RESULT_DIR, 'us_regime_monitor')
    path = os.path.join(out_dir, f'us_regime_monitor_{tag}.txt')
    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(path, 'w', encoding='UTF-8') as f:
            f.write(out + '\n')
        logging.info(f'报告写入 {path}')
    except Exception as e:
        logging.warning(f'报告写盘失败: {e}')


def main():
    parser = OptionParser()
    parser.add_option('--asof', dest='asof', default=None,
                      help='回测: 站在 YYYY-MM-DD 当天往回看 (point-in-time)')
    parser.add_option('--history', dest='history', type='int', default=10,
                      help='末尾附最近 N 个交易日的体制轨迹 (默认 10, 0 关闭)')
    opt, _ = parser.parse_args()
    asof = None
    if opt.asof:
        try:
            asof = pd.Timestamp(opt.asof).normalize()
        except Exception:
            print(f'--asof 日期无法解析: {opt.asof}'); sys.exit(1)
    run(asof, opt.history)


if __name__ == '__main__':
    main()
