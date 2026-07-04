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

  第二意见层 (QQQ 专属, 提示不门控)  HMM
      QQQ 对数收益率两态高斯 HMM 的 forward 滤波 P(高波态)。walk-forward 验证
      (research/hmm_regime_verify.py): QQQ 2003+ 赢 MA200 (CAGR 15.3% vs 12.1%,
      切换频率相当, 四次真熊离场更早), 但同窗 SPY 证伪普适性 → 按 n=1 苗头对待,
      只在报告/JSON 里提示, 不进 DEFEND/WATCH 状态机。前视警示: 全样本平滑解码
      虚增 ~8.5pp CAGR, 故此处只用滤波概率(无 backward), 判定只取锚日当行。

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
IMMUNITY_D    = 10    # 熊反免疫期: DEFEND 段结束后, SPY 须连续 N 日收盘站回 200 日线
                      # 才解除。依据 (huice 2022 回测 + regime --backtest): 熊市反弹里
                      # 20 周线状态机会短暂读回 STRONG (2022-08 最贵的假信号), 而 SPY
                      # 整段熊反从未连续站回 200 日线; 2020 V 型底则 6 月中旬即解除 —
                      # 免疫期只挡熊反、不长期挡真复苏。入场侧消费 (tech_swing 降档),
                      # 不影响持仓管理。
CONF_WATCH    = 4     # 早减仓层: 领先共振 ≥ 此值
CONF_CAUTION  = 3     # 留意层: 共振 ≥ 此值 (或已破 200 线)
SUSTAIN       = 3     # "持续"定义: 连续 N 个交易日成立 (压住单日抖动)
DIVERG_DROP   = 12    # 宽度背离: %>200日 较 60 日前下滑超过此点数
NEAR_HIGH     = 0.98  # 宽度背离: SPY ≥ 60 日高 × 此值 视作"仍在高位"
VIX_REGIME    = 20    # VIX 20 日均上穿此值且抬头 = 波动率体制上移

# ── HMM 第二意见层 (QQQ 专属; 验证与调参依据: research/hmm_regime_verify.py) ──
HMM_K       = 2      # 两态: 低波上涨/高波下跌 (K=3 walk-forward 更差: 鞭打或迟钝)
HMM_SEED    = 7      # EM 对种子不敏感 (5 种子参数逐位相同), 固定只为可复现
HMM_MIN_OBS = 1000   # 最少训练观测 (~4 年) → 1999 起的序列 2003 起有读数
HMM_RISK_TH = 0.5    # P(高波态) ≥ 此值 = 风险态
HMM_REFIT   = 63     # --backtest walk-forward 重拟合周期 (季度, 与 research 版一致)
HMM_START   = '1999-01-01'   # 训练起点 = 验证过的配置 (含 2000/2008 熊)。用 2013 起
                             # 的深缓存训练会低估高波态 σ → 信号从 ~6 次/年恶化到
                             # ~28 次/年 (2018+ 回测实测), 故 QQQ 单独拉全历史。


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


def immunity_series(ind: pd.DataFrame) -> pd.Series:
    """post-DEFEND 熊反免疫期 (逐日布尔)。DEFEND 段结束后保持 True, 直到 SPY 连续
    IMMUNITY_D 日收盘 ≥ 200 日线; DEFEND 当日本身标 False (DEFEND 自己在管)。"""
    above = ~ind['below_200dma'].fillna(False)
    imm, in_imm, run = [], False, 0
    for i in range(len(ind)):
        st, _ = classify(ind, i)
        if st == 'DEFEND':
            in_imm, run = True, 0
            imm.append(False)
            continue
        if in_imm:
            run = run + 1 if bool(above.iloc[i]) else 0
            if run >= IMMUNITY_D:
                in_imm = False
        imm.append(in_imm)
    return pd.Series(imm, index=ind.index)


# ── HMM 第二意见层 ────────────────────────────────────────────────────────────

def _hmm_qqq_close(asof: 'pd.Timestamp | None' = None) -> pd.Series:
    """QQQ 自 HMM_START(1999) 的收盘序列, 专用缓存 QQQ_hmm_max.csv (深缓存的
    2013 起太短, 见 HMM_START 注释)。live 时拼共享 3 年缓存补最新 bar (重叠日取
    live); 拼接缝在 3 年前, 复权基准差造成的单日失真对 EM 无感。--asof 截断,
    缓存未覆盖 asof 时整段重拉 (与 _deep_fetch 同策略)。"""
    path = os.path.join(DEEP_CACHE_DIR, 'QQQ_hmm_max.csv')
    hist = pd.DataFrame()
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, parse_dates=['date']).set_index('date')
            if not df.empty and (asof is None or df.index.max() >= asof):
                hist = df
        except Exception:
            pass
    if hist.empty:
        try:
            raw = yf.Ticker('QQQ').history(start=HMM_START, auto_adjust=True)
            if not raw.empty:
                hist = raw.rename(columns={'Close': 'close'})[['close']].copy()
                hist.index = pd.to_datetime(hist.index).tz_localize(None)
                hist.index.name = 'date'
                hist = hist.dropna()
                os.makedirs(DEEP_CACHE_DIR, exist_ok=True)
                hist.reset_index().to_csv(path, index=False)
        except Exception as e:
            logging.warning(f'QQQ HMM 全历史拉取失败 ({e})')
    parts = [hist['close']] if not hist.empty else []
    if asof is None:
        live = _fetch_daily('QQQ')
        if live is not None and not live.empty:
            parts.append(live['close'])
    if not parts:
        return pd.Series(dtype=float)
    s = pd.concat(parts)
    s = s[~s.index.duplicated(keep='last')].sort_index().dropna()
    return s[s.index <= asof] if asof is not None else s


def _hmm_fit(x: np.ndarray):
    """高斯 HMM EM 拟合 (对数收益率×100), 返回 (pi, A, mu, sd)。
    hmmlearn 缺失或拟合失败返回 None — 上层跳过第二意见层, 不影响主报告。"""
    try:
        from hmmlearn.hmm import GaussianHMM
        m = GaussianHMM(n_components=HMM_K, covariance_type='diag',
                        n_iter=200, tol=1e-4, random_state=HMM_SEED)
        m.fit(x.reshape(-1, 1))
        return (m.startprob_.copy(), m.transmat_.copy(),
                m.means_.ravel().copy(), np.sqrt(m.covars_.ravel()).copy())
    except Exception as e:
        logging.warning(f'HMM 不可用 ({e}) — 跳过第二意见层')
        return None


def _hmm_forward(x: np.ndarray, pi, A, mu, sd) -> np.ndarray:
    """forward 滤波 P(S_t | O_1..O_t) — 只向前递推, 无 backward = 无未来信息。"""
    from scipy.stats import norm
    B = norm.pdf(x[:, None], mu[None, :], sd[None, :]) + 1e-300
    out = np.empty((len(x), len(mu)))
    a = pi * B[0]; a /= a.sum(); out[0] = a
    for t in range(1, len(x)):
        a = (a @ A) * B[t]; a /= a.sum(); out[t] = a
    return out


def hmm_second_opinion(asof: 'pd.Timestamp | None' = None):
    """QQQ HMM 第二意见: 参数拟合于截至锚日的全历史, forward 滤波出逐日
    P(高波态) 序列。返回 (probs, params) 或 (None, None)。
    注意: probs 里锚日以前的行, 参数用到了它们之后的观测 — 仅供轨迹展示;
    体制判定与 JSON 只取最后一行 (那一行严格 point-in-time)。"""
    close = _hmm_qqq_close(asof)
    if len(close) < HMM_MIN_OBS + 1:
        return None, None
    lr = np.log(close).diff().dropna() * 100.0
    fit = _hmm_fit(lr.values)
    if fit is None:
        return None, None
    pi, A, mu, sd = fit
    hi, lo = int(np.argmax(sd)), int(np.argmin(sd))
    probs = pd.Series(_hmm_forward(lr.values, pi, A, mu, sd)[:, hi], index=lr.index)
    params = {'mu_lo': float(mu[lo]), 'sd_lo': float(sd[lo]),
              'mu_hi': float(mu[hi]), 'sd_hi': float(sd[hi]),
              'stay_lo': float(A[lo, lo]), 'stay_hi': float(A[hi, hi])}
    return probs, params


def _hmm_walkforward_prob(close: pd.Series) -> 'pd.Series | None':
    """--backtest 用: 严格 walk-forward 的 P(高波态) — 每 HMM_REFIT 日以截至
    当日的扩展窗重拟合, 块内 forward 滤波 (与 research/hmm_regime_verify.py 同法)。
    前 HMM_MIN_OBS 日为 NaN (纯训练热身)。"""
    if len(close) < HMM_MIN_OBS + 2:
        return None
    lr = np.log(close).diff().dropna() * 100.0
    x = lr.values
    wf = np.full(len(x), np.nan)
    for fit_end in range(HMM_MIN_OBS, len(x), HMM_REFIT):
        blk_end = min(fit_end + HMM_REFIT, len(x))
        fit = _hmm_fit(x[:fit_end])
        if fit is None:
            return None
        pi, A, mu, sd = fit
        hi = int(np.argmax(sd))
        wf[fit_end:blk_end] = _hmm_forward(x[:blk_end], pi, A, mu, sd)[fit_end:blk_end, hi]
    return pd.Series(wf, index=lr.index)


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
    # 熊反免疫期 (post-DEFEND): 全 panel 逐日 classify 便宜, 直接算序列取当日
    imm = bool(immunity_series(ind.iloc[:loc + 1]).iloc[-1])

    asof_txt = f'  (as-of {anchor.date()})' if asof is not None else ''
    lines = []
    P = lines.append
    P('=' * 70)
    P(f'US 牛转熊体制监控{asof_txt}    数据日 {cal[loc].date()}')
    P('=' * 70)
    P(f'  市场状态: 【{state}】 {STATE_CN[state]}')
    for r in reasons:
        P(f'    · {r}')
    if imm:
        P(f'    · 熊反免疫期生效: 上一 DEFEND 段后 SPY 尚未连续 {IMMUNITY_D} 日站回'
          f' 200 日线 — 入场侧对 STRONG 读数打折 (tech_swing 自动降档)')
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

    hmm_p, hmm_par = hmm_second_opinion(asof=asof)
    if hmm_p is not None:
        p_now = float(hmm_p.iloc[-1])
        tag = '高波/风险态 🔴' if p_now >= HMM_RISK_TH else '低波/平稳态 ✓'
        edge = '  (临界区, 体制可能在切换)' if 0.3 < p_now < 0.7 else ''
        P('')
        P('  HMM 第二意见层 (QQQ 波动率体制, 滤波概率 — 提示不门控):')
        P(f'    P(高波态) = {p_now * 100:.0f}%  → {tag}{edge}')
        P(f'    两态画像: 低波 μ{hmm_par["mu_lo"]:+.2f}%/日 σ{hmm_par["sd_lo"]:.2f}%'
          f' · 高波 μ{hmm_par["mu_hi"]:+.2f}%/日 σ{hmm_par["sd_hi"]:.2f}%'
          f' · 自持 {hmm_par["stay_lo"]:.3f}/{hmm_par["stay_hi"]:.3f}')
        P('    仅 QQQ walk-forward 验证通过 (SPY 证伪, n=1 苗头) — research/hmm_regime_verify.py')

    if history and history > 1:
        P('')
        P(f'  近 {history} 日体制轨迹:')
        seg = ind.iloc[max(0, loc - history + 1): loc + 1]
        tbl = []
        for d, r in seg.iterrows():
            st, _ = classify(ind, cal.get_loc(d))
            hp = (f'{hmm_p.get(d, np.nan) * 100:.0f}%'
                  if hmm_p is not None and pd.notna(hmm_p.get(d, np.nan)) else '—')
            tbl.append([d.date(), f'{r["SPY"]:.1f}', f'{r["VIX"]:.0f}',
                        f'{r["pct_above_200"]:.0f}%', int(r['confluence']),
                        'break' if r['trend_break'] else
                        ('<200' if r['below_200dma'] else 'ok'), st, hp])
        P(tab_mod.tabulate(
            tbl, headers=['date', 'SPY', 'VIX', '%>200', 'conf', 'trend', 'state',
                          'P高波'],
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

    # 机读状态快照 — tech_swing 开扫前读它做入场侧门控 (两台状态机打通)。
    # 只在 live 写: --asof 的历史状态写进去会被次日扫描误当现状。
    if asof is None:
        import json
        state_path = os.path.join(out_dir, 'us_regime_state.json')
        try:
            with open(state_path, 'w', encoding='UTF-8') as f:
                json.dump({
                    'date': str(cal[loc].date()), 'state': state,
                    'confluence': int(row['confluence']), 'immunity': imm,
                    'spy': round(float(row['SPY']), 2),
                    # 第二意见层 (提示字段, 消费方可忽略): QQQ HMM 滤波 P(高波态)
                    'hmm_qqq_risk_prob': (round(float(hmm_p.iloc[-1]), 3)
                                          if hmm_p is not None else None),
                    'generated': datetime.datetime.now().isoformat(timespec='seconds'),
                }, f, ensure_ascii=False)
            logging.info(f'状态快照 → {state_path}')
        except Exception as e:
            logging.warning(f'状态快照写盘失败: {e}')


def run_backtest(start: pd.Timestamp, end: 'pd.Timestamp | None'):
    """--backtest: 逐日重放 classify → 状态时间轴 + 按状态调仓的 overlay 评估。

    panel 只建一次(build_panel 每列都是纯回看的 rolling), classify(loc) 逐日
    调用 — 与逐日 --asof 重跑 point-in-time 等价, 但快几个量级。
    ⚠ 读数前必知(docs/bull_to_bear_2021_2022.md §7): 宽度篮子是【今天】的成分股,
    幸存者偏差使历史宽度偏健康 → WATCH 层的历史触发被【系统性低估】;
    DEFEND 层是 SPY 自身, 无此偏差 — overlay 里 DEFEND-only 的数字最可信。
    执行口径: 状态在 t 日收盘可知, 敞口从 t+1 日生效 (shift 1), 不计交易成本。
    """
    spy_deep = _deep_fetch(BAROMETER)
    if end is None:
        end = spy_deep.index.max()      # 锚到深缓存末日, 避免整池重拉
    _sw._ASOF = end
    ind = build_panel(asof=end, deep=True)
    cal = ind.index

    states = pd.Series(
        [classify(ind, i)[0] for i in range(len(cal))], index=cal, name='state')
    win = states.loc[start:end]
    if win.empty:
        print(f'{start.date()}~{end.date()} 无数据'); return
    seg_ind = ind.loc[win.index]

    lines = []
    P = lines.append
    P('=' * 78)
    P(f'US 体制监控 · 逐日重放回测   {win.index[0].date()} → {win.index[-1].date()}'
      f'   (宽度样本 {ind.attrs["n_breadth"]} 只, 今日成分股 ⚠幸存者偏差: WATCH 历史触发被低估)')
    P('=' * 78)

    # ── 状态分布 ──
    days = len(win)
    P('')
    P('[ 状态分布 ]')
    for st in ('BULL', 'CAUTION', 'WATCH', 'DEFEND'):
        n = int((win == st).sum())
        P(f'  {st:<8} {STATE_CN[st]:<12} {n:>5} 日  ({n / days * 100:.0f}%)')

    # ── 状态段落 (连续同状态合并) ──
    qqq = _close('QQQ', cal, _deep_fetch)
    spy = ind['SPY']
    P('')
    P('[ 状态时间轴 ]  (连续同状态合并; 段内涨跌为该段首日→末日收盘)')
    seg_rows = []
    seg_start = win.index[0]
    prev = win.iloc[0]
    bounds = list(win.index[1:]) + [None]
    for d, st in zip(bounds, list(win.iloc[1:]) + [None]):
        if st == prev:
            continue
        seg_end = win.index[win.index.get_loc(d) - 1] if d is not None else win.index[-1]
        n_d = win.index.get_loc(seg_end) - win.index.get_loc(seg_start) + 1
        s0, s1 = spy.loc[seg_start], spy.loc[seg_end]
        q0, q1 = qqq.loc[seg_start], qqq.loc[seg_end]
        seg_rows.append([
            str(seg_start.date()), str(seg_end.date()), n_d, prev,
            f'{(s1 / s0 - 1) * 100:+.1f}%' if s0 > 0 else '—',
            f'{(q1 / q0 - 1) * 100:+.1f}%' if q0 > 0 else '—',
        ])
        if d is not None:
            seg_start, prev = d, st
    P(tab_mod.tabulate(seg_rows,
                       headers=['from', 'to', 'days', 'state', 'SPY', 'QQQ'],
                       tablefmt='simple'))

    # ── overlay 评估 (QQQ 为交易载体) ──
    variants = {
        'B&H QQQ (基准)':          {'BULL': 1.0, 'CAUTION': 1.0, 'WATCH': 1.0, 'DEFEND': 1.0},
        'DEFEND-only (仅防守层)':   {'BULL': 1.0, 'CAUTION': 1.0, 'WATCH': 1.0, 'DEFEND': 0.0},
        'WATCH 减半 + DEFEND 清仓': {'BULL': 1.0, 'CAUTION': 1.0, 'WATCH': 0.5, 'DEFEND': 0.0},
        '保守 (CAUTION 也减)':      {'BULL': 1.0, 'CAUTION': 0.75, 'WATCH': 0.5, 'DEFEND': 0.0},
    }
    ret = qqq.pct_change()
    P('')
    P('[ OVERLAY 评估 ]  (QQQ 载体, 状态收盘可知→次日生效, 无交易成本)')
    rows = []

    def _overlay_row(name, expo):
        """expo: 已 shift(1) 的敞口序列 (win.index 对齐)。"""
        r = (ret.reindex(win.index) * expo).fillna(0)
        eq = (1 + r).cumprod()
        yrs = days / 252
        cagr = eq.iloc[-1] ** (1 / yrs) - 1 if yrs > 0 else 0
        mdd = (eq / eq.cummax() - 1).min()
        vol = r.std() * np.sqrt(252)
        switches = int((expo.diff().fillna(0) != 0).sum())
        rows.append([name, f'{(eq.iloc[-1] - 1) * 100:+.0f}%', f'{cagr * 100:+.1f}%',
                     f'{mdd * 100:.1f}%', f'{vol * 100:.1f}%',
                     f'{cagr / vol:.2f}' if vol > 0 else '—',
                     f'{expo.mean() * 100:.0f}%', switches])

    for name, mp in variants.items():
        expo = states.map(mp).shift(1).reindex(win.index).fillna(mp.get(win.iloc[0], 1.0))
        _overlay_row(name, expo)

    # 第二意见层对照: HMM walk-forward (季度重拟合+滤波, 无前视)。热身期(NaN,
    # 序列 1999 起 → ~2003 前)按满仓处理, 不干预。
    hmm_note = ''
    wf = _hmm_walkforward_prob(_hmm_qqq_close(end))
    if wf is not None:
        cover = wf.dropna()
        if not cover.empty and cover.index[0] <= win.index[-1]:
            hmm_expo = (~(wf >= HMM_RISK_TH)).astype(float)   # NaN → 满仓
            hmm_expo = hmm_expo.reindex(cal).ffill().shift(1).reindex(win.index).fillna(1.0)
            _overlay_row('HMM-only (P高波≥.5 清仓)', hmm_expo)
            defend_expo = states.map(variants['DEFEND-only (仅防守层)']) \
                .shift(1).reindex(win.index).fillna(1.0)
            _overlay_row('DEFEND ∪ HMM (任一触发清仓)',
                         np.minimum(defend_expo, hmm_expo))
            hmm_note = (f'  HMM 行: QQQ 两态高斯HMM walk-forward 滤波 (读数自 '
                        f'{cover.index[0].date()}, 之前热身按满仓); 提示层对照, 不进状态机。')

    P(tab_mod.tabulate(rows, headers=['variant', 'totRet', 'CAGR', 'maxDD', 'vol',
                                      'CAGR/vol', 'avg敞口', '调仓次数'],
                       tablefmt='simple'))
    P('')
    P('  读法: DEFEND-only 是无幸存者偏差的那条 (SPY 自身趋势); WATCH 层的历史价值')
    P('  因宽度篮子偏差被低估, live 前瞻才是它的真实成绩。无交易成本/滑点/税。')
    if hmm_note:
        P(hmm_note)
    P('=' * 78)

    out = '\n'.join(lines)
    print(out)
    out_dir = os.path.join(RESULT_DIR, 'us_regime_monitor')
    path = os.path.join(out_dir,
                        f'us_regime_backtest_{start.strftime("%Y%m%d")}_{end.strftime("%Y%m%d")}.txt')
    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(path, 'w', encoding='UTF-8') as f:
            f.write(out + '\n')
        logging.info(f'回测报告写入 {path}')
    except Exception as e:
        logging.warning(f'报告写盘失败: {e}')


def main():
    parser = OptionParser()
    parser.add_option('--asof', dest='asof', default=None,
                      help='回测: 站在 YYYY-MM-DD 当天往回看 (point-in-time)')
    parser.add_option('--history', dest='history', type='int', default=10,
                      help='末尾附最近 N 个交易日的体制轨迹 (默认 10, 0 关闭)')
    parser.add_option('--backtest', dest='backtest', default=None,
                      help='逐日重放回测起点 YYYY-MM-DD: 状态时间轴+overlay 评估 '
                           '(panel 建一次逐日 classify, 与逐日 --asof 等价但快)')
    parser.add_option('--end', dest='end', default=None,
                      help='--backtest 终点 (默认深缓存末日, 避免整池重拉)')
    opt, _ = parser.parse_args()
    if opt.backtest:
        try:
            start = pd.Timestamp(opt.backtest).normalize()
            end = pd.Timestamp(opt.end).normalize() if opt.end else None
        except Exception:
            print(f'--backtest/--end 日期无法解析'); sys.exit(1)
        run_backtest(start, end)
        return
    asof = None
    if opt.asof:
        try:
            asof = pd.Timestamp(opt.asof).normalize()
        except Exception:
            print(f'--asof 日期无法解析: {opt.asof}'); sys.exit(1)
    run(asof, opt.history)


if __name__ == '__main__':
    main()
