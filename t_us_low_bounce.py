# coding: utf-8
"""
US Low-Bounce Scanner — 250日新低后的首日大阳反弹 (超跌·新低·反转日)

形态: 250日最低价刚刚(前 --low-days 个交易日内)砸出来, 信号日收盘较前收 ≥ --chg%。
这是"最后一跌后的第一根反抗阳线" — 与 pullback_shock (强势股急跌买点) 正好是
光谱两端: 那边买强势里的弱一天, 这边赌弱势里的强一天。

⚠ 定位: 候选发现层, NOT a proven signal source。2026-07 在 r2000ht 池的 20 日
样本里两极分化剧烈 (+72% ~ -46%), 未过 huice 检验; 且与 docs
twenty_week_trend_system §7.5-7.6 的实证一致 — 超跌入场的质量门是防归零
过滤器, 不是收益增强器。故默认带三道流动性/质量硬门 (仙股和无量反弹是该
形态的主要垃圾来源, 见下), 参考止损 = 刚砸出的 250 日低 (跌破 = 反弹证伪,
按 ADR-0002 日收盘判)。

硬门 (命令行全可调):
  · 价格   close ≥ $3         — 仙股的 7% 是一个 tick, 无意义
  · 成交额 20日均成交额 ≥ $5M — 可交易性
  · 放量   当日量 ≥ 1.2×20日均量 — 20日实证: 缩量反弹(<1×)几乎全军覆没,
                                  赢家普遍 3×+ (TVRD 28×/+72%, SNWV 4.9×/+36%)
  · 市值   ≥ $300M            — Nasdaq screener 一次请求全市场, 当日缓存;
                                接口挂了 → 跳过市值门并打警告 (优雅降级)。
                                注意市值是【当前】市值 — --asof 回测时该门
                                不是点位口径, 报告里会标注。

数据: 复用 t_us_tech_swing 的 yfinance 缓存 (_fetch_daily, ADR-0001)。

模式 (对齐 t_us_tr_surge / t_us_pullback_shock):
  --scan [--universe both|ndx|all|r2000ht] [--lookback N]
      扫描池子, 报告最近 N 个交易日 (默认 5) 内的每个命中 — 该形态稀少,
      只看当日报告常年空白, 带上近几日让报告有上下文; 当日命中单独标 ★。
  --ticker SYM
      单票诊断: 今天各条件的通过/未过明细 + 历史命中列表。
  --asof YYYY-MM-DD (两模式通用)
      回测开关: 只用到那天为止的数据 (市值门除外, 见上)。

  --grok [--grok-max N]  (--scan 附加层, cron 默认开)
      对每个命中跑一次 Grok 催化剂复核 (x_search+web_search, ~$0.1/票):
      反弹日窗口内有无实质催化剂被讨论 (✓类别/💬闲聊/✗无人问津)。只标注不门控,
      按 ticker×信号日去重 append 到 result/us_low_bounce/grok_catalyst.csv 积累
      前向样本 (n=1 假说: 催化剂在场=真反转 VSTM, 无人问津=死猫跳 SSTK; 攒够 n
      再定去留)。--asof 下自动禁用 (X 检索非点位口径); 无 key/接口挂 → 跳过不崩。

Usage:
  python t_us_low_bounce.py --scan                          # all 池, 近5日
  python t_us_low_bounce.py --scan --universe r2000ht --lookback 20
  python t_us_low_bounce.py --scan --asof 2026-06-29
  python t_us_low_bounce.py --ticker SNWV
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

from t_us_tech_swing import _fetch_daily

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger('yfinance').setLevel(logging.ERROR)

RESULT_DIR   = '/home/ryan/DATA/result'
UNIVERSE_DIR = '/home/ryan/DATA/DAY_Global/US_universe'
MKTCAP_CACHE = os.path.join(UNIVERSE_DIR, 'mktcap.json')
GROK_LEDGER  = os.path.join(RESULT_DIR, 'us_low_bounce', 'grok_catalyst.csv')
UA = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

# ── 默认门槛 (命令行可调) ────────────────────────────────────────────────────
CHG_MIN      = 7.0     # 信号日收盘较前收 ≥ 此值(%)
LOW_WIN      = 250     # 新低回看窗口(交易日; 不足按全部, 但至少 MIN_BARS)
LOW_WITHIN   = 2       # 250日最低价须出现在信号日前 N 个交易日内
LOOKBACK     = 5       # --scan 报告最近 N 个交易日的命中
MIN_PRICE    = 3.0     # 信号日收盘 ≥ $
MIN_DVOL     = 5e6     # 20日均成交额 ≥ $
MIN_VOLX     = 1.2     # 信号日量 / 20日均量 ≥
MIN_MKTCAP   = 300e6   # 当前市值 ≥ $ (0 = 关闭市值门)
MIN_BARS     = 60      # 数据下限(交易日)


# ── 市值 (Nasdaq screener, 当日缓存, 失败优雅降级) ────────────────────────────
def load_mktcap() -> dict:
    """symbol → 市值($)。当日缓存; 抓取失败 → 陈旧缓存 → {} (调用方跳过市值门)。"""
    os.makedirs(UNIVERSE_DIR, exist_ok=True)
    if os.path.exists(MKTCAP_CACHE):
        mtime = datetime.date.fromtimestamp(os.path.getmtime(MKTCAP_CACHE))
        if mtime == datetime.date.today():
            try:
                with open(MKTCAP_CACHE) as fh:
                    return json.load(fh)
            except Exception:
                pass
    try:
        import urllib.request
        req = urllib.request.Request(
            'https://api.nasdaq.com/api/screener/stocks?download=true', headers=UA)
        rows = json.loads(urllib.request.urlopen(req, timeout=60).read())['data']['rows']
        m = {}
        for r in rows:
            try:
                cap = float(r.get('marketCap') or 0)
            except ValueError:
                cap = 0.0
            if cap > 0:
                m[r['symbol'].strip().upper()] = cap
        with open(MKTCAP_CACHE, 'w') as fh:
            json.dump(m, fh)
        logging.info(f'市值表: {len(m)} 只 (新抓取)')
        return m
    except Exception as e:
        logging.warning(f'市值表抓取失败 ({e})')
        if os.path.exists(MKTCAP_CACHE):
            with open(MKTCAP_CACHE) as fh:
                m = json.load(fh)
            logging.warning(f'降级使用陈旧市值缓存: {len(m)} 只')
            return m
        logging.warning('无市值数据 — 本次跳过市值门')
        return {}


# ── 核心: 纯函数, 判定单日是否命中 ────────────────────────────────────────────
def check_day(d: pd.DataFrame, i: int, chg_min: float, low_within: int,
              min_price: float, min_dvol: float, min_volx: float):
    """d = 完整日线, i = 信号日整数位置。返回 (hit, row|None, fails[])。
    fails 记录未过的门 (--ticker 诊断用)。"""
    fails = []
    c0, c1 = float(d['close'].iloc[i]), float(d['close'].iloc[i - 1])
    if c1 <= 0:
        return False, None, ['数据异常(前收≤0)']
    chg = (c0 / c1 - 1.0) * 100.0
    if chg < chg_min:
        fails.append(f'涨幅 {chg:+.1f}% < {chg_min}%')
    # 新低: 截至前一日的最近 LOW_WIN 根里, 最低价出现在最后 low_within 根内
    win = d.iloc[max(0, i - LOW_WIN):i]
    lo_pos = int(win['low'].values.argmin())
    days_ago = len(win) - lo_pos                    # 1=前一日
    lo250 = float(win['low'].min())
    if days_ago > low_within:
        fails.append(f'250日低在 {days_ago} 天前 > {low_within}')
    if c0 < min_price:
        fails.append(f'价格 {c0:.2f} < {min_price}')
    v20 = d['volume'].iloc[max(0, i - 21):i].mean()
    dvol = float((d['close'] * d['volume']).iloc[max(0, i - 21):i].mean())
    volx = float(d['volume'].iloc[i] / v20) if v20 > 0 else np.nan
    if dvol < min_dvol:
        fails.append(f'20日均成交额 ${dvol/1e6:.1f}M < ${min_dvol/1e6:.0f}M')
    if not (pd.notna(volx) and volx >= min_volx):
        fails.append(f'放量 {volx:.1f}× < {min_volx}×')
    if fails:
        return False, None, fails
    row = {
        'date':     d.index[i].date(),
        'close':    round(c0, 2),
        'chg%':     round(chg, 1),
        'low250':   round(lo250, 2),
        'low_ago':  days_ago,
        'vs_low%':  round((c0 / lo250 - 1) * 100, 1),
        'vol_x':    round(volx, 1),
        'dvol$M':   round(dvol / 1e6, 1),
        'stop':     round(lo250, 2),                # 参考止损 = 刚砸出的250日低
    }
    return True, row, []


# ── Grok 催化剂复核 (标注积累层, 不门控) ──────────────────────────────────────
# 2026-07-11 SSTK/VSTM n=1 探索: X 互动【量】在小盘上太稀薄不可用; 但"反弹日
# 有无实质催化剂讨论"完美分开了真反转(VSTM, JNJ传闻+临床数据)与死猫跳(SSTK,
# 无人问津)。事后叙事风险大 → 先只标注、按 ticker×信号日 append 到 GROK_LEDGER
# 积累前向样本, 攒够 n 再决定要不要当门槛。live-only (--asof 下禁用, X 检索
# 做不到点位口径, 同 signal ledger 原则)。~$0.1/票。
_GROK_SCHEMA = {
    'type': 'json_schema', 'name': 'low_bounce_catalyst',
    'schema': {
        'type': 'object', 'additionalProperties': False,
        'properties': {
            'catalyst':      {'type': 'string',
                              'enum': ['substantive', 'chatter_only', 'none', 'unclear'],
                              'description': 'substantive=有实质催化剂讨论(财报/FDA/并购/合同); '
                                             'chatter_only=只有散户喊单式闲聊; none=无人讨论'},
            'catalyst_type': {'type': 'string', 'description': '催化剂类别, 中文短语, 无则空'},
            'driver':        {'type': 'string', 'description': '在吵什么, 中文一句话'},
            'tone':          {'type': 'string', 'enum': ['bullish', 'bearish', 'mixed', 'unclear']},
            'x_activity':    {'type': 'string', 'enum': ['active', 'sparse', 'none'],
                              'description': '信号日前后X上该票讨论量级'},
            'evidence_dates': {'type': 'array', 'items': {'type': 'string'},
                               'description': '主要依据帖子/新闻的发布日期 YYYY-MM-DD'},
        },
        'required': ['catalyst', 'catalyst_type', 'driver', 'tone',
                     'x_activity', 'evidence_dates'],
    },
}
_GROK_SYS = ('你是量化研究助理。用 x_search/web_search 检索给定美股在指定日期窗口内的'
             '真实帖子和新闻, 判断当时有没有实质催化剂在被讨论。只依据检索到的、'
             '发布日期落在窗口内的内容说话, 窗口外的一律不算, 检索不到就如实说 none, 绝不编造。'
             '所有文本用中文。')
_GROK_MARK = {'substantive': '✓', 'chatter_only': '💬', 'none': '✗', 'unclear': '?'}


def _grok_ledger_load() -> pd.DataFrame:
    if os.path.exists(GROK_LEDGER):
        try:
            return pd.read_csv(GROK_LEDGER, dtype=str)
        except Exception as e:
            logging.warning(f'读 grok ledger 失败 ({e}) — 视为空')
    return pd.DataFrame(columns=['scan_ts', 'ticker', 'signal_date', 'catalyst',
                                 'catalyst_type', 'tone', 'x_activity', 'driver',
                                 'evidence_dates', 'cost_usd'])


def grok_annotate(hits: pd.DataFrame, grok_max: int) -> dict:
    """对窗口内未标注过的 (ticker, 信号日) 逐个跑催化剂复核, append 进 ledger。
    返回 {(ticker, date_str): mark} 供报告列用 (含历史已标注的)。失败降级为空。"""
    led = _grok_ledger_load()
    done = set(zip(led['ticker'], led['signal_date']))
    marks = {(t, d): _GROK_MARK.get(c, '?') + (ct if c == 'substantive' else '')
             for t, d, c, ct in zip(led['ticker'], led['signal_date'],
                                    led['catalyst'], led['catalyst_type'].fillna(''))}
    todo = [(r['ticker'], str(r['date'])) for _, r in hits.iterrows()
            if (r['ticker'], str(r['date'])) not in done]
    if not todo:
        return marks
    try:
        from grok_lib import _structured_scan
    except Exception as e:
        logging.warning(f'grok_lib 不可用 ({e}) — 跳过催化剂复核')
        return marks
    if len(todo) > grok_max:
        logging.warning(f'催化剂复核: 待标注 {len(todo)} > 上限 {grok_max}, 只跑最新 {grok_max} 个')
        todo = sorted(todo, key=lambda x: x[1], reverse=True)[:grok_max]
    new_rows = []
    for tkr, dstr in todo:
        d0 = (pd.Timestamp(dstr) - pd.Timedelta(days=7)).date()
        user = (f'美股 ${tkr} 在 {dstr} 砸出250日新低后单日大阳反弹。'
                f'检索 {d0} ~ {dstr} 期间(含当天)发布的 X 帖子和新闻: '
                f'这次反弹有没有实质催化剂(财报/FDA/并购传闻/大合同/回购等)在被讨论, '
                f'还是只有技术性反弹的闲聊、甚至无人问津?')
        try:
            res = _structured_scan(_GROK_SYS, user, _GROK_SCHEMA,
                                   fallback={'catalyst': 'unclear', 'catalyst_type': '',
                                             'driver': '解析失败', 'tone': 'unclear',
                                             'x_activity': 'none', 'evidence_dates': []})
            cost = (res.get('_meta') or {}).get('cost_usd_est')
            logging.info(f'  grok {tkr}@{dstr}: {res["catalyst"]}/{res["catalyst_type"]} '
                         f'({res["x_activity"]}, ${cost})')
        except Exception as e:
            logging.warning(f'  grok {tkr}@{dstr} 失败 ({e}) — 本次跳过, 下次重试')
            continue
        new_rows.append({
            'scan_ts':      datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
            'ticker':       tkr,
            'signal_date':  dstr,
            'catalyst':     res['catalyst'],
            'catalyst_type': res['catalyst_type'],
            'tone':         res['tone'],
            'x_activity':   res['x_activity'],
            'driver':       res['driver'],
            'evidence_dates': '|'.join(res['evidence_dates'] or []),
            'cost_usd':     cost,
        })
        marks[(tkr, dstr)] = _GROK_MARK.get(res['catalyst'], '?') + \
            (res['catalyst_type'] if res['catalyst'] == 'substantive' else '')
    if new_rows:
        os.makedirs(os.path.dirname(GROK_LEDGER), exist_ok=True)
        add = pd.DataFrame(new_rows)
        (add if led.empty else pd.concat([led, add], ignore_index=True)) \
            .to_csv(GROK_LEDGER, index=False, encoding='UTF-8')
        logging.info(f'grok ledger +{len(new_rows)} → {GROK_LEDGER}')
    return marks


# ── 模式一: 池子扫描 ─────────────────────────────────────────────────────────
def _load_universe(name: str) -> list:
    path = os.path.join(UNIVERSE_DIR, f'{name}.json')
    if not os.path.exists(path):                    # 缓存缺失时现抓 (含当日缓存)
        from t_us_undervalue import load_universe
        return load_universe(name, force=False)
    with open(path) as f:
        return json.load(f)


def run_scan(universe, lookback, chg_min, low_within, min_price, min_dvol,
             min_volx, min_mktcap, asof=None, grok=False, grok_max=6):
    tickers = _load_universe(universe)
    caps = load_mktcap() if min_mktcap > 0 else {}
    asof_txt = f', as-of {asof.date()}' if asof is not None else ''
    logging.info(f'scan universe={universe} ({len(tickers)} 只), '
                 f'近{lookback}日, 涨≥{chg_min}%, 低点≤{low_within}日前, '
                 f'价≥${min_price}, 额≥${min_dvol/1e6:.0f}M, 量≥{min_volx}×, '
                 f'市值≥${min_mktcap/1e6:.0f}M{asof_txt}')
    rows = []
    for k, t in enumerate(tickers):
        try:
            cap = caps.get(t)
            if caps and min_mktcap > 0 and cap is not None and cap < min_mktcap:
                continue                             # 市值已知且过小; 未知 → 放行标 ?
            df = _fetch_daily(t)
            if df.empty:
                continue
            if asof is not None:
                df = df[df.index <= asof]
            if len(df) < MIN_BARS:
                continue
            for i in range(max(len(df) - lookback, 3), len(df)):
                hit, row, _ = check_day(df, i, chg_min, low_within,
                                        min_price, min_dvol, min_volx)
                if hit:
                    row['ticker'] = t
                    row['cap$B'] = round(cap / 1e9, 2) if cap else np.nan
                    row['今日'] = '★' if i == len(df) - 1 else ''
                    rows.append(row)
        except Exception as e:
            logging.warning(f'{t}: {e}')
        if (k + 1) % 200 == 0:
            logging.info(f'  ...{k + 1}/{len(tickers)}')

    tag = (asof or pd.Timestamp(datetime.date.today())).strftime('%Y%m%d')
    if not rows:
        print(f'\n近 {lookback} 个交易日无命中 (universe={universe}{asof_txt}): '
              f'无"250日新低≤{low_within}日前 + 单日≥{chg_min}%"且过流动性门的名。')
        return
    cols = ['date', '今日', 'ticker', 'close', 'chg%', 'low250', 'low_ago',
            'vs_low%', 'vol_x', 'dvol$M', 'cap$B', 'stop']
    out = pd.DataFrame(rows)[cols].sort_values(
        ['date', 'chg%'], ascending=[False, False]).reset_index(drop=True)
    if grok:
        if asof is not None:
            logging.warning('--grok 在 --asof 下禁用: X 检索做不到点位口径, '
                            '事后标注会污染前向样本 (标注积累 live-only)')
        else:
            marks = grok_annotate(out, grok_max)
            out['催化剂'] = [marks.get((r['ticker'], str(r['date'])), '')
                          for _, r in out.iterrows()]
    today_n = int((out['今日'] == '★').sum())
    print(f"\n250日新低·首日反弹  (universe={universe}{asof_txt}, "
          f"近{lookback}日命中 {len(out)}, 其中当日★ {today_n})")
    print(tab_mod.tabulate(out, headers='keys', tablefmt='github', showindex=False))
    print(f"\n列义: chg%=信号日收盘涨幅 · low_ago=250日最低价在几天前(1=前一日) · "
          f"vs_low%=收盘距250日低 · vol_x=当日量/20日均量 · dvol$M=20日均成交额 · "
          f"cap$B=当前市值(非点位口径{', 回测时仅供参考' if asof is not None else ''}"
          f"; 空=screener未收录, 未做门槛) · stop=参考止损=刚砸出的250日低 "
          f"(日收盘跌破=反弹证伪, ADR-0002)。")
    print("解读: 候选发现层, 非验证信号源 — 最后一跌后的第一根反抗阳线, 两极分化剧烈; "
          "质量门是防归零过滤器 (docs §7.5-7.6), 建议对照 bottom_entry 的 ROE 分层 "
          "与 key_kline 再做进场决定。")
    if grok and asof is None and '催化剂' in out.columns:
        print("催化剂列 (Grok X/新闻复核, 标注积累·不门控, n=1 假说 SSTK/VSTM): "
              "✓类别=有实质催化剂讨论 · 💬=只有喊单闲聊 · ✗=无人问津 · ?=不明; "
              f"样本账本 {GROK_LEDGER}")

    out_dir = os.path.join(RESULT_DIR, 'us_low_bounce')
    os.makedirs(out_dir, exist_ok=True)
    csv = os.path.join(out_dir, f'us_low_bounce_{universe}_{tag}.csv')
    try:
        out.to_csv(csv, index=False, encoding='UTF-8')
        logging.info(f'saved {csv} ({len(out)} 行)')
    except Exception as e:
        logging.warning(f'写 CSV 失败: {e}')


# ── 模式二: 单票诊断 ─────────────────────────────────────────────────────────
def run_ticker(ticker, chg_min, low_within, min_price, min_dvol, min_volx,
               asof=None):
    df = _fetch_daily(ticker)
    if df.empty:
        logging.error(f'{ticker}: 无数据')
        return
    if asof is not None:
        df = df[df.index <= asof]
    if len(df) < MIN_BARS:
        logging.error(f'{ticker}: 数据不足 ({len(df)} < {MIN_BARS})')
        return
    hit, row, fails = check_day(df, len(df) - 1, chg_min, low_within,
                                min_price, min_dvol, min_volx)
    d = df.index[-1].date()
    print(f'\n{ticker} @ {d}: ' + ('命中 ✓' if hit else '未命中'))
    if hit:
        print(tab_mod.tabulate(pd.DataFrame([row]), headers='keys',
                               tablefmt='github', showindex=False))
    else:
        for f in fails:
            print(f'  ✗ {f}')
    # 历史命中(全history, 只按形态+价格/量门, 帮助看该票此形态的过往兑现)
    hist = []
    for i in range(3, len(df) - 1):
        h, r, _ = check_day(df, i, chg_min, low_within, min_price, min_dvol, min_volx)
        if h:
            r['此后至今%'] = round((df['close'].iloc[-1] / r['close'] - 1) * 100, 1)
            hist.append(r)
    if hist:
        print(f'\n历史命中 {len(hist)} 次:')
        print(tab_mod.tabulate(pd.DataFrame(hist), headers='keys',
                               tablefmt='github', showindex=False))


def main():
    parser = OptionParser(usage='%prog --scan [--universe all] | --ticker SYM  [--asof YYYY-MM-DD]')
    parser.add_option('--scan', action='store_true', dest='scan', default=False)
    parser.add_option('--ticker', dest='ticker', default=None)
    parser.add_option('--universe', dest='universe', default='all',
                      help='both | ndx | sp500 | r2000ht | all (default all)')
    parser.add_option('--lookback', dest='lookback', type=int, default=LOOKBACK,
                      help=f'--scan 报告最近 N 个交易日的命中 (default {LOOKBACK})')
    parser.add_option('--chg', dest='chg', type=float, default=CHG_MIN,
                      help=f'信号日最小涨幅%% (default {CHG_MIN})')
    parser.add_option('--low-days', dest='low_days', type=int, default=LOW_WITHIN,
                      help=f'250日低须在前 N 个交易日内 (default {LOW_WITHIN})')
    parser.add_option('--min-price', dest='min_price', type=float, default=MIN_PRICE)
    parser.add_option('--min-dvol', dest='min_dvol', type=float, default=MIN_DVOL,
                      help=f'20日均成交额下限$ (default {MIN_DVOL:.0f})')
    parser.add_option('--min-volx', dest='min_volx', type=float, default=MIN_VOLX)
    parser.add_option('--min-mktcap', dest='min_mktcap', type=float, default=MIN_MKTCAP,
                      help=f'市值下限$ (default {MIN_MKTCAP:.0f}; 0=关闭)')
    parser.add_option('--asof', dest='asof', default=None,
                      help='回测: 只用到该日为止的数据 (YYYY-MM-DD)')
    parser.add_option('--grok', action='store_true', dest='grok', default=False,
                      help='--scan: 对命中跑 Grok 催化剂复核 (~$0.1/票, 标注积累·不门控; '
                           '按 ticker×信号日去重, --asof 下自动禁用)')
    parser.add_option('--grok-max', dest='grok_max', type=int, default=6,
                      help='单次运行 Grok 调用上限 (default 6)')
    opt, _ = parser.parse_args()

    asof = pd.Timestamp(opt.asof) if opt.asof else None
    if opt.ticker:
        run_ticker(opt.ticker.upper(), opt.chg, opt.low_days, opt.min_price,
                   opt.min_dvol, opt.min_volx, asof)
    elif opt.scan:
        if opt.universe not in ('both', 'ndx', 'sp500', 'all', 'r2000ht'):
            print('--universe 仅支持 both|ndx|sp500|all|r2000ht'); sys.exit(1)
        run_scan(opt.universe, opt.lookback, opt.chg, opt.low_days, opt.min_price,
                 opt.min_dvol, opt.min_volx, opt.min_mktcap, asof,
                 grok=opt.grok, grok_max=opt.grok_max)
    else:
        parser.error('give --scan or --ticker SYM')


if __name__ == '__main__':
    main()
