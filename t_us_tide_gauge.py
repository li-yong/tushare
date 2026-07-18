# coding: utf-8
"""
US tide gauge (潮位仪·真水量) — the first non-price tide instrument
(docs/tide_wave_wind.md §6 落地方向二; rule_registry #22). 提示不门控.

框架论断: 短期波动是能量现象, 只有潮是水量现象 — 但 regime_monitor 的潮向
仍是价格代理 (拿浪高测潮位)。本脚本直接测水, 三个断面:

  水源 (money side, 真水量) — Fed H.4.1 净流动性 = WALCL 总资产 − TGA 财政部
      账户 − ON RRP 私方逆回购。官方 DDP zip 免 key (fredgraph 对本机不可达,
      FRED API 需 key; H.4.1 是这三个序列的原始出处, 每周四发布周三数据)。
      价值在与价格潮位的背离: 价格在 20 周线上但水在抽 = 2022H1 形态。
  水位 (asset side) — 全市场总市值, Nasdaq screener 全表 marketCap 求和
      (仅个股, 不含 ETF, 无基金双计; 含 ADR/多股类别的少量口径噪声)。
      认识论: 市值是水位 = 潮×浪的合成读数, 不是水量 — Δ市值 ≈ 重定价(能量)
      + 净发行(真水), 分解随快照积累。
  水流 (flow side) — "水从哪里流到哪里": ETF 份额变动 × 价格 = 一级市场创赎
      美元额 (真金白银, 非价格涨跌)。Yahoo 无 ETF 份额历史 → 前向积累快照
      (与 intraday_internals/grok_catalyst 同款路数), 第二次运行起出流向表。
      跨资产篮子: 宽基股/板块/债/现金类/金 — 股债现金间的搬家才是水量转移,
      板块间的"轮动"多半是能量传递 (那是 sector_rotation 的岗位)。

每周日 us_weekly_run.sh 运行 (H.4.1 周更, 市值/流向看周尺度)。输出
result/us_tide_gauge/us_tide_gauge_<date>.txt + 三份历史 CSV (同目录)。

Usage:
  python t_us_tide_gauge.py            # weekly run (H41 缓存 ≤3d 直接用)
  python t_us_tide_gauge.py --force    # 强制重取 H.4.1 zip 与 screener
"""

import io
import os
import sys
import json
import logging
import zipfile
import datetime
import urllib.request
import xml.etree.ElementTree as ET
from optparse import OptionParser

import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)

OUT_DIR   = '/home/ryan/DATA/result/us_tide_gauge'
H41_CSV   = os.path.join(OUT_DIR, 'us_h41_history.csv')
CAP_CSV   = os.path.join(OUT_DIR, 'us_cap_history.csv')
FLOW_CSV  = os.path.join(OUT_DIR, 'us_flow_snapshots.csv')
H41_URL   = ('https://www.federalreserve.gov/datadownload/Output.aspx'
             '?rel=H41&filetype=zip')
H41_MAX_AGE_D = 3          # cached parse younger than this = reuse (周四发布)
UA = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

# H.4.1 DDP mnemonics (verified 2026-07-14 against published levels):
#   RESPPA_N.WW    = Assets: total assets                      (FRED WALCL)
#   RESPPLLDT_N.WW = Deposits: U.S. Treasury, General Account  (FRED WTREGEN)
#   RESPPLLRD_N.WW = Reverse repos: others (private ON RRP)    (FRED RRPONTSYD)
#   (RESPPLLRF = foreign-official repo pool — 常年 $300-400B 的存量池, 不计)
H41_SERIES = {'RESPPA_N.WW': 'walcl', 'RESPPLLDT_N.WW': 'tga',
              'RESPPLLRD_N.WW': 'rrp'}

# 水流篮子: 类别内是"水的容器", 类别间搬家 = 真水量转移。
FLOW_BASKET = {
    'EQ_BROAD': ['SPY', 'QQQ', 'VTI', 'IWM'],
    'EQ_SECTOR': ['XLK', 'SMH', 'IGV', 'XBI', 'XLE', 'XLF', 'XLV', 'XLP',
                  'XLU', 'XLY', 'XLI', 'XLC', 'XLRE', 'XLB'],
    'BOND': ['TLT', 'IEF', 'AGG', 'LQD', 'HYG'],
    'CASH': ['BIL', 'SGOV', 'SHV'],
    'GOLD': ['GLD'],
    'INTL': ['EFA', 'EEM'],
}
CLASS_CN = {'EQ_BROAD': '宽基股', 'EQ_SECTOR': '板块股', 'BOND': '债',
            'CASH': '现金类', 'GOLD': '金', 'INTL': '海外股'}


# ── 水源: Fed H.4.1 净流动性 ──────────────────────────────────────────────────

def fetch_h41(force: bool) -> pd.DataFrame:
    """Full weekly history [walcl, tga, rrp, netliq] in $bn, date-indexed."""
    if (not force and os.path.exists(H41_CSV)
            and (datetime.date.today()
                 - datetime.date.fromtimestamp(os.path.getmtime(H41_CSV))).days
            < H41_MAX_AGE_D):
        logging.info(f'H41: using cached parse {H41_CSV}')
        return pd.read_csv(H41_CSV, index_col=0, parse_dates=True)

    logging.info('H41: downloading DDP zip (~9MB) …')
    req = urllib.request.Request(H41_URL, headers=UA)
    raw = urllib.request.urlopen(req, timeout=180).read()
    zf = zipfile.ZipFile(io.BytesIO(raw))
    xml_name = next(n for n in zf.namelist() if n.endswith('_data.xml'))

    # SDMX can split one logical series over several <Series> blocks — collect
    # observations per mnemonic across all of them.
    obs: dict[str, dict] = {c: {} for c in H41_SERIES.values()}
    with zf.open(xml_name) as f:
        for _, el in ET.iterparse(f, events=('end',)):
            if el.tag.split('}')[-1] != 'Series':
                continue
            col = H41_SERIES.get(el.get('SERIES_NAME'))
            if col:
                for o in el:
                    if o.tag.split('}')[-1] == 'Obs':
                        v = o.get('OBS_VALUE')
                        if v not in (None, '', 'ND'):
                            obs[col][o.get('TIME_PERIOD')] = float(v)
            el.clear()

    df = pd.DataFrame(obs)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index() / 1000.0            # $mn → $bn
    df = df.dropna(subset=['walcl'])
    df[['tga', 'rrp']] = df[['tga', 'rrp']].fillna(0.0)
    df['netliq'] = df['walcl'] - df['tga'] - df['rrp']
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(H41_CSV)
    logging.info(f'H41: {len(df)} weekly obs {df.index[0].date()} → '
                 f'{df.index[-1].date()} → {H41_CSV}')
    return df


def netliq_readings(h41: pd.DataFrame) -> dict:
    """Levels + Δ over 4/13/26 weekly obs, verdict = sign of Δ13w."""
    r = {'date': h41.index[-1].date()}
    for col in ('walcl', 'tga', 'rrp', 'netliq'):
        s = h41[col]
        r[col] = s.iloc[-1]
        for w in (4, 13, 26):
            r[f'{col}_d{w}w'] = (s.iloc[-1] - s.iloc[-1 - w]
                                 if len(s) > w else None)
    d13 = r.get('netliq_d13w')
    r['verdict'] = None if d13 is None else ('RISING' if d13 > 0 else 'EBBING')
    return r


# ── 水位: 全市场总市值 ────────────────────────────────────────────────────────

def fetch_total_cap() -> tuple[float, dict, int]:
    """(total $, {sector: $}, n_names) from the Nasdaq screener full table."""
    req = urllib.request.Request(
        'https://api.nasdaq.com/api/screener/stocks?download=true', headers=UA)
    rows = json.loads(urllib.request.urlopen(req, timeout=90).read())['data']['rows']
    total, n, by_sector = 0.0, 0, {}
    for r in rows:
        try:
            cap = float(r.get('marketCap') or 0)
        except (TypeError, ValueError):
            continue
        if cap <= 0:
            continue
        total += cap
        n += 1
        s = (r.get('sector') or 'Unknown').strip() or 'Unknown'
        by_sector[s] = by_sector.get(s, 0.0) + cap
    if n < 3000:
        raise ValueError(f'screener returned only {n} capped names — 不落历史')
    return total, by_sector, n


def append_cap_history(today: str, total: float, by_sector: dict, n: int) -> pd.DataFrame:
    """One row per run date (rerun same day = overwrite); returns full history."""
    rec = {'date': today, 'n_names': n, 'total': total}
    rec.update({f'sec_{k}': v for k, v in by_sector.items()})
    hist = (pd.read_csv(CAP_CSV) if os.path.exists(CAP_CSV)
            else pd.DataFrame())
    hist = hist[hist.get('date') != today] if len(hist) else hist
    hist = pd.concat([hist, pd.DataFrame([rec])], ignore_index=True)
    hist = hist.sort_values('date').reset_index(drop=True)
    os.makedirs(OUT_DIR, exist_ok=True)
    hist.to_csv(CAP_CSV, index=False)
    return hist


# ── 水流: ETF 创赎估算 (前向积累) ─────────────────────────────────────────────

def snapshot_flows(today: str) -> pd.DataFrame:
    """Snapshot SO/AUM/price for the basket; append; return full history."""
    import yfinance as yf
    recs = []
    for cls, tickers in FLOW_BASKET.items():
        for t in tickers:
            try:
                info = yf.Ticker(t).info
                recs.append({
                    'date': today, 'ticker': t, 'cls': cls,
                    'so': info.get('sharesOutstanding'),
                    'aum': info.get('totalAssets'),
                    'price': info.get('previousClose') or info.get('navPrice'),
                })
            except Exception as e:
                logging.warning(f'flow snapshot {t}: {e}')
    hist = (pd.read_csv(FLOW_CSV) if os.path.exists(FLOW_CSV)
            else pd.DataFrame())
    if len(hist):
        hist = hist[hist['date'] != today]
    hist = pd.concat([hist, pd.DataFrame(recs)], ignore_index=True)
    hist = hist.sort_values(['ticker', 'date']).reset_index(drop=True)
    os.makedirs(OUT_DIR, exist_ok=True)
    hist.to_csv(FLOW_CSV, index=False)
    return hist


def compute_flows(hist: pd.DataFrame, today: str) -> pd.DataFrame | None:
    """Per ticker: flow $ between the two latest snapshot dates.

    首选 ΔSO × 现价 (一级市场创赎的直接量); 缺 SO 的容器退回 AUM 分解
    flow ≈ AUM1 − AUM0×(P1/P0) (剔除价格重定价后的残差)。
    """
    dates = sorted(hist['date'].unique())
    if len(dates) < 2 or dates[-1] != today:
        return None
    d1, d0 = dates[-1], dates[-2]
    a = hist[hist['date'] == d0].set_index('ticker')
    b = hist[hist['date'] == d1].set_index('ticker')
    out = []
    for t in b.index.intersection(a.index):
        r0, r1 = a.loc[t], b.loc[t]
        flow, how = None, None
        if pd.notna(r1['so']) and pd.notna(r0['so']) and pd.notna(r1['price']):
            flow, how = (r1['so'] - r0['so']) * r1['price'], 'ΔSO'
        elif (pd.notna(r1['aum']) and pd.notna(r0['aum'])
              and pd.notna(r1['price']) and pd.notna(r0['price']) and r0['price'] > 0):
            flow, how = r1['aum'] - r0['aum'] * (r1['price'] / r0['price']), 'ΔAUM'
        if flow is None:
            continue
        out.append({'ticker': t, 'cls': r1['cls'], 'flow': flow, 'how': how,
                    'aum': r1['aum'],
                    'pct': flow / r1['aum'] * 100 if pd.notna(r1['aum']) and r1['aum'] else None})
    if not out:
        return None
    df = pd.DataFrame(out)
    df.attrs['window'] = f'{d0} → {d1}'
    return df.sort_values('flow', ascending=False)


# ── Report ────────────────────────────────────────────────────────────────────

def _bn(v, unit=1.0):
    return f'${v * unit / 1000:,.2f}T' if v is not None else '—'


def _dbn(v):
    return f'{v:+,.0f}bn' if v is not None and pd.notna(v) else '—'


def main():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--force', dest='force', action='store_true', default=False,
                      help='强制重取 H.4.1 zip 与 screener (无视缓存)')
    opts, _ = parser.parse_args()

    today = datetime.date.today().isoformat()
    lines = []

    def p(*args):
        line = ' '.join(str(a) for a in args)
        lines.append(line)
        print(line)

    p()
    p('=' * 78)
    p(f'  US TIDE GAUGE 潮位仪·真水量  —  {today}   '
      f'(潮浪风框架落地二, 提示不门控)')
    p('=' * 78)

    # 水源
    p()
    try:
        h41 = fetch_h41(opts.force)
        r = netliq_readings(h41)
        p(f'[ 水源 净流动性 (Fed H.4.1, 数据至 {r["date"]}) ]')
        rows = []
        for col, name, note in (('walcl', 'Fed总资产 WALCL', ''),
                                ('tga', '财政部 TGA', '(升=抽水)'),
                                ('rrp', 'ON RRP 私方', '(升=抽水)'),
                                ('netliq', '净流动性 =A−T−R', '')):
            rows.append(f'  {name:<18} {_bn(r[col]):>9}   '
                        f'Δ4w {_dbn(r[f"{col}_d4w"]):>9} · '
                        f'Δ13w {_dbn(r[f"{col}_d13w"]):>9} · '
                        f'Δ26w {_dbn(r[f"{col}_d26w"]):>9}  {note}')
        for line in rows:
            p(line)
        verdict_cn = {'RISING': '注水 (潮涨)', 'EBBING': '抽水 (潮退)'}.get(r['verdict'], '未知')
        d13 = r.get('netliq_d13w')
        pct = (f' (Δ13w {d13 / r["netliq"] * 100:+.1f}% — 贴零读数看幅度, '
               f'真抽水的量级: 2022-03 -3.6% / 2022-06 -11.3%)' if d13 is not None else '')
        p(f'  → 水量潮向 (Δ13w 符号): 【{r["verdict"] or "?"}】 {verdict_cn}{pct}')
        # 与价格潮位对照 — 背离才是本仪器的价值所在 (2022H1: 价格在线上, 水在抽)
        try:
            import sea_state
            tide = sea_state._read_tide()
            if tide['tide'] and r['verdict']:
                if tide['tide'] == r['verdict']:
                    p(f'  潮向对照: 水量仪 {r["verdict"]} ≡ 价格潮位 {tide["tide"]} '
                      f'(regime {tide["state"]}, {tide["date"]}) — 一致')
                else:
                    p(f'  ⚠ 潮向背离: 水量仪 {r["verdict"]} ≠ 价格潮位 {tide["tide"]} '
                      f'(regime {tide["state"]}, {tide["date"]}) — 2022H1 型信号, 记入周日复盘')
        except Exception as e:
            logging.info(f'tide cross-check skipped ({e})')
    except Exception as e:
        logging.warning(f'水源 H.4.1 失败: {e}')
        p('[ 水源 净流动性 ]  ⚠ H.4.1 不可用, 本周跳过 (历史缓存见 us_h41_history.csv)')

    # 水位
    p()
    try:
        total, by_sector, n = fetch_total_cap()
        hist = append_cap_history(today, total, by_sector, n)
        p(f'[ 水位 全市场总市值 (Nasdaq screener, {n} 只个股, 不含ETF) ]')
        p(f'  TOTAL {_bn(total / 1e9):>9}    板块前5: '
          + ' · '.join(f'{k} {_bn(v / 1e9)}'
                       for k, v in sorted(by_sector.items(), key=lambda kv: -kv[1])[:5]))
        if len(hist) >= 2:
            prev = hist.iloc[-2]
            dcap = (total - prev['total']) / 1e9
            p(f'  Δ since {prev["date"]}: {_dbn(dcap)}'
              f'  ({(total / prev["total"] - 1) * 100:+.1f}%)')
        else:
            p(f'  历史: 首次快照, 基线已落 {CAP_CSV}')
        p('  注: 市值 = 水位 (潮×浪合成读数) ≠ 水量; Δ市值 ≈ 重定价(能量) + 净发行(真水)。')
        p('      快照积累后减去指数收益的残差 ≈ 净发行 — IPO/增发多 = 有人在浪头卖水。')
    except Exception as e:
        logging.warning(f'水位 screener 失败: {e}')
        p('[ 水位 全市场总市值 ]  ⚠ screener 不可用, 本周跳过')

    # 水流
    p()
    try:
        fh = snapshot_flows(today)
        flows = compute_flows(fh, today)
        n_snap = fh['date'].nunique()
        p(f'[ 水流 ETF 创赎估算 (前向积累, 第 {n_snap} 次快照) ]')
        if flows is None:
            p(f'  基线已落 {FLOW_CSV} ({fh[fh["date"] == today]["ticker"].nunique()} 只容器:'
              f' 宽基股/板块/债/现金类/金/海外)。下次运行起报告流向。')
        else:
            p(f'  窗口 {flows.attrs["window"]} · flow = ΔSO×价 (缺SO用 ΔAUM−价格效应残差)')
            cls_sum = flows.groupby('cls')['flow'].sum().sort_values(ascending=False)
            p('  类别间搬家 (真水量转移):')
            for cls, v in cls_sum.items():
                p(f'    {CLASS_CN.get(cls, cls):<6} {v / 1e9:+8.1f}bn')
            top_in = flows.head(5)
            top_out = flows.tail(5).iloc[::-1]
            p('  流入前5: ' + ' · '.join(
                f'{r.ticker} {r.flow / 1e9:+.1f}bn' for r in top_in.itertuples()))
            p('  流出前5: ' + ' · '.join(
                f'{r.ticker} {r.flow / 1e9:+.1f}bn' for r in top_out.itertuples()))
            p('  注: 一级市场创赎 ≈ 真金流向; 板块间轮动多半是能量传递, 那是 sector_rotation 的岗位。')
    except Exception as e:
        logging.warning(f'水流快照失败: {e}')
        p('[ 水流 ETF 创赎估算 ]  ⚠ 快照失败, 本周跳过')

    p()
    p('[ 读法 ]')
    p('  水源 = 真水量 (钱端): 净流动性 Δ13w 定潮向; 与价格潮位背离 > 一致 (背离才有新信息)')
    p('  水位 = 潮×浪合成: 只看趋势别当水量; 水流 = 容器间搬家, 股↔债↔现金 才是水量转移')
    p('  全部提示不门控; sea_state 的潮向仍以 regime 快照为准, 换轨须过 registry #22 复验')
    p()

    os.makedirs(OUT_DIR, exist_ok=True)
    out_file = os.path.join(OUT_DIR, f'us_tide_gauge_{today}.txt')
    with open(out_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    logging.info(f'Tide gauge report → {out_file}')


if __name__ == '__main__':
    main()
