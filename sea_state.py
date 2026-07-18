# coding: utf-8
"""
Sea state (海况) — the live 潮×风 quadrant from the 潮浪风 framework
(docs/tide_wave_wind.md; rule_registry #21). 提示不门控: this module only
labels, it gates nothing.

Tide (潮) — the slow layer's verdict, from the regime monitor's live snapshot
(us_regime_state.json): BULL/CAUTION = RISING, WATCH/DEFEND = EBBING. Until a
true water gauge exists this is still a price-derived proxy; the snapshot is
live-only, so an --asof caller must not read it (look-ahead) — same contract
as the regime gate in t_us_tech_swing.

Wind (风) — narrative/risk-appetite direction, from the latest sector-rotation
table: median 21d relative return of the offensive ETFs minus the defensive
ETFs (columns already computed by t_us_sector_rotation; nothing new invented).
Positive = RISK_ON.

Quadrants:
  RISING × RISK_ON   → ALIGNED    風潮同向  (huice 正期望的来源海况)
  RISING × RISK_OFF  → CHOP       風頂潮    (碎浪区 — MIXED 负期望的近亲)
  EBBING × RISK_ON   → EBB_RALLY  大風退潮  (熊市叙事反弹, 浪再高水位在降)
  EBBING × RISK_OFF  → EBB        風潮同退  (全防守)
Any missing/stale input → UNKNOWN (never guess a quadrant).

Consumers: t_us_tech_swing morning-report banner (live only) and
signal_ledger.log_signals, which stamps every new episode's sea_state so
t_us_signal_attrib can group forward outcomes by quadrant — turning the
framework from metaphor into a falsifiable forward record.
"""

import os
import glob
import json
import logging
import datetime

import pandas as pd

REGIME_STATE_FILE = '/home/ryan/DATA/result/us_regime_monitor/us_regime_state.json'
ROTATION_DIR      = '/home/ryan/DATA/result/us_sector_rotation'
TIDE_MAX_AGE_D    = 5    # regime snapshot older than this (calendar days) = stale
WIND_MAX_AGE_D    = 7    # newest rotation CSV older than this = stale

# Offense/defense split of the rotation universe (classic risk-on/risk-off
# pairs; commodity/inflation ETFs excluded — they are neither).
OFFENSE = ['SMH', 'XLK', 'IGV', 'XLC', 'XLY', 'XBI']
DEFENSE = ['XLP', 'XLU', 'XLV', 'XLRE']

QUADRANT = {
    ('RISING', 'RISK_ON'):  'ALIGNED',
    ('RISING', 'RISK_OFF'): 'CHOP',
    ('EBBING', 'RISK_ON'):  'EBB_RALLY',
    ('EBBING', 'RISK_OFF'): 'EBB',
}
LABEL_CN = {'ALIGNED': '風潮同向', 'CHOP': '風頂潮',
            'EBB_RALLY': '大風退潮', 'EBB': '風潮同退', 'UNKNOWN': '海况未知'}
HINT = {
    'ALIGNED':   '黄金海况 — huice 正期望集中于此 (仍是假说, ledger 分组积累中)',
    'CHOP':      '碎浪区 — 潮涨但风逆, 顺势信号期望走低, 优先观察',
    'EBB_RALLY': '浪再高水位在降 — 叙事反弹, 与熊反免疫期同款警觉',
    'EBB':       '全防守 — 与体制门控方向一致',
    'UNKNOWN':   '输入缺失/过期 — 不猜象限',
}

_memo: dict | None = None   # per-process cache: banner + ledger stamp share one read


def _read_tide() -> dict:
    """{'tide': RISING|EBBING|None, 'state': ..., 'date': ...}"""
    out = {'tide': None, 'state': None, 'date': None}
    try:
        with open(REGIME_STATE_FILE, encoding='UTF-8') as f:
            st = json.load(f)
        age = (datetime.date.today() - pd.Timestamp(st['date']).date()).days
        if age > TIDE_MAX_AGE_D:
            logging.info(f'sea_state: regime 快照过期 ({st["date"]}, {age}d) — 潮向未知')
            return out
        out.update(state=st['state'], date=st['date'],
                   tide='RISING' if st['state'] in ('BULL', 'CAUTION') else 'EBBING')
    except Exception as e:
        logging.info(f'sea_state: regime 快照不可读 ({e}) — 潮向未知')
    return out


def _read_wind() -> dict:
    """{'wind': RISK_ON|RISK_OFF|None, 'score': off−def rel21, 'date': ...}"""
    out = {'wind': None, 'score': None, 'date': None}
    try:
        files = sorted(glob.glob(os.path.join(ROTATION_DIR, 'us_sector_rotation_*.csv')))
        if not files:
            return out
        latest = files[-1]
        date = os.path.basename(latest)[len('us_sector_rotation_'):-len('.csv')]
        age = (datetime.date.today() - pd.Timestamp(date).date()).days
        if age > WIND_MAX_AGE_D:
            logging.info(f'sea_state: rotation 表过期 ({date}, {age}d) — 风向未知')
            return out
        t = pd.read_csv(latest, index_col=0)
        off = t.loc[t.index.intersection(OFFENSE), 'rel21'].median()
        dfn = t.loc[t.index.intersection(DEFENSE), 'rel21'].median()
        if pd.isna(off) or pd.isna(dfn):
            return out
        score = float(off - dfn)
        out.update(score=score, date=date,
                   wind='RISK_ON' if score > 0 else 'RISK_OFF')
    except Exception as e:
        logging.info(f'sea_state: rotation 表不可读 ({e}) — 风向未知')
    return out


def current_sea_state(refresh: bool = False) -> dict:
    """Live sea-state reading; memoised per process. Never raises."""
    global _memo
    if _memo is not None and not refresh:
        return _memo
    tide, wind = _read_tide(), _read_wind()
    ss = QUADRANT.get((tide['tide'], wind['wind']), 'UNKNOWN')
    _memo = {
        'sea_state': ss, 'label_cn': LABEL_CN[ss], 'hint': HINT[ss],
        'tide': tide['tide'], 'tide_state': tide['state'], 'tide_date': tide['date'],
        'wind': wind['wind'], 'wind_score': wind['score'], 'wind_date': wind['date'],
    }
    return _memo


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    s = current_sea_state()
    print(f"海况: {s['label_cn']} {s['sea_state']}")
    print(f"  潮 = {s['tide']} (regime {s['tide_state']}, {s['tide_date']})")
    sc = f"{s['wind_score']:+.1%}" if s['wind_score'] is not None else '—'
    print(f"  风 = {s['wind']} (off−def rel21 {sc}, rotation {s['wind_date']})")
    print(f"  ↳ {s['hint']}")
