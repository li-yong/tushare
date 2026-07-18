# coding: utf-8
"""
Signal attribution ledger (信号归因日志) — shared writer.

One CSV, one row per signal EPISODE: the first day a scanner emits a Setup for
a ticker opens an episode and freezes entry/stop/target at that day's values
(the actionable moment); the same signal re-emitted on following days only
refreshes last_seen. After EPISODE_GAP_D calendar days of silence, the next
emission opens a new episode. Append/refresh only — rows are never deleted, so
the ledger accumulates the system's real signal history.

Why this exists (2026-07 strategy review, priority 5): the question "which
screen actually has edge, and which is repackaged theme beta?" is unanswerable
without a record of what was signalled when. Twelve months of this ledger is a
real track record; no new screen is worth more than that feedback loop.

Writers call log_signals(rows, source=...); t_us_tech_swing.py is wired in.
Other scanners join by passing their own source name — extra dict keys are
ignored, missing ones are left blank, so schemas can differ per scanner.
Backtest (--asof) runs must NOT write: the ledger is only meaningful if every
row was emitted live, before the outcome was knowable.

Read side: t_us_signal_attrib.py computes forward outcomes per source×type.
"""

import os
import logging
import datetime

import pandas as pd

LEDGER_DIR    = '/home/ryan/DATA/result/us_signal_log'
LEDGER_PATH   = os.path.join(LEDGER_DIR, 'us_signal_ledger.csv')
EPISODE_GAP_D = 7   # calendar days of silence (≈5 sessions) → next emission
                    # opens a new episode instead of refreshing the old one

# Fixed column order; unknown keys in incoming rows are dropped, missing keys
# stay blank. first_seen/last_seen are managed here, never by the caller.
COLUMNS = [
    'first_seen', 'last_seen', 'source', 'ticker', 'signal_type',
    'confidence', 'market_state', 'sea_state', 'close', 'entry', 'stop',
    'target', 'rr', 'rr_ok', 'er_days', 'er_blackout', 'gap_tier',
]


def _load(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=COLUMNS)
    df = pd.read_csv(path)
    for c in COLUMNS:            # tolerate a ledger written by an older schema
        if c not in df.columns:
            df[c] = None
    return df


def log_signals(rows: list[dict], source: str,
                path: str = LEDGER_PATH,
                today: str | None = None) -> tuple[int, int]:
    """Record today's emitted signals; returns (n_new_episodes, n_refreshed).

    Each row needs at least ticker + signal_type; everything else in COLUMNS is
    optional. An episode key is (source, ticker, signal_type): if its latest
    row was seen within EPISODE_GAP_D days we refresh that row's last_seen
    (entry/stop stay frozen at first_seen — the day the signal was actionable);
    otherwise we open a new episode.
    """
    if not rows:
        return 0, 0
    today = today or datetime.date.today().isoformat()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = _load(path)

    # 海况 stamp (潮浪风框架, docs/tide_wave_wind.md): frozen at episode open so
    # attribution can group by quadrant. Stamped centrally here so every writer
    # gets it; callers are live-only (--asof runs never reach this function),
    # which keeps the label point-in-time honest. Failure = blank, never fatal.
    try:
        import sea_state
        _ss = sea_state.current_sea_state()['sea_state']
    except Exception as e:
        logging.info(f'sea_state unavailable ({e}) — ledger rows left blank')
        _ss = None
    for row in rows:
        row.setdefault('sea_state', _ss)

    n_ref, new_recs = 0, []
    for row in rows:
        ticker = row.get('ticker')
        stype  = row.get('signal_type')
        if not ticker or not stype:
            continue
        mask = ((df['source'] == source) & (df['ticker'] == ticker)
                & (df['signal_type'] == stype))
        idx = df.index[mask]
        if len(idx):
            last_i = idx[-1]
            gap = (pd.Timestamp(today)
                   - pd.Timestamp(df.at[last_i, 'last_seen'])).days
            if 0 <= gap <= EPISODE_GAP_D:
                df.at[last_i, 'last_seen'] = today
                n_ref += 1
                continue
        rec = {c: row.get(c) for c in COLUMNS}
        rec.update({'first_seen': today, 'last_seen': today, 'source': source})
        new_recs.append(rec)

    n_new = len(new_recs)
    if new_recs:
        add = pd.DataFrame(new_recs, columns=COLUMNS)
        df = add if df.empty else pd.concat([df, add], ignore_index=True)
    df.to_csv(path, index=False, columns=COLUMNS)
    logging.info(f'signal ledger [{source}]: +{n_new} new episode(s), '
                 f'{n_ref} refreshed → {path}')
    return n_new, n_ref
