#!/usr/bin/env bash
# us_daily_run.sh — daily US swing scan (phase 1: opportunity identification).
#
# Runs the 4-layer scanner once, after the US close, and writes the Morning
# Report to /home/ryan/DATA/result/us_tech_swing_<date>.txt. The scanner tries
# Futu for live holdings stop-status; if OpenD is down it degrades gracefully
# (see ADR-0002). Bars come from yfinance with stale-cache fallback (ADR-0001).
#
# Schedule via cron at 14:00 America/Los_Angeles, weekdays — see the CRON block
# at the bottom of this file.

set -u
set -o pipefail

REPO=/home/ryan/tushare_ryan
PY=/home/ryan/miniconda3/bin/python
LOG=$REPO/us_daily_run.log

# cron runs with a minimal environment; make conda's python and tools reachable.
export PATH=/home/ryan/miniconda3/bin:/usr/local/bin:/usr/bin:/bin

cd "$REPO" || { echo "cannot cd to $REPO" >&2; exit 1; }

ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

echo "===== us_daily_run start $(ts) =====" >> "$LOG"

# Full-universe scan. No --no-futu: let it use OpenD when available.
"$PY" t_us_tech_swing.py >> "$LOG" 2>&1
rc=$?

if [ $rc -eq 0 ]; then
    echo "===== us_daily_run OK    $(ts) =====" >> "$LOG"
else
    echo "===== us_daily_run FAIL (rc=$rc) $(ts) =====" >> "$LOG"
fi

exit $rc

# ── CRON ──────────────────────────────────────────────────────────────────────
# Install with `crontab -e` and add the two lines below. CRON_TZ pins the
# schedule to US Pacific regardless of the server's own timezone (this box was
# Asia/Shanghai at setup time), so it fires at 14:00 PT — ~1h after the close —
# on weekdays. DST is handled automatically by the IANA zone.
#
#   CRON_TZ=America/Los_Angeles
#   0 14 * * 1-5  /home/ryan/tushare_ryan/us_daily_run.sh
