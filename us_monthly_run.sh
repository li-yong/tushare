#!/usr/bin/env bash
# us_monthly_run.sh — monthly US memory-cycle monitor.
#
# t_us_cycle_monitor.py derives three DRAM/AI-cycle indicators (gross-margin
# trend, inventory weeks, CapEx acceleration) from SEC EDGAR XBRL quarterly
# filings. The underlying data only moves when a company files a new 10-Q/10-K,
# so a daily — even weekly — run is pointless; monthly comfortably catches every
# new filing. Default ticker is MU (the cycle bellwether).
#
# Schedule via cron — 1st of each month, 14:00 America/Los_Angeles. See the CRON
# block at the bottom.

set -u
set -o pipefail

REPO=/home/ryan/tushare_ryan
PY=/home/ryan/miniconda3/bin/python
LOG=$REPO/us_monthly_run.log

export PATH=/home/ryan/miniconda3/bin:/usr/local/bin:/usr/bin:/bin

cd "$REPO" || { echo "cannot cd to $REPO" >&2; exit 1; }

ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

echo "===== us_monthly_run start $(ts) =====" >> "$LOG"

"$PY" t_us_cycle_monitor.py >> "$LOG" 2>&1
rc=$?

if [ $rc -eq 0 ]; then
    echo "===== us_monthly_run OK    $(ts) =====" >> "$LOG"
else
    echo "===== us_monthly_run FAIL (rc=$rc) $(ts) =====" >> "$LOG"
fi

exit $rc

# ── CRON ──────────────────────────────────────────────────────────────────────
#   CRON_TZ=America/Los_Angeles
#   0 14 1 * *  /home/ryan/tushare_ryan/us_monthly_run.sh
