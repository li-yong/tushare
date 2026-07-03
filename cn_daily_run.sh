#!/usr/bin/env bash
# cn_daily_run.sh — daily CN 科创板 (STAR-50) swing scan (opportunity ID).
#
# Runs t_cn_star_swing.py once, after the A-share close, and writes the report to
# /home/ryan/DATA/result/cn_star_swing/cn_star_swing_<date>.txt. This is the
# A-share port of us_daily_run.sh: it reuses the same calibrated engine
# (t_us_tech_swing, retargeted to the CN universe), so there is no separate
# strategy to maintain. Bars come from yfinance with .SS/.SZ suffixes and
# stale-cache fallback (ADR-0001).
#
# Unlike the US runner there are NO supplementary stages and NO Futu/OpenD call:
#   • PEAD/财报 and Futu CN-holdings sync are not ported in v1 (see the script
#     header). The user trades manually off this single report.
#   • A-share risk note baked into the report: ±20% 涨跌停 + 连续跌停 means the
#     fast hard-stop may be UNABLE to exit — Layer 0/1 degrades to "de-risk
#     earlier", not "exit faster".
#
# Schedule via cron at 16:00 Asia/Shanghai, weekdays (~1h after the 15:00 close)
# — see the CRON block at the bottom of this file.

set -u
set -o pipefail

REPO=/home/ryan/tushare_ryan
PY=/home/ryan/miniconda3/bin/python
LOG=$REPO/cn_daily_run.log

# cron runs with a minimal environment; make conda's python and tools reachable.
export PATH=/home/ryan/miniconda3/bin:/usr/local/bin:/usr/bin:/bin

cd "$REPO" || { echo "cannot cd to $REPO" >&2; exit 1; }

ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

echo "===== cn_daily_run start $(ts) =====" >> "$LOG"

# Primary (and only) stage: the STAR-50 swing scan. Writes its own dated report
# to result/cn_star_swing/. Its rc is the run's exit code.
"$PY" t_cn_star_swing.py >> "$LOG" 2>&1
rc=$?

if [ $rc -eq 0 ]; then
    echo "===== cn_daily_run OK    $(ts) =====" >> "$LOG"
else
    echo "===== cn_daily_run FAIL (rc=$rc) $(ts) =====" >> "$LOG"
fi

exit $rc

# ── CRON ──────────────────────────────────────────────────────────────────────
# Install with `crontab -e` and add the two lines below. CRON_TZ pins the
# schedule to China time (this box is already Asia/Shanghai, but pin it so the
# intent survives a server TZ change). Fires at 16:00 CST — ~1h after the 15:00
# A-share close — on weekdays. A-shares observe no DST.
#
#   CRON_TZ=Asia/Shanghai
#   0 16 * * 1-5  /home/ryan/tushare_ryan/cn_daily_run.sh
