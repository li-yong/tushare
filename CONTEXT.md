# US Swing Trading System

An opportunity-identification system for mid-to-long-term positions (weeks to months) in US Nasdaq tech stocks. It produces a daily Morning Report; the user executes all trades manually through a Futu account. Automated execution is a planned future phase, not part of the current system. Successor to the retired CN A-share screener pipeline in this repo.

## Language

**Watchlist**:
The hand-curated set of tickers scanned daily for entry signals. Lives in one YAML config file; never hardcoded in scripts. The NDX predictor informs manual edits but never modifies it.
_Avoid_: universe, stock list, candidates

**Holdings**:
The positions currently held in the Futu account, queried live. Never maintained in config; the account is the single source of truth.
_Avoid_: US_HOLD, portfolio file, position list

**Barometer**:
An index ETF (QQQ, SOXX) used only to classify market state, never traded by the system.

**Market State**:
The regime classification (STRONG / MIXED / WEAK) derived from the barometers versus their 20-week MA. Decides which entry type the scanner looks for.
_Avoid_: market condition, regime (in code)

**Setup**:
A fully-specified trade candidate: entry price, stop, target, and risk:reward ratio. A signal without all four is not a setup and cannot be acted on.
_Avoid_: signal (when entry/stop/target are attached), recommendation

**Pocket Pivot**:
A volume confirmation: today's up-volume exceeds every down-day's volume over the prior 10 sessions. Used to qualify breakout entries, never as a standalone entry.

**Key Level**:
A support/resistance price derived from historical touch-count clustering and swing highs/lows. Used to place stops and targets; preferred over fixed-percent offsets from an MA.
_Avoid_: key point (old repo name), support line

**Morning Report**:
The daily output of the scan: Market State, new Setups with share counts, and the stop status of every holding (including "stop breached → exit today"). The system's sole product until the automation phase.
_Avoid_: signal list, candidates CSV

**R (Risk Unit)**:
1% of live account equity (queried from Futu). The amount lost if a Setup's stop is hit at planned size: `shares = R / (entry − stop)`, capped at 25% of equity per name and ~5 concurrent positions. Targets are expressed as R-multiples.
_Avoid_: position size (when the risk amount is meant), bet
