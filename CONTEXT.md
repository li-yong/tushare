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
The regime classification (STRONG / MIXED / WEAK) derived from the barometers versus their 20-week MA, judged with a ±3% hysteresis band: inside the band a barometer (and each general in the leadership-breadth gate) keeps its previous side; only a close beyond the band edge flips it. Replayed deterministically from the bars, so `--asof` reproduces it. Decides which entry type the scanner looks for.
_Avoid_: market condition, regime (in code)

**Regime (体制)**:
The statistical climate of the market: a stretch of time over which the return structure (mean, volatility, correlation, tail shape, trend persistence) stays in one quasi-stable configuration — operationally, **the period during which a rule keeps its expectancy** (2022: every signal source's meanR flipped negative — the rules didn't break, their validity domain expired; hence the 有效域 column in `docs/rule_registry.md`). Mechanically a regime is pinned by two unobservables — who the marginal price-setter is (cash buyer in bulls, forced seller in bears: same news, opposite price response) and which shared assumption is being priced ("Fed put exists", "AI capex accelerates"); regime change = the assumption dies and the marginal actor changes identity. Both are detected only by lagging proxies (20wMA, breadth, vol structure) — there is no universal early warning, which is the entire justification for trend following: detect persistence, don't predict transitions; the cost is the boundary zone, and MIXED's negative EV is exactly that boundary. Four instruments embody it at different layers: Market State (trading layer, scan mode + time budgets), `t_us_regime_monitor` (bull→bear transition, the one hard gate), HMM second opinion (statistical filter, advisory), sea state (tide×wind quadrants). Related but not identical to Tide: tide (liquidity) is the regime's chief driver; regime is the whole climate including narrative era and volatility character. Fresh vs stale panic (§六) is a regime question: fresh panic is an accident *inside* a regime (holders' problem — the old regime returns to catch you); stale panic is the new regime expressing itself (no one is coming — obey the stress tiers until regime lift confirms).
_Avoid_: regime as a synonym for Market State (that is one instrument at one layer), "the market feels different" (name which proxy flipped), predicting regime change (right-倾 error is holding old rules past the change; left-倾 error is front-running a change that hasn't happened)

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

**Effective Stop**:
A holding's live stop = max(initial technical stop, breakeven line once P/L ≥ +30%, 20-week MA). The initial stop is the Setup's stop, hand-registered in `select.yml US_SWING_STOPS` on the day the trade is taken and deleted on exit; it and the breakeven line are judged at the daily close (Layer 1.5), the 20-week MA only at the weekly close. Keeps 1R real — without it a breakout entry far above the 20wMA has no stop between the fast crash layers and the slow weekly line.
_Avoid_: trailing stop (this never ratchets daily), stop-loss order (nothing rests at the broker)

**Open Heat**:
The total loss if every holding's Effective Stop were hit on the same day, as % of equity. The watchlist is one highly-correlated theme, so this — not the per-name R count — is the real unit of portfolio risk. Budgeted at ≤6% of equity; the Morning Report prints it daily and blocks new entries when over.
_Avoid_: exposure (notional is not risk), diversification count

**Signal Episode**:
One row in the signal ledger (`result/us_signal_log/us_signal_ledger.csv`, written via `signal_ledger.py`): the first day a scanner emits a Setup for a ticker opens an episode and freezes entry/stop/target at that day's values; re-emissions on following days refresh `last_seen`; ≥7 quiet days later, a new episode. Live runs only — an `--asof` row would be look-ahead. `t_us_signal_attrib.py` reads the ledger weekly and reports forward outcomes per source × signal type: the system's real track record, and the only honest answer to "which screen has edge".
_Avoid_: trade log (no trade happened; these are virtual), backtest (rows are written before the outcome is knowable)

**Tide / Wind / Wave (潮·風·浪)**:
The three-timescale mental model behind every indicator (full text: `docs/tide_wave_wind.md`). Tide = the slow water-level change (liquidity cycle + aggregate earnings; measured by regime monitor / 20wMA — obeyed, never fought). Wind = narrative/sentiment energy injection (news, sector themes; judged, and split into gusts vs monsoons — only monsoons justify position-level response). Wave = price action itself (what the scanners read and the system trades; lowest information content, useful mainly to infer wind and tide). Sea state = their combination, computed live by `sea_state.py` (tide from the regime snapshot × wind from the sector-rotation offense−defense spread) into four quadrants (ALIGNED 風潮同向 / CHOP 風頂潮 / EBB_RALLY 大風退潮 / EBB 風潮同退): a Morning-Report banner, frozen onto each Signal Episode at open, grouped in weekly attribution — advisory only, never a gate. Hypothesis under test: wind-with-tide is where the pool's positive expectancy lives; wind-against-tide is the negative-EV pocket. Short-term moves are energy phenomena (no money "flows in"), only the tide is a water-quantity phenomenon.
_Avoid_: "money flowing into the market" (数水思维), judging the tide from daily bar shapes (拿浪高测潮位)

**Panic Add Order (恐慌加仓次序)**:
The buy-side discipline when the panic ruler fires (the three-tier stress scale in `docs/panic_drawdown_empirics.md` §四): add index first (QQQ/SOXX tier), then pool (system-screened candidates), single names last — 指数 > 池 > 单票. Basis: catching a single stock's knife differs from catching the index's by one "extinction dimension" — the point-in-time NDX-2000 cohort shows a −70% single-name dip-buy has negative whole-cohort EV (even survivors' 26-year median only matches the index; the remembered right tails first fell another ~65% after entry), while the index at −70% recovered with zero extinction risk (`docs/deep_drawdown_cohort_study.md`, registry #27). What counts as panic is calibrated per layer (§六, 1%-percentile of 1d/5d/21d trading-day returns, ~1.5× volatility per layer down): index −4/−8/−17, semis sector ^SOX −6/−13/−24, single name by its own percentile (large-tech cross-median −7/−15/−28; high-vol MU/NVDA/AMD −10/−20/−36). Thresholds establish "is panic" only; buyability needs freshness (≤180 calendar days from ATH — stale panic is a clearance waterfall, registry #28), at pool level also the indiscriminateness check (whole market down = tide; this sector alone = its wind died), and at single-name level it is never established by price alone (β-decompose; the residual is information, burden of proof on 错杀 via SHOCK_DIVERGENCE / low_bounce grok). Any single-name deep-dip position must be sized assuming a further −50% is the median path and −99% is in sample; a bounce day is not a signal, and a first 200-day reclaim inside a structural bear is not either. Advisory, not a gate. The larger the subject, the closer a deep dip is to guaranteed repair; the smaller, the closer to a lottery ticket.
_Avoid_: "buy what fell most", quality as an exemption from stops (质地=防归零, 不防-90%), 讨债单 thinking (it fell 80% so it must recover), reading MU's −9% day with MSFT's ruler (single-name thresholds are per-name percentiles, not one number)

**Earnings Blackout**:
No new entry when a name reports within 5 days; each holding reporting within 5 days forces a written pre-earnings decision (hold full / halve / exit — no decision defaults to halve). Most Layer-1 single-day crushes are earnings.
_Avoid_: earnings play, layout window (that is the 2–4 week pre-ER accumulation concept, a different thing)
