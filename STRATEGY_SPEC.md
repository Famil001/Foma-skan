# Famil Scanner v1.0 — Strategy Specification

## 1. Purpose
Analysis-only Binance Spot + USDⓈ-M Futures scanner. No order execution.

Pipeline:
Technical Analysis -> Binance microstructure -> Confluence -> Signal Journal -> Result Engine -> Weekly Audit.

## 2. Timeframes
- 1h: higher-timeframe trend and major structure.
- 15m: intermediate structure, support/resistance, liquidity.
- 5m: primary setup timeframe.
- 1m/3m: entry confirmation only when available; otherwise 5m close confirmation.

## 3. Core technical layer
Mandatory:
1. Market structure: HH/HL, LH/LL, BOS/CHOCH.
2. Support/resistance from recent confirmed swing points and 20/50-bar context.
3. Price Action/candlestick confirmation.
4. Volume relative to recent average.
5. Chart pattern detection when a mechanically identifiable pattern exists.

Indicators are confirmation, not standalone triggers:
- MA
- RSI
- MACD
- ADX
- OBV
- Stochastic
- Accumulation/Distribution
- Fibonacci levels when structure provides a meaningful anchor.

## 4. Liquidity and Order Blocks
Liquidity:
- recent swing highs/lows;
- equal highs/lows;
- range extremes;
- sweep = price trades through a marked liquidity level and closes back inside the prior range.

Order Block:
- last opposing candle before a measurable displacement;
- bullish OB supports LONG setups;
- bearish OB supports SHORT setups;
- OB is invalidated by a decisive close through its invalidation boundary.

The scanner must distinguish:
- CONFIRMED OB
- POTENTIAL OB
- NONE

## 5. Spot confirmation
LONG requires:
- bullish 1h/15m context OR a valid 15m bullish reversal structure;
- 5m bullish structure;
- valid liquidity sweep or clean breakout/retest;
- bullish OB or confirmed breakout/retest;
- volume/price-action confirmation;
- positive spot order-book pressure near price;
- R/R >= 2.0.

SHORT is symmetric where short-side Spot analysis is reported as bearish setup, but Spot execution remains informational unless explicitly enabled.

## 6. Futures confirmation
LONG requires:
- bullish technical structure;
- futures order-book pressure supportive of buyers;
- taker buy/sell flow supportive of buyers;
- OI (Open Interest) interpreted together with price;
- funding rate checked for crowding;
- top-trader account and position ratios checked;
- basis checked;
- no major conflicting liquidation/liquidity structure;
- R/R >= 2.0.

SHORT is symmetric.

Important: OI, funding, ratios and taker flow are contextual filters. None is a standalone signal.

## 7. OI interpretation
Use price + OI together:
- price up + OI up: new participation supports continuation;
- price up + OI down: short-covering possibility;
- price down + OI up: new short participation;
- price down + OI down: long-liquidation possibility.

These labels are descriptive, not proof of trader identity.

## 8. Confluence
Do not average Spot and Futures into one opaque score.

Output independently:
SPOT: LONG/WAIT
FUTURES: LONG/SHORT/WAIT
CONFLUENCE: LONG / SHORT / NO ENTRY

CONFLUENCE requires directional agreement plus no critical conflict.

## 9. Entry / risk
Entry:
- confirmation close, or
- OB retest when explicitly confirmed.

Stop:
- beyond sweep/OB invalidation, with a structure-based buffer.

TP1:
- nearest opposing liquidity / structure target.

TP2:
- next major liquidity / structure target.

Minimum R/R: 2.0.

No signal is emitted when stop placement makes R/R < 2.0.

## 10. Mandatory WHY NO ENTRY
Every WAIT must state exact missing/conflicting conditions.

Examples:
- no bullish 5m structure;
- liquidity sweep not confirmed;
- order-book pressure neutral;
- taker flow conflicts;
- OI context inconclusive;
- funding crowding conflict;
- TP1 gives R/R below 2.0.

## 11. Signal states
- LONG SETUP
- LONG CONFIRMED
- SHORT SETUP
- SHORT CONFIRMED
- WAIT

A setup is not counted as a confirmed signal.

## 12. Signal Journal
For every CONFIRMED signal store:
- timestamp
- symbol
- market (SPOT/FUTURES/CONFLUENCE)
- direction
- strategy version
- timeframe
- entry
- stop
- TP1
- TP2
- R/R
- structure
- OB
- liquidity event
- volume
- order-book metrics
- OI
- funding
- basis
- taker flow
- top-trader ratios
- reasons
- raw-data snapshot hash/id where practical.

## 13. Result Engine
After confirmation, monitor subsequent market data.

Priority:
- TP1 hit before Stop -> WIN_TP1
- TP2 hit after TP1 -> WIN_TP2
- Stop hit before TP1 -> LOSS
- neither hit yet -> OPEN
- same candle reaches both and ordering cannot be established -> AMBIGUOUS

Do not rewrite historical signals when strategy rules change.

## 14. Weekly audit
Report:
- total confirmed signals;
- Spot/Futures/Confluence counts;
- LONG/SHORT counts;
- TP1 wins;
- TP2 wins;
- losses;
- open;
- ambiguous;
- TP1 hit rate;
- stop-before-TP1 rate;
- average R;
- expectancy;
- average favorable excursion where measurable;
- average adverse excursion where measurable;
- condition-level performance;
- failure reasons;
- consecutive wins/losses;
- strategy-version comparison.

Accuracy is an observed historical statistic, not a guarantee.

## 15. Versioning
Current baseline: v1.0.

Rules are not silently changed after signals are recorded. Improvements are proposed from weekly evidence and assigned a new version.

## 16. Binance data sources
Use official Binance market-data endpoints/connectors for Spot and USDⓈ-M Futures. Funding, OI statistics, taker volume, top-trader ratios and basis are available as separate market-data series; they must be recorded independently.
