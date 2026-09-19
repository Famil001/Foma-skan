# Foma-skan

Binance Spot + USDⓈ-M Futures market scanner.

## Architecture
- SPOT: LONG/BUY or WAIT
- FUTURES: LONG/SHORT/WAIT
- CONFLUENCE: compares independent Spot and Futures signals
- WHY NO ENTRY: mandatory rejection reasons
- Large-liquidity behavior model; never claims to identify a specific market maker
- WebSocket market data with reconnect logic
- REST snapshots for slower metrics
- Signals only; no automated trading

The scanner uses public Binance market data and does not require trading API keys.
