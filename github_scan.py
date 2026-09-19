import asyncio
import time
import json
from aiohttp import ClientSession, ClientTimeout
import scanner
import main

async def run_once():
    scanner.DB_PATH = "./data/famil_scanner.db"
    scanner.db().close()
    timeout = ClientTimeout(total=20)
    async with ClientSession(timeout=timeout) as session:
        for symbol in scanner.WATCHLIST:
            await scanner.refresh(session, symbol)
            for base, market in ((scanner.SPOT_REST, "SPOT"), (scanner.FUT_REST, "FUTURES")):
                try:
                    path = "/api/v3/klines" if market == "SPOT" else "/fapi/v1/klines"
                    async with session.get(base + path, params={"symbol": symbol, "interval": "5m", "limit": 3}) as r:
                        candles = await r.json(content_type=None)
                    if scanner.valid(candles):
                        scanner.resolve_results(symbol, market, candles)
                except Exception as e:
                    scanner.state["last_error"] = f"{symbol}/{market}: {type(e).__name__}: {e}"
    closed, proposals, errors, _ = main.learn_cycle()
    print(json.dumps({
        "event": "github_scan_once",
        "ts": int(time.time()),
        "symbols": scanner.WATCHLIST,
        "closed": closed,
        "proposals": proposals,
        "errors": errors,
        "last_error": scanner.state["last_error"]
    }))

if __name__ == "__main__":
    asyncio.run(run_once())
