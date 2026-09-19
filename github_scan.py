import os
import asyncio
import time
import json
from aiohttp import ClientSession, ClientTimeout
import scanner
import main


async def telegram_send(session, text):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        ) as r:
            return r.status == 200
    except Exception as e:
        scanner.state["last_error"] = f"telegram: {type(e).__name__}: {e}"
        return False

def format_signal(symbol, confluence, sp, fu):
    side = "LONG" if confluence == "LONG" else "SHORT"
    s = fu if fu.get("signal", "").startswith(side) else sp
    return (
        f"Famil Scanner | {symbol} | {side}\n"
        f"Time: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}\n"
        f"Confluence: {confluence}\n"
        f"Entry: {s.get('entry', 'n/a')}\n"
        f"Stop: {s.get('stop', 'n/a')}\n"
        f"TP1: {s.get('tp1', 'n/a')}\n"
        f"TP2: {s.get('tp2', 'n/a')}\n"
        f"RR: {s.get('rr', 'n/a')}\n"
        f"Strategy: {scanner.state.get('version', '1.0')}"
    )

async def run_once():
    scanner.DB_PATH = "./data/famil_scanner.db"
    scanner.db().close()
    timeout = ClientTimeout(total=20)
    async with ClientSession(timeout=timeout) as session:
        for symbol in scanner.WATCHLIST:
            await scanner.refresh(session, symbol)
            # Result evaluation needs a short candle window; use a separate
            # fetch so the scanner's signal history is not affected.
            for base, market in ((scanner.SPOT_REST, "SPOT"), (scanner.FUT_REST, "FUTURES")):
                try:
                    path = "/api/v3/klines" if market == "SPOT" else "/fapi/v1/klines"
                    async with session.get(base + path, params={"symbol": symbol, "interval": "5m", "limit": 60}) as r:
                        candles = await r.json(content_type=None)
                    if scanner.valid(candles):
                        scanner.resolve_results(symbol, market, candles)
                except Exception as e:
                    scanner.state["last_error"] = f"{symbol}/{market}: {type(e).__name__}: {e}"

    closed, proposals, errors, _ = main.learn_cycle()

    for symbol in scanner.WATCHLIST:
        confluence = scanner.state["confluence"].get(symbol, "NO ENTRY")
        if confluence in ("LONG", "SHORT"):
            sp = scanner.state["spot"].get(symbol, {})
            fu = scanner.state["futures"].get(symbol, {})
            await telegram_send(session, format_signal(symbol, confluence, sp, fu))

    diagnostics = {}
    for symbol in scanner.WATCHLIST:
        sp = scanner.state["spot"].get(symbol, {})
        fu = scanner.state["futures"].get(symbol, {})
        diagnostics[symbol] = {
            "SPOT": sp.get("signal", "NO DATA"),
            "FUTURES": fu.get("signal", "NO DATA"),
            "CONFLUENCE": scanner.state["confluence"].get(symbol, "NO ENTRY"),
            "WHY_NO_ENTRY": {
                "SPOT": sp.get("why_no_entry", []),
                "FUTURES": fu.get("why_no_entry", [])
            }
        }

    print(json.dumps({
        "event": "github_scan_once",
        "ts": int(time.time()),
        "version": scanner.state.get("version", "1.0"),
        "symbols": scanner.WATCHLIST,
        "diagnostics": diagnostics,
        "closed": closed,
        "proposals": proposals,
        "errors": errors,
        "last_error": scanner.state["last_error"]
    }, ensure_ascii=False))
    
if __name__ == "__main__":
    asyncio.run(run_once())
