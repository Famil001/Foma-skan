import os
import asyncio
import time
import json
from aiohttp import ClientSession, ClientTimeout
import scanner
import main


def telegram_credentials():
    return (
        os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAMBOTTOKEN"),
        os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAMCHATID"),
    )


async def telegram_send(session, text):
    token, chat_id = telegram_credentials()
    if not token or not chat_id:
        scanner.state["last_error"] = "telegram: credentials not configured"
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        ) as r:
            if r.status == 200:
                return True
            body = await r.text()
            scanner.state["last_error"] = f"telegram HTTP {r.status}: {body[:300]}"
            return False
    except Exception as e:
        scanner.state["last_error"] = f"telegram: {type(e).__name__}: {e}"
        return False


def ensure_notification_table():
    conn = scanner.db()
    conn.execute("CREATE TABLE IF NOT EXISTS telegram_notifications(signal_id INTEGER PRIMARY KEY, ts INTEGER)")
    conn.commit()
    conn.close()


def notification_needed(symbol, confluence):
    conn = scanner.db()
    row = conn.execute(
        "SELECT id FROM signals WHERE symbol=? AND direction=? AND status='OPEN' ORDER BY id DESC LIMIT 1",
        (symbol, confluence),
    ).fetchone()
    if not row:
        conn.close()
        return None
    seen = conn.execute(
        "SELECT 1 FROM telegram_notifications WHERE signal_id=?", (row[0],)
    ).fetchone()
    conn.close()
    return None if seen else row[0]


def mark_notified(signal_id):
    conn = scanner.db()
    conn.execute(
        "INSERT OR IGNORE INTO telegram_notifications(signal_id,ts) VALUES(?,?)",
        (signal_id, int(time.time())),
    )
    conn.commit()
    conn.close()


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


def ensure_report_table():
    conn = scanner.db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS report_log("
        "kind TEXT, period_key TEXT, ts INTEGER, "
        "PRIMARY KEY(kind, period_key))"
    )
    conn.commit()
    conn.close()


def _report_due(kind, period_key):
    conn = scanner.db()
    row = conn.execute(
        "SELECT 1 FROM report_log WHERE kind=? AND period_key=?",
        (kind, period_key),
    ).fetchone()
    conn.close()
    return row is None


def _mark_report(kind, period_key):
    conn = scanner.db()
    conn.execute(
        "INSERT OR IGNORE INTO report_log(kind,period_key,ts) VALUES(?,?,?)",
        (kind, period_key, int(time.time())),
    )
    conn.commit()
    conn.close()


def build_period_report(kind, period_key, since_ts):
    conn = scanner.db()
    total = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE ts>=?", (since_ts,)
    ).fetchone()[0]
    wins = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE ts>=? "
        "AND status IN ('WIN_TP1','WIN_TP2')", (since_ts,)
    ).fetchone()[0]
    losses = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE ts>=? AND status='LOSS'", (since_ts,)
    ).fetchone()[0]
    open_n = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE ts>=? AND status='OPEN'", (since_ts,)
    ).fetchone()[0]
    top = conn.execute(
        "SELECT symbol, COUNT(*) n FROM signals WHERE ts>=? "
        "GROUP BY symbol ORDER BY n DESC LIMIT 5", (since_ts,)
    ).fetchall()
    conn.close()

    lines = [
        f"Famil Scanner — {kind.upper()} REPORT",
        f"Period: {period_key}",
        f"Strategy: v{scanner.state.get('version','1.0')}",
        "",
        f"Confirmed signals: {total}",
        f"Wins: {wins}",
        f"Losses: {losses}",
        f"Open: {open_n}",
    ]
    if top:
        lines.append(
            "Active symbols: " + ", ".join(f"{s} ({n})" for s, n in top)
        )
    else:
        lines.append("Confirmed signals: none in this period.")

    lines.append("")
    lines.append("Current confluence:")
    for symbol in scanner.WATCHLIST:
        lines.append(
            f"{symbol}: {scanner.state['confluence'].get(symbol,'NO ENTRY')}"
        )
    return "\n".join(lines)


async def send_periodic_reports(session):
    ensure_report_table()
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime

        now = time.time()
        local = datetime.fromtimestamp(now, ZoneInfo("Asia/Tbilisi"))

        hour_key = local.strftime("%Y-%m-%d-%H")
        if local.minute < 5 and _report_due("hourly", hour_key):
            report = build_period_report("hourly", hour_key, int(now) - 3600)
            if await telegram_send(session, report):
                _mark_report("hourly", hour_key)

        if local.hour == 9 and local.minute < 5:
            day_key = local.strftime("%Y-%m-%d")
            if _report_due("daily", day_key):
                report = build_period_report("daily", day_key, int(now) - 86400)
                if await telegram_send(session, report):
                    _mark_report("daily", day_key)

        if local.weekday() == 6 and local.hour == 9 and local.minute < 5:
            week_key = local.strftime("%Y-W%V")
            if _report_due("weekly", week_key):
                report = build_period_report("weekly", week_key, int(now) - 7 * 86400)
                if await telegram_send(session, report):
                    _mark_report("weekly", week_key)

    except Exception as e:
        scanner.state["last_error"] = f"report: {type(e).__name__}: {e}"


async def run_once():
    scanner.DB_PATH = "./data/famil_scanner.db"
    scanner.db().close()

    timeout = ClientTimeout(total=20)
    async with ClientSession(timeout=timeout) as session:
        for symbol in scanner.WATCHLIST:
            await scanner.refresh(session, symbol)

            for base, market in (
                (scanner.SPOT_REST, "SPOT"),
                (scanner.FUT_REST, "FUTURES"),
            ):
                try:
                    path = "/api/v3/klines" if market == "SPOT" else "/fapi/v1/klines"
                    async with session.get(
                        base + path,
                        params={"symbol": symbol, "interval": "5m", "limit": 60},
                    ) as r:
                        candles = await r.json(content_type=None)
                    if scanner.valid(candles):
                        scanner.resolve_results(symbol, market, candles)
                except Exception as e:
                    scanner.state["last_error"] = (
                        f"{symbol}/{market}: {type(e).__name__}: {e}"
                    )

        closed, proposals, errors, _ = main.learn_cycle()

        ensure_notification_table()
        for symbol in scanner.WATCHLIST:
            confluence = scanner.state["confluence"].get(symbol, "NO ENTRY")
            if confluence in ("LONG", "SHORT"):
                signal_id = notification_needed(symbol, confluence)
                if signal_id:
                    sp = scanner.state["spot"].get(symbol, {})
                    fu = scanner.state["futures"].get(symbol, {})
                    if await telegram_send(
                        session, format_signal(symbol, confluence, sp, fu)
                    ):
                        mark_notified(signal_id)

        await send_periodic_reports(session)

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
                    "FUTURES": fu.get("why_no_entry", []),
                },
                "DATA_HEALTH": scanner.state.get("data_health", {}).get(symbol, {}),
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
            "last_error": scanner.state["last_error"],
        }, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(run_once())
