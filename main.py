import asyncio, json, os, time, uuid
from datetime import datetime, timezone
from aiohttp import web, ClientSession, ClientTimeout
import scanner

STARTED=time.time()
WATCHLIST=scanner.WATCHLIST
INTERVAL=scanner.INTERVAL
PORT=scanner.PORT
SPOT_REST=scanner.SPOT_REST
FUT_REST=scanner.FUT_REST

TAXONOMY=[
 "FALSE_SWEEP","BAD_OB","WEAK_VOLUME","ORDER_BOOK_TRAP","OI_CONFLICT",
 "FUNDING_CONFLICT","LIQUIDITY_TARGET_TOO_CLOSE","STRUCTURE_BREAK",
 "FALSE_BREAKOUT","DATA_CONFLICT","OTHER"
]

def ensure_learning_tables():
    c=scanner.db()
    c.execute("""CREATE TABLE IF NOT EXISTS learning_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id INTEGER UNIQUE, ts INTEGER,
      symbol TEXT, error_code TEXT, candle_close REAL, expected_vs_actual TEXT,
      evidence TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS learning_proposals(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, base_version TEXT,
      proposal TEXT, evidence TEXT, status TEXT DEFAULT 'PROPOSED')""")
    c.commit(); c.close()

def classify(payload,direction):
    if not payload:
        return "DATA_CONFLICT"
    if not payload.get("sweep"):
        return "FALSE_SWEEP"
    if payload.get("ob_type") in (None,"NONE"):
        return "BAD_OB"
    if float(payload.get("rel_volume",0) or 0) < 1:
        return "WEAK_VOLUME"
    b=float(payload.get("book",{}).get("imbalance",0) or 0)
    flow=float(payload.get("flow",0) or 0)
    funding=float(payload.get("funding",0) or 0)
    if direction=="LONG" and b < -0.05 or direction=="SHORT" and b > 0.05:
        return "ORDER_BOOK_TRAP"
    if direction=="LONG" and flow < -0.05 or direction=="SHORT" and flow > 0.05:
        return "OI_CONFLICT"
    if direction=="LONG" and funding > 0.001 or direction=="SHORT" and funding < -0.001:
        return "FUNDING_CONFLICT"
    if float(payload.get("rr",0) or 0) < scanner.MIN_RR:
        return "LIQUIDITY_TARGET_TOO_CLOSE"
    return "STRUCTURE_BREAK"

def learn_cycle():
    ensure_learning_tables()
    c=scanner.db()
    rows=c.execute("""SELECT id,symbol,direction,status,payload,entry FROM signals
                      WHERE status IN ('LOSS','WIN_TP1','WIN_TP2')""").fetchall()
    for sid,symbol,direction,status,payload_text,entry in rows:
        if status!="LOSS":
            continue
        if c.execute("SELECT 1 FROM learning_events WHERE signal_id=?",(sid,)).fetchone():
            continue
        try:
            payload=json.loads(payload_text).get("signal",{})
        except Exception:
            payload={}
        code=classify(payload,direction)
        evidence={
          "status":status,
          "entry":entry,
          "rel_volume":payload.get("rel_volume"),
          "order_book_imbalance":payload.get("book",{}).get("imbalance"),
          "taker_flow":payload.get("flow"),
          "oi_delta":payload.get("oi_delta"),
          "funding":payload.get("funding"),
          "ob_type":payload.get("ob_type"),
          "sweep":payload.get("sweep")
        }
        c.execute("""INSERT INTO learning_events
          (signal_id,ts,symbol,error_code,candle_close,expected_vs_actual,evidence)
          VALUES(?,?,?,?,?,?,?)""",
          (sid,int(time.time()),symbol,code,None,
           json.dumps({"expected_direction":direction,"result":status}),
           json.dumps(evidence,separators=(",",":"))))
    closed=c.execute("SELECT COUNT(*) FROM signals WHERE status IN ('LOSS','WIN_TP1','WIN_TP2')").fetchone()[0]
    proposals=0
    if closed>=30 and c.execute("SELECT COUNT(*) FROM learning_proposals WHERE status='PROPOSED'").fetchone()[0]==0:
        errors=c.execute("""SELECT error_code,COUNT(*) n FROM learning_events
                            GROUP BY error_code ORDER BY n DESC""").fetchall()
        if errors and errors[0][1]>=5:
            proposal={
              "rule_change_candidate":errors[0][0],
              "evidence_count":errors[0][1],
              "action":"review and backtest before creating a new strategy version",
              "constraint":"historical versions remain unchanged"
            }
            c.execute("""INSERT INTO learning_proposals
              (ts,base_version,proposal,evidence) VALUES(?,?,?,?)""",
              (int(time.time()),scanner.state["version"],json.dumps(proposal),json.dumps(errors)))
            proposals=1
    errors=c.execute("""SELECT error_code,COUNT(*) FROM learning_events
                        GROUP BY error_code ORDER BY COUNT(*) DESC""").fetchall()
    props=c.execute("""SELECT id,ts,base_version,proposal,status
                       FROM learning_proposals ORDER BY id DESC LIMIT 20""").fetchall()
    c.commit(); c.close()
    return closed,proposals,errors,props

async def fetch_closed(session,base,symbol):
    try:
        async with session.get(base+"/api/v3/klines" if base==SPOT_REST else base+"/fapi/v1/klines",
          params={"symbol":symbol,"interval":"5m","limit":3}) as r:
            d=await r.json(content_type=None)
            return d if r.status==200 else []
    except Exception:
        return []

async def cycle():
    timeout=ClientTimeout(total=12)
    async with ClientSession(timeout=timeout) as session:
        await asyncio.gather(*(scanner.refresh(session,s) for s in WATCHLIST))
        for symbol in WATCHLIST:
            sc=await fetch_closed(session,SPOT_REST,symbol)
            fc=await fetch_closed(session,FUT_REST,symbol)
            if scanner.valid(sc):
                scanner.resolve_results(symbol,"SPOT",sc)
            if scanner.valid(fc):
                scanner.resolve_results(symbol,"FUTURES",fc)
            print(json.dumps({
              "event":"last_closed_candle",
              "symbol":symbol,
              "spot_close_ts":sc[-2][0] if scanner.valid(sc) and len(sc)>=2 else None,
              "futures_close_ts":fc[-2][0] if scanner.valid(fc) and len(fc)>=2 else None
            }))
    closed,proposals,_,_=learn_cycle()
    scanner.state["updated"]=int(time.time())
    return closed,proposals

async def health(request):
    return web.json_response({
      "status":"ok","scanner":"running","uptime":int(time.time()-STARTED),
      "version":scanner.state["version"],"learning":"evidence_only",
      "updated":scanner.state["updated"],"last_error":scanner.state["last_error"]
    })

async def state_report(request):
    return web.json_response({
      "version":scanner.state["version"],"updated":scanner.state["updated"],
      "last_error":scanner.state["last_error"],"confluence":scanner.state["confluence"],
      "spot":scanner.state["spot"],"futures":scanner.state["futures"]
    })

async def weekly(request):
    since=int(time.time())-7*86400
    c=scanner.db()
    total=c.execute("SELECT COUNT(*) FROM signals WHERE ts>=?",(since,)).fetchone()[0]
    closed=c.execute("SELECT COUNT(*) FROM signals WHERE ts>=? AND status IN ('LOSS','WIN_TP1','WIN_TP2')",(since,)).fetchone()[0]
    wins=c.execute("SELECT COUNT(*) FROM signals WHERE ts>=? AND status IN ('WIN_TP1','WIN_TP2')",(since,)).fetchone()[0]
    errors=c.execute("""SELECT error_code,COUNT(*) FROM learning_events
                        WHERE ts>=? GROUP BY error_code ORDER BY COUNT(*) DESC""",(since,)).fetchall()
    c.close()
    return web.json_response({
      "version":scanner.state["version"],"period_days":7,"signals":total,
      "closed":closed,"wins":wins,"observed_accuracy":(wins/closed if closed else None),
      "learning_events":errors
    })

async def learning(request):
    closed,proposals,errors,props=learn_cycle()
    return web.json_response({
      "ready":closed>=30,"signals_count":closed,"min_required":30,
      "mode":"evidence_only","taxonomy":TAXONOMY,
      "errors":errors,"proposals_created":proposals,"proposals":props
    })

async def loop():
    while True:
        started=time.time()
        try:
            await cycle()
        except Exception as e:
            scanner.state["last_error"]=f"{type(e).__name__}: {e}"
            print("scanner cycle error:",scanner.state["last_error"])
        await asyncio.sleep(max(1,INTERVAL-(time.time()-started)))

async def main():
    ensure_learning_tables()
    app=web.Application()
    app.router.add_get("/health",health)
    app.router.add_get("/state",state_report)
    app.router.add_get("/weekly",weekly)
    app.router.add_get("/learning",learning)
    runner=web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner,"0.0.0.0",PORT).start()
    await loop()

if __name__=="__main__":
    asyncio.run(main())
