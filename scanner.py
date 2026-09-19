import asyncio, json, logging, os, time
from aiohttp import web, ClientSession, ClientTimeout
import websockets

WATCHLIST=[x.strip().upper() for x in os.getenv("WATCHLIST","BTCUSDT,ETHUSDT,SOLUSDT,AAVEUSDT,XRPUSDT,VETUSDT,INJUSDT").split(",") if x.strip()]
INTERVAL=max(5,int(os.getenv("SCAN_INTERVAL_SECONDS","15")))
MIN_RR=float(os.getenv("MIN_RR","2.0"))
PORT=int(os.getenv("PORT","8080"))
SPOT_REST="https://api.binance.com"
FUT_REST="https://fapi.binance.com"
SPOT_WS="wss://stream.binance.com:9443/stream"
FUT_WS="wss://fstream.binance.com/stream"

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
state={"spot":{},"futures":{},"confluence":{},"updated":None,"last_error":None}

def valid_candles(c):
    return isinstance(c,list) and len(c)>=30 and all(isinstance(x,list) and len(x)>=6 for x in c)

def closes(c): return [float(x[4]) for x in c]
def highs(c): return [float(x[2]) for x in c]
def lows(c): return [float(x[3]) for x in c]

def calc_levels(c):
    if not valid_candles(c): return None
    try:
        cs,hs,ls=closes(c),highs(c),lows(c)
        return {"price":cs[-1],"support":min(ls[-20:]),"resistance":max(hs[-20:]),
                "high20":max(hs[-20:]),"low20":min(ls[-20:])}
    except (ValueError,TypeError,IndexError):
        return None

def ob_signal(c):
    if not valid_candles(c): return "NONE",None
    for i in range(len(c)-4,max(2,len(c)-30),-1):
        try:
            o,h,l,cl=map(float,[c[i][1],c[i][2],c[i][3],c[i][4]])
            ncl=float(c[i+2][4])
            if ncl>h*1.008 and cl<o: return "BULLISH",{"low":l,"high":o}
            if ncl<l*0.992 and cl>o: return "BEARISH",{"low":o,"high":h}
        except (ValueError,TypeError,IndexError):
            continue
    return "NONE",None

def direction(levels,candles):
    if not levels: return "NEUTRAL"
    p,s,r=levels["price"],levels["support"],levels["resistance"]
    if p>s*1.002 and p<r*0.998:
        recent=closes(candles[-6:])
        if recent[-1]>recent[0]: return "BULLISH"
        if recent[-1]<recent[0]: return "BEARISH"
    return "NEUTRAL"

def book_imbalance(book):
    if not isinstance(book,dict): return 0.0
    try:
        bids=sum(float(x[1]) for x in book.get("bids",[])[:20])
        asks=sum(float(x[1]) for x in book.get("asks",[])[:20])
        return (bids-asks)/(bids+asks) if bids+asks else 0.0
    except (ValueError,TypeError,IndexError): return 0.0

def make_spot(symbol,candles,book):
    lv=calc_levels(candles)
    if not lv:return {"symbol":symbol,"signal":"WAIT","why_no_entry":["insufficient or invalid candle data"]}
    d=direction(lv,candles); typ,ob=ob_signal(candles); imbalance=book_imbalance(book)
    p=lv["price"]; reasons=[]; signal="WAIT"; entry=p; stop=lv["support"]; target=lv["resistance"]; rr=0
    if d=="BULLISH" and imbalance>0.08 and typ in ("BULLISH","NONE"):
        stop=min(lv["support"],ob["low"] if ob and typ=="BULLISH" else p*0.985)
        risk=entry-stop; target=lv["resistance"]; rr=(target-entry)/risk if risk>0 else 0
        if rr>=MIN_RR: signal="LONG/BUY"
        else: reasons.append(f"R/R {rr:.2f} below minimum {MIN_RR}")
    else:
        if d!="BULLISH": reasons.append("no bullish structure")
        if imbalance<=0.08: reasons.append("spot order-book demand not confirmed")
        if typ=="BEARISH": reasons.append("bearish order block")
    return {"symbol":symbol,"signal":signal,"price":p,"support":lv["support"],"resistance":lv["resistance"],
            "order_block":ob,"order_block_type":typ,"order_book_imbalance":imbalance,
            "entry":entry,"stop":stop,"tp1":target,"tp2":target*1.01,"rr":rr,
            "why_no_entry":reasons if signal=="WAIT" else []}

def make_futures(symbol,candles,book,oi,top,taker):
    lv=calc_levels(candles)
    if not lv:return {"symbol":symbol,"signal":"WAIT","why_no_entry":["insufficient or invalid candle data"]}
    d=direction(lv,candles); typ,ob=ob_signal(candles); p=lv["price"]; imbalance=book_imbalance(book)
    try: top_ratio=float(top.get("longShortRatio",1) or 1)
    except: top_ratio=1.0
    try:
        buy=float(taker.get("buyVol",0) or 0); sell=float(taker.get("sellVol",0) or 0)
        flow=(buy-sell)/(buy+sell) if buy+sell else 0
    except: flow=0.0
    reasons=[]; signal="WAIT"; entry=p
    if d=="BULLISH" and imbalance>0.08 and flow>0.05 and typ!="BEARISH":
        stop=min(lv["support"],ob["low"] if ob and typ=="BULLISH" else p*.985); tp1=lv["resistance"]
        rr=(tp1-entry)/(entry-stop) if entry>stop else 0
        if rr>=MIN_RR: signal="LONG"
        else: reasons.append(f"R/R {rr:.2f} below minimum {MIN_RR}")
    elif d=="BEARISH" and imbalance<-0.08 and flow<-0.05 and typ!="BULLISH":
        stop=max(lv["resistance"],ob["high"] if ob and typ=="BEARISH" else p*1.015); tp1=lv["support"]
        rr=(entry-tp1)/(stop-entry) if stop>entry else 0
        if rr>=MIN_RR: signal="SHORT"
        else: reasons.append(f"R/R {rr:.2f} below minimum {MIN_RR}")
    else:
        stop=lv["support"] if d=="BULLISH" else lv["resistance"]; tp1=lv["resistance"] if d=="BULLISH" else lv["support"]; rr=0
        if d=="NEUTRAL": reasons.append("structure is neutral")
        if abs(imbalance)<=0.08: reasons.append("futures order-book liquidity does not confirm")
        if abs(flow)<=0.05: reasons.append("taker flow does not confirm")
        if d=="BULLISH" and typ=="BEARISH": reasons.append("bearish order block conflict")
        if d=="BEARISH" and typ=="BULLISH": reasons.append("bullish order block conflict")
    return {"symbol":symbol,"signal":signal,"price":p,"support":lv["support"],"resistance":lv["resistance"],
            "order_block":ob,"order_block_type":typ,"order_book_imbalance":imbalance,
            "open_interest":oi,"top_trader_long_short_ratio":top_ratio,"taker_flow":flow,
            "entry":entry,"stop":stop,"tp1":tp1,"tp2":tp1,"rr":rr,
            "why_no_entry":reasons if signal=="WAIT" else []}

async def rest_json(session,url,params=None):
    try:
        async with session.get(url,params=params) as r:
            data=await r.json(content_type=None)
            if r.status!=200:
                logging.warning("REST %s status=%s data=%s",url,r.status,data)
                return {}
            return data
    except Exception as e:
        logging.warning("REST %s: %s",url,e); return {}

async def refresh(session,symbol):
    try:
        sc,fc,sb,fb,oi,top,tak=await asyncio.gather(
            rest_json(session,SPOT_REST+"/api/v3/klines",{"symbol":symbol,"interval":"5m","limit":120}),
            rest_json(session,FUT_REST+"/fapi/v1/klines",{"symbol":symbol,"interval":"5m","limit":120}),
            rest_json(session,SPOT_REST+"/api/v3/depth",{"symbol":symbol,"limit":100}),
            rest_json(session,FUT_REST+"/fapi/v1/depth",{"symbol":symbol,"limit":100}),
            rest_json(session,FUT_REST+"/fapi/v1/openInterest",{"symbol":symbol}),
            rest_json(session,FUT_REST+"/futures/data/topLongShortPositionRatio",{"symbol":symbol,"period":"5m","limit":1}),
            rest_json(session,FUT_REST+"/futures/data/takerlongshortRatio",{"symbol":symbol,"period":"5m","limit":1})
        )
        top=top[0] if isinstance(top,list) and top else {}
        tak=tak[0] if isinstance(tak,list) and tak else {}
        sp=make_spot(symbol,sc,sb)
        fu=make_futures(symbol,fc,fb,float(oi.get("openInterest",0) or 0) if isinstance(oi,dict) else 0,top,tak)
        con="CONFIRMED" if sp["signal"]=="LONG/BUY" and fu["signal"]=="LONG" else ("BEARISH_CONFLUENCE" if fu["signal"]=="SHORT" else "NO_CONFLUENCE")
        state["spot"][symbol]=sp; state["futures"][symbol]=fu; state["confluence"][symbol]=con
    except Exception as e:
        state["last_error"]=f"{symbol}: {type(e).__name__}: {e}"
        logging.exception("refresh failed for %s",symbol)

async def poller():
    timeout=ClientTimeout(total=12)
    async with ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.gather(*(refresh(session,s) for s in WATCHLIST),return_exceptions=True)
            state["updated"]=int(time.time())
            await asyncio.sleep(INTERVAL)

async def ws_loop(url,streams):
    while True:
        try:
            async with websockets.connect(url+"?streams=" + "/".join(streams),ping_interval=20,ping_timeout=20) as ws:
                async for _ in ws: pass
        except Exception as e:
            logging.warning("WebSocket reconnect: %s",e); await asyncio.sleep(3)

async def health(request):
    return web.json_response({"ok":state["updated"] is not None,"updated":state["updated"],"symbols":WATCHLIST,"last_error":state["last_error"]})

async def scan(request):
    return web.json_response(state)

async def main():
    app=web.Application(); app.router.add_get("/health",health); app.router.add_get("/scan",scan)
    runner=web.AppRunner(app); await runner.setup(); site=web.TCPSite(runner,"0.0.0.0",PORT); await site.start()
    spot_streams=[s.lower()+"@aggTrade" for s in WATCHLIST]+[s.lower()+"@depth20@100ms" for s in WATCHLIST]
    fut_streams=[s.lower()+"@aggTrade" for s in WATCHLIST]+[s.lower()+"@depth20@100ms" for s in WATCHLIST]
    await asyncio.gather(poller(),ws_loop(SPOT_WS,spot_streams),ws_loop(FUT_WS,fut_streams))

if __name__=="__main__":
    asyncio.run(main())
