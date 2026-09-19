import asyncio, json, logging, os, sqlite3, time, hashlib
from aiohttp import web, ClientSession, ClientTimeout

WATCHLIST=[x.strip().upper() for x in os.getenv("WATCHLIST","BTCUSDT,ETHUSDT,SOLUSDT,AAVEUSDT,XRPUSDT,VETUSDT,INJUSDT").split(",") if x.strip()]
INTERVAL=max(10,int(os.getenv("SCAN_INTERVAL_SECONDS","15")))
MIN_RR=float(os.getenv("MIN_RR","2.0"))
PORT=int(os.getenv("PORT","8080"))
DB_PATH=os.getenv("JOURNAL_DB","/data/famil_scanner.db")

SPOT_REST="https://api.binance.com"
FUT_REST="https://fapi.binance.com"

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
state={"spot":{},"futures":{},"confluence":{},"updated":None,"last_error":None,"version":"1.0"}

def db():
    os.makedirs(os.path.dirname(DB_PATH) or ".",exist_ok=True)
    c=sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS signals(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, symbol TEXT, market TEXT, direction TEXT,
      version TEXT, timeframe TEXT, entry REAL, stop REAL, tp1 REAL, tp2 REAL, rr REAL,
      payload TEXT, status TEXT DEFAULT 'OPEN', result_ts INTEGER, result_r REAL)""")
    c.commit()
    return c

def valid(c):
    return isinstance(c,list) and len(c)>=60 and all(isinstance(x,list) and len(x)>=11 for x in c)

def vals(c,i): return [float(c[i][j]) for j in (1,2,3,4,5)]
def closes(c): return [float(x[4]) for x in c]
def highs(c): return [float(x[2]) for x in c]
def lows(c): return [float(x[3]) for x in c]
def volumes(c): return [float(x[5]) for x in c]

def ema(a,n):
    if len(a)<n:return None
    k=2/(n+1); e=sum(a[:n])/n
    for x in a[n:]: e=x*k+e*(1-k)
    return e

def rsi(a,n=14):
    if len(a)<n+1:return None
    gains=[]; losses=[]
    for i in range(1,len(a)): 
        d=a[i]-a[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains[-n:])/n; al=sum(losses[-n:])/n
    if al==0:return 100.0
    return 100-(100/(1+ag/al))

def atr(c,n=14):
    if len(c)<n+1:return None
    tr=[]
    for i in range(1,len(c)):
        h=float(c[i][2]); l=float(c[i][3]); pc=float(c[i-1][4])
        tr.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(tr[-n:])/n

def structure(c):
    if not valid(c): return "NEUTRAL",{}
    h=highs(c); l=lows(c); cl=closes(c)
    hh=max(h[-12:]); ll=min(l[-12:]); p=cl[-1]
    prev_h=max(h[-24:-12]); prev_l=min(l[-24:-12])
    bull=p>prev_h and cl[-1]>cl[-3]
    bear=p<prev_l and cl[-1]<cl[-3]
    if bull:return "BULLISH",{"bos":True,"high":hh,"low":ll}
    if bear:return "BEARISH",{"bos":True,"high":hh,"low":ll}
    # HH/HL or LH/LL proxy using halves of recent range
    if max(h[-6:])>max(h[-12:-6]) and min(l[-6:])>=min(l[-12:-6]): return "BULLISH",{"bos":False}
    if min(l[-6:])<min(l[-12:-6]) and max(h[-6:])<=max(h[-12:-6]): return "BEARISH",{"bos":False}
    return "NEUTRAL",{"bos":False}

def levels(c):
    if not valid(c): return None
    h=highs(c); l=lows(c); p=closes(c)[-1]
    return {"price":p,"support":min(l[-30:]),"resistance":max(h[-30:])}

def sweep(c,side):
    if not valid(c): return False,None
    h=highs(c); l=lows(c); cl=closes(c)
    if side=="LONG":
        level=min(l[-8:-1]); hit=l[-1]<level and cl[-1]>level
        return hit,level
    level=max(h[-8:-1]); hit=h[-1]>level and cl[-1]<level
    return hit,level

def order_block(c):
    if not valid(c): return "NONE",None
    a=atr(c)
    if not a:return "NONE",None
    for i in range(len(c)-3,max(2,len(c)-25),-1):
        o,h,l,cl=map(float,[c[i][1],c[i][2],c[i][3],c[i][4]])
        ncl=float(c[i+2][4])
        if cl<o and ncl-h >= 1.0*a:
            return "BULLISH",{"low":l,"high":o}
        if cl>o and l-ncl >= 1.0*a:
            return "BEARISH",{"low":o,"high":h}
    return "NONE",None

def book_metrics(book):
    if not isinstance(book,dict):return {"imbalance":0.0,"bid_wall":0.0,"ask_wall":0.0}
    try:
        bids=[(float(x[0]),float(x[1])) for x in book.get("bids",[])[:50]]
        asks=[(float(x[0]),float(x[1])) for x in book.get("asks",[])[:50]]
        bs=sum(q for _,q in bids); ass=sum(q for _,q in asks)
        return {"imbalance":(bs-ass)/(bs+ass) if bs+ass else 0.0,
                "bid_wall":max([q for _,q in bids] or [0]),"ask_wall":max([q for _,q in asks] or [0])}
    except Exception:return {"imbalance":0.0,"bid_wall":0.0,"ask_wall":0.0}

def tech(c):
    if not valid(c):return {}
    cl=closes(c); vol=volumes(c); d,meta=structure(c); lv=levels(c); a=atr(c)
    rv=vol[-1]/(sum(vol[-21:-1])/20) if len(vol)>21 and sum(vol[-21:-1]) else 0
    return {"direction":d,"meta":meta,"levels":lv,"atr":a,"ema20":ema(cl,20),"ema50":ema(cl,50),"rsi":rsi(cl),"rel_volume":rv}

def signal_from(c5,c15,c1h,book,kind,extra=None):
    t5=tech(c5); t15=tech(c15); t1=tech(c1h)
    if not t5 or not t15 or not t1:return {"signal":"WAIT","why_no_entry":["insufficient data"]}
    bm=book_metrics(book); p=t5["levels"]["price"]; reasons=[]
    bull_htf=t1["direction"]=="BULLISH" or t15["direction"]=="BULLISH"
    bear_htf=t1["direction"]=="BEARISH" or t15["direction"]=="BEARISH"
    bull5=t5["direction"]=="BULLISH"; bear5=t5["direction"]=="BEARISH"
    obt,ob=order_block(c5)
    swL,sl=sweep(c5,"LONG"); swS,sh=sweep(c5,"SHORT")
    rv=t5["rel_volume"]
    if kind=="SPOT":
        if bull_htf and bull5 and (swL or obt=="BULLISH") and bm["imbalance"]>0.05 and rv>=1.0:
            stop=min(sl if swL else t5["levels"]["support"],ob["low"] if obt=="BULLISH" else t5["levels"]["support"])
            tp1=t5["levels"]["resistance"]; risk=p-stop; rr=(tp1-p)/risk if risk>0 else 0
            if rr>=MIN_RR:
                return {"price":p,"signal":"LONG CONFIRMED","entry":p,"stop":stop,"tp1":tp1,"tp2":tp1+2*risk,"rr":rr,
                        "structure":t5,"htf":t15,"ob_type":obt,"ob":ob,"sweep":swL,"book":bm,"rel_volume":rv,"why_no_entry":[]}
        if not bull_htf:reasons.append("higher-timeframe bullish context missing")
        if not bull5:reasons.append("5m bullish structure missing")
        if not swL and obt!="BULLISH":reasons.append("liquidity sweep or bullish OB not confirmed")
        if bm["imbalance"]<=0.05:reasons.append("spot order-book demand not confirmed")
        if rv<1.0:reasons.append("relative volume below baseline")
        return {"price":p,"signal":"WAIT","structure":t5,"htf":t15,"book":bm,"ob_type":obt,"ob":ob,"sweep":swL,"rel_volume":rv,"why_no_entry":reasons}
    e=extra or {}
    flow=e.get("taker_flow",0.0); oi_delta=e.get("oi_delta",0.0); funding=e.get("funding",0.0); basis=e.get("basis",0.0)
    long_top=e.get("top_long_ratio",1.0)
    if bull_htf and bull5 and (swL or obt=="BULLISH") and bm["imbalance"]>0.05 and flow>0.05 and rv>=1.0:
        stop=min(sl if swL else t5["levels"]["support"],ob["low"] if obt=="BULLISH" else t5["levels"]["support"])
        tp1=t5["levels"]["resistance"]; risk=p-stop; rr=(tp1-p)/risk if risk>0 else 0
        if rr>=MIN_RR:return {"signal":"LONG CONFIRMED","entry":p,"stop":stop,"tp1":tp1,"tp2":tp1+2*risk,"rr":rr,"structure":t5,"htf":t15,"ob_type":obt,"ob":ob,"sweep":swL,"book":bm,"rel_volume":rv,"flow":flow,"oi_delta":oi_delta,"funding":funding,"basis":basis,"top_long_ratio":long_top,"why_no_entry":[]}
    if bear_htf and bear5 and (swS or obt=="BEARISH") and bm["imbalance"]<-0.05 and flow<-0.05 and rv>=1.0:
        stop=max(sh if swS else t5["levels"]["resistance"],ob["high"] if obt=="BEARISH" else t5["levels"]["resistance"])
        tp1=t5["levels"]["support"]; risk=stop-p; rr=(p-tp1)/risk if risk>0 else 0
        if rr>=MIN_RR:return {"price":p,"signal":"SHORT CONFIRMED","entry":p,"stop":stop,"tp1":tp1,"tp2":tp1-2*risk,"rr":rr,"structure":t5,"htf":t15,"ob_type":obt,"ob":ob,"sweep":swS,"book":bm,"rel_volume":rv,"flow":flow,"oi_delta":oi_delta,"funding":funding,"basis":basis,"top_long_ratio":long_top,"why_no_entry":[]}
    if not bull_htf and not bear_htf:reasons.append("higher-timeframe structure neutral")
    if not bull5 and not bear5:reasons.append("5m structure neutral")
    if abs(bm["imbalance"])<=0.05:reasons.append("futures order-book pressure neutral")
    if abs(flow)<=0.05:reasons.append("taker flow not directional")
    if rv<1.0:reasons.append("relative volume below baseline")
    return {"signal":"WAIT","structure":t5,"htf":t15,"book":bm,"ob_type":obt,"ob":ob,"sweep":swL or swS,"rel_volume":rv,"flow":flow,"oi_delta":oi_delta,"funding":funding,"basis":basis,"top_long_ratio":long_top,"why_no_entry":reasons}

async def rest(session,url,params):
    try:
        async with session.get(url,params=params) as r:
            d=await r.json(content_type=None)
            return d if r.status==200 else {}
    except Exception:return {}

def flow_from(x):
    if not isinstance(x,list) or not x:return 0.0
    d=x[-1]
    b=float(d.get("takerBuyVolValue",d.get("buyVol",0)) or 0); s=float(d.get("takerSellVolValue",d.get("sellVol",0)) or 0)
    return (b-s)/(b+s) if b+s else 0.0

async def refresh(session,symbol):
    try:
        sc,sm,sl,fc,f15,f1h,fb,oi,ois,fr,basis,top,tak=await asyncio.gather(
          rest(session,SPOT_REST+"/api/v3/klines",{"symbol":symbol,"interval":"5m","limit":150}),
          rest(session,SPOT_REST+"/api/v3/klines",{"symbol":symbol,"interval":"15m","limit":150}),
          rest(session,SPOT_REST+"/api/v3/klines",{"symbol":symbol,"interval":"1h","limit":150}),
          rest(session,FUT_REST+"/fapi/v1/klines",{"symbol":symbol,"interval":"5m","limit":150}),
          rest(session,FUT_REST+"/fapi/v1/klines",{"symbol":symbol,"interval":"15m","limit":150}),
          rest(session,FUT_REST+"/fapi/v1/klines",{"symbol":symbol,"interval":"1h","limit":150}),
          rest(session,FUT_REST+"/fapi/v1/depth",{"symbol":symbol,"limit":100}),
          rest(session,FUT_REST+"/fapi/v1/openInterest",{"symbol":symbol}),
          rest(session,FUT_REST+"/futures/data/openInterestHist",{"symbol":symbol,"period":"5m","limit":3}),
          rest(session,FUT_REST+"/fapi/v1/fundingRate",{"symbol":symbol,"limit":3}),
          rest(session,FUT_REST+"/futures/data/basis",{"pair":symbol,"contractType":"PERPETUAL","period":"5m","limit":3}),
          rest(session,FUT_REST+"/futures/data/topLongShortPositionRatio",{"symbol":symbol,"period":"5m","limit":1}),
          rest(session,FUT_REST+"/futures/data/takerBuySellVol",{"symbol":symbol,"contractType":"PERPETUAL","period":"5m","limit":3})
        )
        topd=top[-1] if isinstance(top,list) and top else {}
        funding=float(fr[-1].get("fundingRate",0) or 0) if isinstance(fr,list) and fr else 0
        basisv=float(basis[-1].get("basisRate",basis[-1].get("basis",0)) or 0) if isinstance(basis,list) and basis else 0
        oi_delta=0
        if isinstance(ois,list) and len(ois)>=2:
            a=float(ois[-2].get("sumOpenInterestValue",ois[-2].get("sumOpenInterest",0)) or 0); b=float(ois[-1].get("sumOpenInterestValue",ois[-1].get("sumOpenInterest",0)) or 0)
            oi_delta=(b-a)/a if a else 0
        extra={"taker_flow":flow_from(tak),"oi_delta":oi_delta,"funding":funding,"basis":basisv,"top_long_ratio":float(topd.get("longShortRatio",1) or 1)}
        sp=signal_from(sc,sm,sl,await rest(session,SPOT_REST+"/api/v3/depth",{"symbol":symbol,"limit":100}),"SPOT")
        fu=signal_from(fc,f15,f1h,fb,"FUTURES",extra)
        state["spot"][symbol]=sp; state["futures"][symbol]=fu
        if sp["signal"]=="LONG CONFIRMED" and fu["signal"]=="LONG CONFIRMED": state["confluence"][symbol]="LONG"
        elif fu["signal"]=="SHORT CONFIRMED": state["confluence"][symbol]="SHORT"
        else: state["confluence"][symbol]="NO ENTRY"
        journal(symbol,"SPOT",sp); journal(symbol,"FUTURES",fu)
        resolve_results(symbol,float(oi.get("openInterest",0) or 0))
    except Exception as e:
        state["last_error"]=f"{symbol}: {type(e).__name__}: {e}"
        logging.exception("refresh failed for %s",symbol)

def journal(symbol,market,s):
    if s.get("signal") not in ("LONG CONFIRMED","SHORT CONFIRMED"):return
    direction="LONG" if s["signal"].startswith("LONG") else "SHORT"
    payload=json.dumps(s,separators=(",",":"))
    h=hashlib.sha256(payload.encode()).hexdigest()
    c=db()
    exists=c.execute("SELECT id FROM signals WHERE symbol=? AND market=? AND direction=? AND status='OPEN' ORDER BY id DESC LIMIT 1",(symbol,market,direction)).fetchone()
    if not exists:
        c.execute("INSERT INTO signals(ts,symbol,market,direction,version,timeframe,entry,stop,tp1,tp2,rr,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
          (int(time.time()),symbol,market,direction,state["version"],"1h/15m/5m",s["entry"],s["stop"],s["tp1"],s["tp2"],s["rr"],json.dumps({"hash":h,"signal":s})))
        c.commit()
    c.close()

def resolve_results(symbol,market,candles):
    if not valid(candles) or len(candles)<2:return
    # Evaluate the last completed candle so results are not based on an unfinished bar.
    hi=float(candles[-2][2]); lo=float(candles[-2][3])
    c=db()
    rows=c.execute("SELECT id,direction,entry,stop,tp1,tp2 FROM signals WHERE symbol=? AND market=? AND status='OPEN'",(symbol,market)).fetchall()
    for sid,direction,entry,stop,tp1,tp2 in rows:
        hit_stop = lo<=stop if direction=="LONG" else hi>=stop
        hit_tp1 = hi>=tp1 if direction=="LONG" else lo<=tp1
        hit_tp2 = hi>=tp2 if direction=="LONG" else lo<=tp2
        status=None; r=None
        if hit_stop and (hit_tp1 or hit_tp2):
            status="AMBIGUOUS"
        elif hit_tp2:
            status="WIN_TP2"; r=(tp2-entry)/abs(entry-stop) if direction=="LONG" else (entry-tp2)/abs(stop-entry)
        elif hit_tp1:
            status="WIN_TP1"; r=(tp1-entry)/abs(entry-stop) if direction=="LONG" else (entry-tp1)/abs(stop-entry)
        elif hit_stop:
            status="LOSS"; r=-1.0
        if status:
            c.execute("UPDATE signals SET status=?,result_ts=?,result_r=? WHERE id=?",(status,int(time.time()),r,sid))
    c.commit(); c.close()
async def weekly_report(request):
    since=int(time.time())-7*24*3600
    c=db()
    rows=c.execute("SELECT market,direction,status,COUNT(*) FROM signals WHERE ts>=? GROUP BY market,direction,status",(since,)).fetchall()
    total=c.execute("SELECT COUNT(*) FROM signals WHERE ts>=?",(since,)).fetchone()[0]
    closed=c.execute("SELECT COUNT(*) FROM signals WHERE ts>=? AND status NOT IN ('OPEN','AMBIGUOUS')",(since,)).fetchone()[0]
    wins=c.execute("SELECT COUNT(*) FROM signals WHERE ts>=? AND status IN ('WIN_TP1','WIN_TP2')",(since,)).fetchone()[0]
    avg_r=c.execute("SELECT AVG(result_r) FROM signals WHERE ts>=? AND result_r IS NOT NULL",(since,)).fetchone()[0]
    expectancy=c.execute("SELECT AVG(result_r) FROM signals WHERE ts>=? AND status NOT IN ('OPEN','AMBIGUOUS')",(since,)).fetchone()[0]
    report={"version":state["version"],"period_days":7,"generated_at":int(time.time()),"total_signals":total,
            "closed":closed,"wins_tp1_or_tp2":wins,"observed_accuracy":(wins/closed if closed else None),
            "average_R":avg_r,"expectancy_R":expectancy,"breakdown":rows}
    c.close(); return web.json_response(report)

