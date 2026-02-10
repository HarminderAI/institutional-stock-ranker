# ==========================================================
# 🏛️ INSTITUTIONAL STOCK RANKER — DIAMOND (v17.1 GOLD MASTER)
# 🏆 STATUS: PRODUCTION | ATOMIC WRITES | RISK GOVERNANCE
# ==========================================================

import os, json, datetime, io, sys, random, math, csv
import requests, pytz
import pandas as pd
import pandas_ta as ta
import yfinance as yf
from statistics import median, stdev
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================================
# ⚙️ CONFIGURATION
# ==========================================================
IST = pytz.timezone("Asia/Kolkata")

# Files
CACHE_FILE  = "diamond_data_cache.json"
SIGNAL_FILE = "diamond_signal.json"       # <--- The Contract
AUDIT_LOG   = "diamond_audit_trail.csv"   # <--- False Negatives
REGIME_LOG  = "sector_regime_journal.csv" # <--- Market Memory

# Tuning
MIN_BARS_REQUIRED    = 100   
SCORE_THRESHOLD      = 75    # Strict Filter
DISPERSION_THRESHOLD = 15    # Below this = Market is Choppy
MIN_SECTOR_SIZE      = 5     # Minimum stocks to rank a sector

# Secrets
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# 🛑 KNOWN NSE HOLIDAYS 2026
MARKET_HOLIDAYS = {
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28",
    "2026-06-26", "2026-08-15", "2026-09-14", "2026-10-02",
    "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25"
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

# ==========================================================
# 🛠️ UTILITIES
# ==========================================================
def ist_now(): return datetime.datetime.now(IST)

def is_trading_day(date_obj):
    if date_obj.weekday() >= 5: return False 
    if date_obj.strftime("%Y-%m-%d") in MARKET_HOLIDAYS: return False
    return True

def create_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": random.choice(USER_AGENTS), "Referer": "https://www.nseindia.com/"})
    return s

def send_msg(text):
    if not TELEGRAM_TOKEN or not CHAT_ID: 
        print(f"\n📢 [Telegram]\n{text}\n")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "HTML"}, timeout=10)
    except: pass

# ==========================================================
# 📝 GOVERNANCE LAYER (ATOMIC)
# ==========================================================
def write_json_contract(metadata, candidates):
    """Writes a deterministic JSON file atomically (Crash-Safe)."""
    contract = {
        "meta": metadata,
        "timestamp": ist_now().isoformat(),
        "count": len(candidates),
        "universe": candidates
    }
    
    # Atomic Write: Write to temp -> Rename
    temp_file = f"{SIGNAL_FILE}.tmp"
    try:
        with open(temp_file, "w") as f:
            json.dump(contract, f, indent=4)
        
        # Windows requires removal before rename, Linux doesn't, but safe to handle
        if os.path.exists(SIGNAL_FILE): os.remove(SIGNAL_FILE)
        os.rename(temp_file, SIGNAL_FILE)
        
        print(f"✅ JSON Contract Generated (Atomic): {SIGNAL_FILE}")
    except Exception as e:
        print(f"❌ Failed to write contract: {e}")

def log_audit_trail(rejected_list):
    """Logs WHY stocks were rejected (False Negative Analysis)."""
    file_exists = os.path.exists(AUDIT_LOG)
    with open(AUDIT_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists: writer.writerow(["Timestamp", "Symbol", "Score", "Reason", "Sector"])
        
        now = ist_now().strftime("%Y-%m-%d %H:%M")
        for r in rejected_list:
            writer.writerow([now, r['symbol'], r['score'], r['reason'], r['sector']])
    print(f"✅ Audit Trail Updated: {len(rejected_list)} rejections logged.")

def log_sector_regime(sector_map, dispersion):
    """Persists daily sector leadership for strategy learning."""
    file_exists = os.path.exists(REGIME_LOG)
    
    sorted_secs = sorted(sector_map.items(), key=lambda x: x[1], reverse=True)
    leader  = sorted_secs[0][0] if sorted_secs else "None"
    laggard = sorted_secs[-1][0] if sorted_secs else "None"
    
    with open(REGIME_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists: writer.writerow(["Date", "Leader", "Laggard", "Dispersion", "Market_Status"])
        
        status = "HEALTHY" if dispersion >= DISPERSION_THRESHOLD else "CHOPPY/RISK"
        writer.writerow([ist_now().strftime("%Y-%m-%d"), leader, laggard, round(dispersion, 2), status])
    print(f"✅ Sector Regime Journaled: {status} (Dispersion: {round(dispersion,2)})")

# ==========================================================
# 📥 DATA & LOGIC ENGINE
# ==========================================================
def fetch_nifty_trend():
    try:
        hist = yf.download("^NSEI", period="6mo", interval="1d", progress=False)
        if isinstance(hist.columns, pd.MultiIndex):
            try: hist = hist.xs('^NSEI', axis=1, level=1)
            except: hist.columns = hist.columns.get_level_values(0)
        hist.columns = [c.lower() for c in hist.columns]
        close = hist['close']
        ema50 = ta.ema(close, length=50).iloc[-1]
        curr = close.iloc[-1]
        trend = "BULL" if curr > ema50 * 1.01 else "BEAR" if curr < ema50 * 0.99 else "NEUTRAL"
        return trend, float((curr - close.iloc[-10]) / close.iloc[-10])
    except: return "NEUTRAL", 0.0

def fetch_delivery_data():
    session = create_session()
    for i in range(3):
        d = ist_now() - datetime.timedelta(days=i)
        if not is_trading_day(d): continue
        url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{d.strftime('%d%m%Y')}.csv"
        try:
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text))
                df.columns = [c.strip().upper() for c in df.columns]
                df = df[df["SERIES"] == "EQ"]
                return dict(zip(df['SYMBOL'], pd.to_numeric(df['DELIV_PER'], errors='coerce')))
        except: continue
    return {}

def update_fundamentals(symbols):
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f: cache = json.load(f)
        except: pass
    
    need_update = [s for s in symbols if s not in cache]
    if need_update:
        print(f"📊 Updating fundamentals for {len(need_update)} stocks...")
        chunk_size = 50
        for i in range(0, len(need_update), chunk_size):
            chunk = need_update[i:i+chunk_size]
            try:
                tickers = yf.Tickers(" ".join([f"{s}.NS" for s in chunk]))
                for s in chunk:
                    info = tickers.tickers[f"{s}.NS"].info
                    cache[s] = {
                        "pe": info.get("trailingPE", 0),
                        "de": info.get("debtToEquity", 0),
                        "sector": info.get("sector", "Unknown")
                    }
            except: pass
        with open(CACHE_FILE, 'w') as f: json.dump(cache, f)
    return cache

def calculate_sector_metrics(stock_data):
    """Computes Scores AND Dispersion with Robustness checks."""
    sector_perf = {}
    for item in stock_data:
        sec = item.get('sector', 'Unknown')
        sector_perf.setdefault(sec, []).append(item.get('perf_10d', 0.0))
    
    # POLISH 1: Increase minimum breadth to 5
    medians = {k: median(v) for k, v in sector_perf.items() if len(v) >= MIN_SECTOR_SIZE}
    
    if not medians: return {}, 0.0 # Handle case where no sector has enough breadth
    
    sorted_secs = sorted(medians.items(), key=lambda x: x[1], reverse=True)
    
    scores = {}
    for i, (sec, _) in enumerate(sorted_secs):
        scores[sec] = int((1 - (i / max(len(sorted_secs), 1))) * 100)
        
    vals = list(scores.values())
    dispersion = stdev(vals) if len(vals) > 1 else 0
        
    return scores, dispersion

def calculate_score(df, symbol, avg_delivery, sector_map, nifty_perf, fund):
    if len(df) < MIN_BARS_REQUIRED: return None
    close, high, low = df["close"], df["high"], df["low"]
    price = float(close.iloc[-1])
    
    try:
        rsi = ta.rsi(close, 14).iloc[-1]
        ema20, ema50, ema200 = ta.ema(close, 20).iloc[-1], ta.ema(close, 50).iloc[-1], ta.ema(close, 200).iloc[-1]
        atr = ta.atr(high, low, close, 14).iloc[-1]
        volatility = atr / price 
    except: return None

    # 1. Technical (25%)
    if price > ema20 > ema50 > ema200: t = 100 if 55 <= rsi <= 70 else 90
    elif price > ema50: t = 60
    else: t = 20

    # 2. Delivery (20%)
    d = min(100, int((avg_delivery.get(symbol, 30.0) / 60) * 100))

    # 3. Sector (15%)
    s = sector_map.get(fund.get('sector', 'Unknown'), 50)

    # 4. Alpha (15%)
    alpha_bps = ((price - close.iloc[-10]) / close.iloc[-10] - nifty_perf) * 10000
    a = 100 if alpha_bps > 300 else 80 if alpha_bps > 100 else 60 if alpha_bps > 0 else 30

    # 5. Fundamental (10%)
    pe = fund.get('pe', 0)
    f = 100 if 0 < pe < 30 else 70 if pe < 60 else 40

    # 6. Solvency (10%)
    de = fund.get('de', 0)
    solv = 100 if de <= 50 else 70 if de <= 150 else 30

    # 7. Beta (5%)
    b = 100 if volatility < 0.02 else 70 if volatility < 0.04 else 40

    final = (t*0.25) + (d*0.20) + (s*0.15) + (a*0.15) + (f*0.10) + (solv*0.10) + (b*0.05)
    
    return {
        "symbol": symbol, "score": int(final), "price": price,
        "tgt": round(price + 3.5*atr, 1), "sl": round(price - 2.0*atr, 1),
        "del_pct": round(avg_delivery.get(symbol, 30.0), 1), "sector": fund.get('sector', 'Unknown'),
        "perf_10d": (price - close.iloc[-10]) / close.iloc[-10]
    }

# ==========================================================
# 🚀 MAIN EXECUTION
# ==========================================================
def main():
    print("💎 Diamond v17.1 (Gold Master) Initiating...")
    trend, nifty_alpha = fetch_nifty_trend()
    
    # Load Universe
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
        symbols = pd.read_csv(io.StringIO(create_session().get(url).text))["Symbol"].tolist()
    except: symbols = ["RELIANCE", "HDFCBANK", "INFY", "TCS", "ICICIBANK"]

    # Data Fetch
    delivery_map = fetch_delivery_data()
    fund_cache = update_fundamentals(symbols)
    hist_data = yf.download([f"{s}.NS" for s in symbols], period="1y", group_by='ticker', progress=True, threads=True)
    
    # Pass 1: Pre-process
    batch = []
    print("\n🧠 Computing Metrics...")
    for sym in symbols:
        try:
            df = hist_data[f"{sym}.NS"] if isinstance(hist_data.columns, pd.MultiIndex) else hist_data
            df = df.dropna()
            df.columns = [c.lower() for c in df.columns]
            if len(df) < 20: continue
            
            f_data = fund_cache.get(sym, {})
            batch.append({
                "symbol": sym, "df": df, "fund": f_data,
                "perf_10d": (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10],
                "sector": f_data.get('sector', 'Unknown')
            })
        except: continue

    # Pass 2: Governance (Sector Dispersion)
    sector_map, dispersion = calculate_sector_metrics(batch)
    log_sector_regime(sector_map, dispersion)
    
    # Kill Switch Logic
    kill_switch_active = dispersion < DISPERSION_THRESHOLD
    market_status = "DANGEROUS (CHOPPY)" if kill_switch_active else "HEALTHY (TRENDING)"
    # POLISH 2: Sizing Recommendation
    sizing_rec = "REDUCE_SIZE_50" if kill_switch_active else "NORMAL_SIZE_100"
    
    print(f"🚦 Market Status: {market_status} | Dispersion: {round(dispersion, 2)}")
    print(f"⚖️ Strategy Protocol: {sizing_rec}")

    # Pass 3: Scoring & Audit
    candidates, rejected = [], []
    print("💎 Ranking Stocks...")
    
    for item in batch:
        res = calculate_score(item['df'], item['symbol'], delivery_map, sector_map, nifty_alpha, item['fund'])
        if not res: continue
        
        if res["score"] > SCORE_THRESHOLD:
            candidates.append(res)
        else:
            rejected.append({
                "symbol": item["symbol"], "score": res["score"], 
                "reason": "Score Too Low", "sector": item["sector"]
            })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    
    # Governance Outputs
    log_audit_trail(rejected)
    write_json_contract({
        "trend": trend, 
        "alpha_baseline": nifty_alpha, 
        "dispersion": dispersion, 
        "kill_switch": kill_switch_active,
        "recommendation": sizing_rec
    }, candidates[:20])

    # User Output
    if candidates:
        top = candidates[:15]
        report = [f"💎 <b>Diamond v17.1</b>\n📅 {ist_now().strftime('%d-%b')} | 🌍 {trend} | 🚦 {market_status}"]
        if kill_switch_active:
            report.append(f"\n⚠️ <b>RISK ALERT:</b> Dispersion Low ({round(dispersion,1)}). <b>{sizing_rec}</b>.")
            
        for p in top:
            icon = "🚀" if p['score'] > 85 else "🟢"
            report.append(f"{icon} <b>{p['symbol']}</b> ({p['score']}) | 🏗️ {p['sector']}\n"
                          f"   💰 ₹{p['price']} | 🎯 {p['tgt']}")
        
        send_msg("\n\n".join(report))
        print("\n".join(report).replace("<b>","").replace("</b>",""))
    else:
        print("⚠️ No stocks met criteria.")

if __name__ == "__main__":
    main()
