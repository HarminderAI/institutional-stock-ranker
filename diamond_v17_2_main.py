# ==========================================================
# 🏛️ DIAMOND v17.2 — STRATEGY ENGINE (FORTIFIED & COMPLETE)
# 🏆 STATUS: PRODUCTION | SELF-HEALING | AUDITABLE
# ==========================================================

import os, json, time, datetime, io, sys, random, math, csv
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
CACHE_FILE      = "diamond_data_cache.json"
SIGNAL_FILE     = "diamond_signal.json"
QUARANTINE_FILE = "diamond_quarantine.json"
AUDIT_LOG       = "diamond_audit_trail.csv"
REGIME_LOG      = "sector_regime_journal.csv"

# Safety Thresholds
MAX_FAILURE_RATE = 0.15     # Abort if >15% of universe fails
FUNDAMENTAL_ABORT= 0.05     # Skip fundamentals if >5% price fetch fails
QUARANTINE_DAYS  = 7        # Jail bad symbols for a week
HEALTH_PROBE_SYM = "SBIN"   # Connectivity check ticker

# Tuning
MIN_BARS_REQUIRED = 100
MIN_SECTOR_SIZE   = 5
SCORE_THRESHOLD   = 75
DISPERSION_THRESHOLD = 15

# Secrets
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

# ==========================================================
# 🛡️ DEFENSE LAYER (FIXED)
# ==========================================================
def load_quarantine():
    if not os.path.exists(QUARANTINE_FILE): return {}
    try:
        with open(QUARANTINE_FILE, "r") as f: return json.load(f)
    except: return {}

def save_quarantine(q_data):
    with open(QUARANTINE_FILE, "w") as f: json.dump(q_data, f, indent=2)

def is_quarantined(symbol, q_data):
    """Checks jail status and handles parole persistence."""
    if symbol not in q_data: return False
    
    try:
        jail_date = datetime.datetime.strptime(q_data[symbol], "%Y-%m-%d").date()
        days_served = (datetime.datetime.now().date() - jail_date).days
        
        if days_served >= QUARANTINE_DAYS:
            print(f"🔓 Paroled: {symbol} (Served {days_served} days)")
            del q_data[symbol]
            save_quarantine(q_data) # [FIX 1] Persist parole to disk
            return False
        return True
    except:
        return False # If date parsing fails, release

def add_to_quarantine(symbol, reason, q_data):
    print(f"☣️ QUARANTINING {symbol}: {reason}")
    q_data[symbol] = datetime.datetime.now().strftime("%Y-%m-%d")
    save_quarantine(q_data)

def check_market_health():
    """Probe Yahoo Finance connectivity before starting."""
    print(f"🩺 Probing Market Health ({HEALTH_PROBE_SYM}.NS)...")
    try:
        df = yf.download(f"{HEALTH_PROBE_SYM}.NS", period="5d", progress=False, threads=False)
        if df.empty or len(df) < 2: raise Exception("Empty Data")
        print("✅ Pulse Good.")
        return True
    except Exception as e:
        print(f"💀 HEALTH CHECK FAILED: {e}")
        return False

# ==========================================================
# 🛠️ UTILITIES
# ==========================================================
def ist_now(): return datetime.datetime.now(IST)

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
# 📥 DATA ENGINE
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
        if d.weekday() >= 5: continue
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

def update_fundamentals_batch(symbols, quarantine_list):
    """Safely updates fundamentals with Jitter."""
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f: cache = json.load(f)
        except: cache = {}

    today = datetime.datetime.now()
    to_update = []
    
    for s in symbols:
        if s in quarantine_list: continue
        last_date = cache.get(s, {}).get('date', '2000-01-01')
        days_old = (today - datetime.datetime.strptime(last_date, "%Y-%m-%d")).days
        if s not in cache or days_old > 7:
            to_update.append(s)

    if to_update:
        print(f"📊 Updating fundamentals for {len(to_update)} stocks (Stealth)...")
        random.shuffle(to_update)
        chunk_size = 10 
        
        for i in range(0, len(to_update), chunk_size):
            chunk = to_update[i:i+chunk_size]
            try:
                tickers = yf.Tickers(" ".join([f"{s}.NS" for s in chunk]))
                for s in chunk:
                    try:
                        info = tickers.tickers[f"{s}.NS"].info
                        cache[s] = {
                            "date": today.strftime("%Y-%m-%d"),
                            "pe": info.get("trailingPE", 0),
                            "de": info.get("debtToEquity", 0),
                            "sector": info.get("sector", "Unknown")
                        }
                        time.sleep(random.uniform(0.5, 1.5)) # Jitter
                    except: 
                        cache[s] = {"date": today.strftime("%Y-%m-%d"), "pe":0, "de":0, "sector": "Unknown"}
                
                print(f"   ... Batch {i//chunk_size + 1} done")
                time.sleep(random.uniform(2.0, 4.0)) # Batch pause
            except Exception: pass
            
        with open(CACHE_FILE, 'w') as f: json.dump(cache, f)
        
    return cache

def load_existing_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f: return json.load(f)
        except: pass
    return {}

# ==========================================================
# 🧠 LOGIC ENGINE (RESTORED FROM v17.1)
# ==========================================================
def calculate_sector_metrics(stock_data):
    sector_perf = {}
    for item in stock_data:
        sec = item.get('sector', 'Unknown')
        sector_perf.setdefault(sec, []).append(item.get('perf_10d', 0.0))
    
    medians = {k: median(v) for k, v in sector_perf.items() if len(v) >= MIN_SECTOR_SIZE}
    if not medians: return {}, 0.0
    
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
# 📝 GOVERNANCE (RESTORED)
# ==========================================================
def write_json_contract(metadata, candidates):
    """Writes the v17 signal file atomically."""
    contract = {
        "meta": metadata,
        "timestamp": ist_now().isoformat(),
        "count": len(candidates),
        "universe": candidates
    }
    temp_file = f"{SIGNAL_FILE}.tmp"
    try:
        with open(temp_file, "w") as f: json.dump(contract, f, indent=4)
        if os.path.exists(SIGNAL_FILE): os.remove(SIGNAL_FILE)
        os.rename(temp_file, SIGNAL_FILE)
        print(f"✅ JSON Contract Generated: {SIGNAL_FILE}")
    except Exception as e:
        print(f"❌ Failed to write contract: {e}")

def log_audit_trail(rejected_list):
    file_exists = os.path.exists(AUDIT_LOG)
    with open(AUDIT_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists: writer.writerow(["Timestamp", "Symbol", "Score", "Reason", "Sector"])
        now = ist_now().strftime("%Y-%m-%d %H:%M")
        for r in rejected_list:
            writer.writerow([now, r['symbol'], r['score'], r['reason'], r['sector']])

def log_sector_regime(sector_map, dispersion):
    file_exists = os.path.exists(REGIME_LOG)
    sorted_secs = sorted(sector_map.items(), key=lambda x: x[1], reverse=True)
    leader = sorted_secs[0][0] if sorted_secs else "None"
    with open(REGIME_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists: writer.writerow(["Date", "Leader", "Dispersion", "Status"])
        status = "HEALTHY" if dispersion >= DISPERSION_THRESHOLD else "CHOPPY"
        writer.writerow([ist_now().strftime("%Y-%m-%d"), leader, round(dispersion, 2), status])

# ==========================================================
# 🚀 MAIN EXECUTION
# ==========================================================
def main():
    print("💎 Diamond v17.2 (Fortified) Initiating...")
    
    # 1. Health Probe
    if not check_market_health(): return

    # 2. Load Quarantine
    q_data = load_quarantine()
    print(f"☣️ Quarantine: {len(q_data)} symbols.")

    # 3. Market Context
    trend, nifty_alpha = fetch_nifty_trend()
    delivery_map = fetch_delivery_data()
    
    # 4. Load Universe
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
        s = create_session()
        df_list = pd.read_csv(io.StringIO(s.get(url, timeout=10).text))
        raw_symbols = df_list["Symbol"].tolist()
    except: 
        print("⚠️ NSE List Error. Aborting.")
        return

    # 5. Filter Universe
    symbols = [s for s in raw_symbols if not is_quarantined(s, q_data)]
    print(f"📋 Universe: {len(symbols)}")

    # 6. Bulk Fetch
    print("⏳ Fetching Prices...")
    hist_data = yf.download([f"{s}.NS" for s in symbols], period="1y", group_by='ticker', progress=True, threads=4)
    
    valid_batch = []
    failures = 0
    
    for sym in symbols:
        try:
            df = hist_data[f"{sym}.NS"] if isinstance(hist_data.columns, pd.MultiIndex) else hist_data
            
            # [FIX 2] Logic: Don't auto-quarantine on global count. Only log.
            if df.empty or len(df) < 20: 
                failures += 1
                # Optional: specific symbol logic here if desired later
                continue

            df = df.dropna()
            df.columns = [c.lower() for c in df.columns]
            valid_batch.append({"symbol": sym, "df": df})
        except: 
            failures += 1
            continue

    fail_rate = failures / len(symbols)
    print(f"📊 Failure Rate: {fail_rate:.1%}")
    
    # 7. Circuit Breaker (Abort Run)
    if fail_rate > MAX_FAILURE_RATE:
        print(f"🛑 FAILURE RATE TOO HIGH. ABORTING.")
        send_msg(f"⚠️ Diamond v17.2 Aborted: {fail_rate:.1%} Failures.")
        return

    # 8. Fundamentals (Conditional Update) [FIX 3]
    if fail_rate < FUNDAMENTAL_ABORT:
        fund_cache = update_fundamentals_batch(symbols, q_data)
    else:
        print("🛑 High Fail Rate: Skipping Fundamentals Update (Using Cache).")
        fund_cache = load_existing_cache()

    # 9. Processing
    print("\n🧠 Computing Metrics...")
    processed_data = []
    for item in valid_batch:
        sym = item['symbol']
        df = item['df']
        f_data = fund_cache.get(sym, {})
        
        try:
            processed_data.append({
                "symbol": sym, "df": df, "fund": f_data,
                "perf_10d": (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10],
                "sector": f_data.get('sector', 'Unknown')
            })
        except: continue

    # 10. Governance & Scoring [FIX 4: Logic Restored]
    sector_map, dispersion = calculate_sector_metrics(processed_data)
    log_sector_regime(sector_map, dispersion)
    
    kill_switch = dispersion < DISPERSION_THRESHOLD
    protocol = "REDUCE_SIZE_50" if kill_switch else "NORMAL_SIZE_100"
    
    candidates, rejected = [], []
    print("💎 Ranking Stocks...")
    
    for item in processed_data:
        res = calculate_score(item['df'], item['symbol'], delivery_map, sector_map, nifty_alpha, item['fund'])
        if not res: continue
        
        if res["score"] > SCORE_THRESHOLD:
            candidates.append(res)
        else:
            rejected.append({"symbol": item['symbol'], "score": res['score'], "reason": "Low Score", "sector": item['sector']})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    
    # 11. Write Outputs
    log_audit_trail(rejected)
    write_json_contract({
        "trend": trend, "dispersion": dispersion, 
        "kill_switch": kill_switch, "protocol": protocol
    }, candidates[:20])

    print(f"✅ Run Complete. {len(candidates)} candidates found.")

if __name__ == "__main__":
    main()
