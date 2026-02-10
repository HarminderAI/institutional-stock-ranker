# ==========================================================
# 🏛️ DIAMOND v16.3 — EXECUTION ENGINE (PRODUCTION)
# 🏆 STATUS: STEALTH | HEALTH-PROBED | ROBUST
# ==========================================================

import os, json, datetime, sys, time, random
import pytz, requests
import pandas as pd  # <--- [FIX 1] Critical Import Added
import pandas_ta as ta
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================================
# ⚙️ CONFIGURATION
# ==========================================================
IST = pytz.timezone("Asia/Kolkata")
SIGNAL_FILE = "diamond_signal.json"

# [DOCS] Execution sanity only; Deep analysis guaranteed upstream (v17).
MIN_BARS_REQUIRED = 50 
HEALTH_PROBE_SYM  = "SBIN" # Liquid stock to test connection

# Secrets
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN")
CHAT_ID         = os.getenv("CHAT_ID")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_JSON_RAW = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# [STEALTH] Clean Desktop Agents Only (No Mobile/Bot-like strings)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
]

# ==========================================================
# 🛠️ UTILITIES
# ==========================================================
def ist_now(): return datetime.datetime.now(IST)

def generate_run_id():
    """Generates a unique Trace ID for this execution instance."""
    return ist_now().strftime("%Y%m%d_%H%M")

def send_msg(text):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(f"\n📢 [Telegram]\n{text}\n")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "HTML"}, timeout=10)
    except: pass

def check_market_health():
    """
    [DEFENSE] The Health Probe.
    Checks connection to Yahoo Finance before launching the full run.
    If this fails, WE DO NOT RUN. Silence is safer than persistence.
    """
    print(f"🩺 Probing Market Health ({HEALTH_PROBE_SYM}.NS)...")
    try:
        # Single thread, minimal data request
        df = yf.download(f"{HEALTH_PROBE_SYM}.NS", period="5d", progress=False, threads=False)
        if df.empty or len(df) < 2:
            raise Exception("Empty Data Received")
        print("✅ Connection Healthy. Proceeding.")
        return True
    except Exception as e:
        print(f"💀 HEALTH CHECK FAILED: {e}")
        print("🛑 SYSTEM ABORT: Staying silent to avoid ban escalation.")
        return False

def check_sheets_idempotency(rows_to_add, worksheet):
    """
    Prevents duplicates within the same Run ID.
    Key: Date | Symbol | RunID
    """
    try:
        existing_data = worksheet.get_all_values()
        existing_set = set()
        
        # Skip header row
        for row in existing_data[1:]:
            if len(row) >= 3: # [FIX 4] Robust length check
                # Key: "YYYY-MM-DD|SYMBOL|RUN_ID" (RunID is always last)
                key = f"{row[0]}|{row[1]}|{row[-1]}"
                existing_set.add(key)
        
        unique_rows = []
        for row in rows_to_add:
            key = f"{row[0]}|{row[1]}|{row[-1]}"
            if key not in existing_set:
                unique_rows.append(row)
            else:
                print(f"🔄 Skipping Duplicate: {row[1]}")
                
        return unique_rows
    except Exception as e:
        print(f"⚠️ Idempotency Check Failed: {e}. Appending all.")
        return rows_to_add

# ==========================================================
# 📜 SIGNAL INGESTION
# ==========================================================
def load_signal():
    if not os.path.exists(SIGNAL_FILE):
        print("❌ v17 Signal File not found.")
        return None, None
        
    try:
        with open(SIGNAL_FILE, "r") as f: data = json.load(f)
        meta = data.get("meta", {})
        universe = data.get("universe", [])
        
        # Protocol Alignment
        protocol = meta.get("protocol") or meta.get("recommendation", "NORMAL_SIZE_100")
        meta["protocol"] = protocol 

        # Schema Validation
        valid_universe = []
        required_keys = {"symbol", "score"}
        
        for u in universe:
            if not required_keys.issubset(u.keys()): continue
            valid_universe.append(u)
        
        # Pre-Sort Universe by Score (Optimization)
        valid_universe.sort(key=lambda x: x["score"], reverse=True)
            
        return meta, valid_universe
        
    except Exception as e:
        print(f"❌ Corrupt Signal File: {e}")
        return None, None

# ==========================================================
# 🧠 EXECUTION REFINEMENT
# ==========================================================
def refine_trade(df):
    if len(df) < MIN_BARS_REQUIRED: return None

    close, high, low = df["Close"], df["High"], df["Low"]
    live_price = float(close.iloc[-1])
    
    try:
        ema50 = ta.ema(close, 50).iloc[-1]
        atr = ta.atr(high, low, close, 14).iloc[-1]
    except: return None

    if pd.isna(atr) or atr <= 0: return None

    # Trend Sanity Check
    if live_price < ema50: return None

    sl = round(live_price - (2.0 * atr), 1)
    tgt = round(live_price + (3.5 * atr), 1)

    return live_price, sl, tgt

# ==========================================================
# 🚀 MAIN EXECUTION
# ==========================================================
def main():
    run_id = generate_run_id()
    print(f"💎 Diamond v16.3 (Run ID: {run_id}) Initiating...")

    # 1. [DEFENSE] Health Probe
    # If this fails, we return immediately. No retries.
    if not check_market_health():
        return

    # 2. Load Contract
    meta, universe = load_signal()
    if not meta or not universe: return

    kill_switch = meta.get("kill_switch", False)
    protocol = meta.get("protocol", "NORMAL")
    
    # 3. Throttling Logic
    max_display = 2 if kill_switch else 5
    
    print(f"📥 Loaded: {len(universe)} candidates | Protocol: {protocol}")
    if kill_switch: print("⚠️ KILL SWITCH ACTIVE: High Quality Throttling Enabled.")

    # 4. Fetch Live Prices (Safe Mode)
    symbols = [u["symbol"] for u in universe]
    print("⏳ Fetching Live Prices (Stealth Mode)...")
    
    tickers = [f"{s}.NS" for s in symbols]
    
    # [DEFENSE] Concurrency Cap
    # threads=2 (instead of True/Max) drastically reduces ban risk.
    try:
        hist = yf.download(tickers, period="1y", group_by="ticker", threads=2, progress=False)
    except Exception as e:
        print(f"❌ Bulk Download Failed: {e}")
        return
    
    executable_setups = []
    sheet_rows = []

    for u in universe:
        sym = u["symbol"]
        
        try:
            # Safe Data Extraction
            if isinstance(hist.columns, pd.MultiIndex):
                # [FIX 3] Log missing symbols
                if f"{sym}.NS" not in hist.columns.levels[0]: 
                    print(f"⚠️ Missing Data: {sym}.NS (Dropped by Yahoo)")
                    continue
                df = hist[f"{sym}.NS"].copy()
            else:
                if len(symbols) == 1: df = hist.copy()
                else: continue 

            df = df.dropna()
            if df.empty: continue
            df.columns = [c.capitalize() for c in df.columns]

            # Refine
            refined = refine_trade(df)
            if not refined: continue
            
            live_price, sl, tgt = refined

            # Prepare Output
            setup = {
                "symbol": sym, "score": u["score"], "price": live_price,
                "sl": sl, "tgt": tgt, "sector": u.get("sector", "Unknown"),
                "protocol": protocol
            }
            executable_setups.append(setup)

            # Prepare Sheet Row (With Run ID)
            sheet_rows.append([
                ist_now().strftime("%Y-%m-%d"), 
                sym, u["score"], live_price, sl, tgt, 
                u.get("sector", "Unknown"), u.get("del_pct", 0), 
                protocol, run_id 
            ])

        except Exception: continue

    # ======================================================
    # 📢 REPORTING
    # ======================================================
    if not executable_setups:
        print("⚠️ No setups passed Live Refinement.")
        return
    
    # [DEFENSIVE] Re-sort to ensure best setups survive refinement
    executable_setups.sort(key=lambda x: x["score"], reverse=True)
    
    msg = [
        f"💎 <b>Diamond v16.3 Execution</b>",
        f"🆔 Run: <code>{run_id}</code>",
        f"📅 {ist_now().strftime('%d-%b %H:%M')} | 🚦 {kill_switch}",
        f"⚖️ Protocol: <b>{protocol}</b>",
        ""
    ]

    # Display Throttled List
    for r in executable_setups[:max_display]:
        icon = "🚀" if r["score"] > 85 else "✅"
        msg.append(
            f"{icon} <b>{r['symbol']}</b> ({r['score']})\n"
            f"   💰 {round(r['price'], 2)} | 🏗️ {r['sector']}\n"
            f"   🎯 {round(r['tgt'], 2)} | 🛑 {round(r['sl'], 2)}"
        )
    
    if len(executable_setups) > max_display:
        msg.append(f"\n<i>...and {len(executable_setups) - max_display} more (Hidden by Protocol)</i>")

    final_msg = "\n".join(msg)
    print(final_msg.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""))
    send_msg(final_msg)

    # ======================================================
    # 📊 GOOGLE SHEETS PUSH
    # ======================================================
    if GOOGLE_JSON_RAW and GOOGLE_SHEET_ID:
        try:
            creds = Credentials.from_service_account_info(
                json.loads(GOOGLE_JSON_RAW),
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            client = gspread.authorize(creds)
            ws = client.open_by_key(GOOGLE_SHEET_ID).worksheet("history")
            
            unique_rows = check_sheets_idempotency(sheet_rows, ws)
            
            if unique_rows:
                ws.append_rows(unique_rows)
                print(f"✅ Pushed {len(unique_rows)} unique rows to Sheets (Run {run_id}).")
            else:
                print("✅ No new rows to push (Duplicates skipped).")
                
        except Exception as e:
            print(f"⚠️ Sheets Error: {e}")

if __name__ == "__main__":
    main()
