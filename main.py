import time
import datetime
import pytz
from keep_alive import keep_alive

# Import your two bots
import diamond_v17_main   # v17.1 (The Brain)
import diamond_v16_1_execution_engine  # v16.1 (The Hands)

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
IST = pytz.timezone("Asia/Kolkata")

# How often to check for trades? (Seconds)
# 900 = 15 minutes. Don't go lower than this to avoid rate limits.
EXECUTION_INTERVAL = 900 

# What time to run the full strategy scan? (24h format)
# e.g., "09:15" means 9:15 AM IST
STRATEGY_RUN_TIME = "09:15" 

# ==========================================
# 🚀 MASTER LOOP
# ==========================================
def main():
    print("💎 Diamond System: ONLINE (24/7 Mode)")
    
    # 1. Start the Web Server (Keeps Replit awake)
    keep_alive()
    
    # Track when we last ran the heavy strategy
    last_strategy_run = None

    while True:
        now = datetime.datetime.now(IST)
        current_time_str = now.strftime("%H:%M")
        
        print(f"\n⏰ Heartbeat: {current_time_str}")

        # --- TASK 1: Run Strategy (Once per day) ---
        # We run it if it matches the time AND we haven't run it today yet
        is_time_match = (current_time_str == STRATEGY_RUN_TIME)
        is_new_day = (last_strategy_run != now.date())

        if is_time_match and is_new_day:
            print("🧠 Starting Daily Strategy Scan (v17)...")
            try:
                diamond_v17_main.main()
                last_strategy_run = now.date()
                print("✅ Strategy Complete. Signal Updated.")
            except Exception as e:
                print(f"❌ Strategy Failed: {e}")

        # --- TASK 2: Run Execution (Every Interval) ---
        # Only run if market is open (approx 9:15 to 3:30)
        # Simple check: Is it between 9 AM and 4 PM?
        if 9 <= now.hour < 16 and now.weekday() < 5:
            print("⚡ Checking Live Prices (v16)...")
            try:
                diamond_v16_1_execution_engine.main()
            except Exception as e:
                print(f"❌ Execution Failed: {e}")
        else:
            print("💤 Market Closed. Sleeping...")

        # Sleep until next check
        time.sleep(EXECUTION_INTERVAL)

if __name__ == "__main__":
    main()
