import time
import datetime
import pytz
from keep_alive import keep_alive

# ==========================================================
# 🔗 IMPORT ENGINES (CURRENT NAMING)
# ==========================================================
import diamond_v17_main                 # v17.1 — Strategy / Brain
import diamond_v16_1_execution_engine   # v16.1 — Execution / Hands

# ==========================================================
# ⚙️ CONFIGURATION
# ==========================================================
IST = pytz.timezone("Asia/Kolkata")

# Execution heartbeat (seconds)
# 900 = 15 minutes (SAFE for yfinance + NSE)
EXECUTION_INTERVAL = 900  

# Daily strategy scan time (IST, 24h format)
STRATEGY_RUN_TIME = "09:15"

# ==========================================================
# 🚀 MASTER ORCHESTRATOR
# ==========================================================
def main():
    print("💎 Diamond System: ONLINE (24/7 Mode)")

    # Keep Render / Replit alive
    keep_alive()

    last_strategy_run = None

    while True:
        now = datetime.datetime.now(IST)
        current_time_str = now.strftime("%H:%M")

        print(f"\n⏰ Heartbeat: {current_time_str}")

        # ==================================================
        # 🧠 TASK 1: STRATEGY ENGINE (v17.1)
        # Runs ONCE per day, anytime AFTER 09:15 IST
        # ==================================================
        try:
            strategy_time = datetime.datetime.strptime(
                STRATEGY_RUN_TIME, "%H:%M"
            ).time()

            is_past_strategy_time = now.time() >= strategy_time
            is_new_day = (last_strategy_run != now.date())

            if is_past_strategy_time and is_new_day:
                print("🧠 Starting Daily Strategy Scan (v17.1)...")
                diamond_v17_main.main()
                last_strategy_run = now.date()
                print("✅ Strategy Complete. Signal File Updated.")
        except Exception as e:
            print(f"❌ Strategy Engine Error: {e}")

        # ==================================================
        # ⚡ TASK 2: EXECUTION ENGINE (v16.1)
        # Runs intraday during market hours
        # ==================================================
        if 9 <= now.hour < 16 and now.weekday() < 5:
            print("⚡ Checking Live Prices (v16.1)...")
            try:
                diamond_v16_1_execution_engine.main()
            except Exception as e:
                print(f"❌ Execution Engine Error: {e}")
        else:
            print("💤 Market Closed. Execution Paused.")

        # ==================================================
        # ⏳ SLEEP
        # ==================================================
        time.sleep(EXECUTION_INTERVAL)

# ==========================================================
# 🏁 ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()
