"""
cron.py - Runs once, executes the right session based on time.
Railway cron calls this at 7 AM and 8 PM daily.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime

hour = datetime.now().hour

if 6 <= hour <= 9:
    print("[CRON] Running morning session...")
    from strategy.paper_trade import run_morning_session
    trades, log = run_morning_session()
    print(f"[CRON] Morning done. {len(trades)} trades placed.")
    print(log)

elif 19 <= hour <= 22:
    print("[CRON] Running evening session...")
    from strategy.paper_trade import run_evening_session
    log = run_evening_session()
    print(f"[CRON] Evening done.")
    print(log)

else:
    print(f"[CRON] Nothing to do at hour {hour}")
