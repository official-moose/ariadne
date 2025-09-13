#>> 🍁 A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 5                ✅ Production ready                
#>>
#>> Ticker Sticker
#>> mm/utils/tickersticker/alma.py
#>>
#>> Captures market data every 3 seconds and stores to db. 
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]        💫 PERSISTANT RUNTIME  ➰ MONIT MANAGED      
#>>────────────────────────────────────────────────────────────────

# Build|20250905.01

import time
import psycopg2
import os
import logging
import signal
import sys
import atexit
from datetime import datetime
from mm.conn.conn_kucoin import KucoinClient
from mm.utils.helpers.wintermute import update_heartbeat
from mm.utils.helpers.wintermute import now_local  # local tz helper

# heartbeat tracer logger -> mm/utils/tickersticker/alma_heartbeats.log
_hb_logger = logging.getLogger("alma_hb")
_hb_logger.setLevel(logging.INFO)
_log_path = "mm/utils/tickersticker/alma_heartbeats.log"
os.makedirs(os.path.dirname(_log_path), exist_ok=True)
if not _hb_logger.handlers:
    _fh = logging.FileHandler(_log_path)
    _fh.setFormatter(logging.Formatter('%(message)s'))
    _hb_logger.addHandler(_fh)

# Global shutdown flag
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle SIGTERM and SIGINT for graceful shutdown"""
    global shutdown_requested
    print(f"[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT,  signal_handler)

# PID support for MONIT
PID_FILE = "/root/Echelon/valentrix/mm/utils/tickersticker/alma.pid"

def _cleanup_pidfile():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

# remove stale pid if dead
if os.path.exists(PID_FILE):
    try:
        with open(PID_FILE) as f:
            old = f.read().strip()
        if old.isdigit() and not os.path.exists(f"/proc/{old}"):
            os.remove(PID_FILE)
    except Exception:
        pass

# write our pid
try:
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
except Exception as e:
    print(f"[PID ERROR] {e}", file=sys.stderr)
    sys.exit(1)

atexit.register(_cleanup_pidfile)

DB_NAME = "ariadne"
TABLE_NAME = "tickstick"
INTERVAL = 3

client = KucoinClient()

FIELDS = [
    "symbol", "symbol_name", "buy", "sell", "last", "best_bid_size", "best_ask_size",
    "change_rate", "change_price", "high", "low", "vol", "vol_value",
    "average_price", "taker_fee_rate", "maker_fee_rate", "taker_coefficient", "maker_coefficient"
]

def get_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(database=DB_NAME, user="postgres", host="localhost")
        return conn
    except psycopg2.Error as e:
        print(f"[DB ERROR] Failed to connect: {e}")
        sys.exit(1)

def insert_rows(conn, timestamp, tickers):
    """Insert ticker data into PostgreSQL"""
    
    # Get current UTC timestamp
    timestamp = int(datetime.utcnow().timestamp())

    rows = []
    for t in tickers:
        try:
            symbol = t.get("symbol", "")
            if not symbol.endswith("-USDT"):
                continue

            vol_value = float(t.get("volValue", 0.0))
            if not (2_000_000 <= vol_value < 200_000_000):
                continue

            # Map API fields to DB columns
            row = [
                timestamp,
                t.get("symbol"),
                t.get("symbolName"),
                float(t.get("buy", 0)) if t.get("buy") else None,
                float(t.get("sell", 0)) if t.get("sell") else None,
                float(t.get("last", 0)) if t.get("last") else None,
                float(t.get("bestBidSize", 0)) if t.get("bestBidSize") else None,
                float(t.get("bestAskSize", 0)) if t.get("bestAskSize") else None,
                float(t.get("changeRate", 0)) if t.get("changeRate") else None,
                float(t.get("changePrice", 0)) if t.get("changePrice") else None,
                float(t.get("high", 0)) if t.get("high") else None,
                float(t.get("low", 0)) if t.get("low") else None,
                float(t.get("vol", 0)) if t.get("vol") else None,
                float(t.get("volValue", 0)) if t.get("volValue") else None,
                float(t.get("averagePrice", 0)) if t.get("averagePrice") else None,
                float(t.get("takerFeeRate", 0)) if t.get("takerFeeRate") else None,
                float(t.get("makerFeeRate", 0)) if t.get("makerFeeRate") else None,
                float(t.get("takerCoefficient", 0)) if t.get("takerCoefficient") else None,
                float(t.get("makerCoefficient", 0)) if t.get("makerCoefficient") else None
            ]
            rows.append(tuple(row))
        except Exception as e:
            print(f"[PARSE ERROR] {t.get('symbol', '?')} → {e}")

    if rows:
        cursor = conn.cursor()
        try:
            placeholders = ",".join(["%s"] * 19)  # 19 columns total
            sql = f"""
            INSERT INTO {TABLE_NAME} 
            (timestamp, symbol, symbol_name, buy, sell, last, best_bid_size, best_ask_size,
             change_rate, change_price, high, low, vol, vol_value, average_price,
             taker_fee_rate, maker_fee_rate, taker_coefficient, maker_coefficient)
            VALUES ({placeholders})
            """
            cursor.executemany(sql, rows)
            conn.commit()
            return len(rows)
        except psycopg2.Error as e:
            print(f"[INSERT ERROR] {e}")
            conn.rollback()
            return 0
        finally:
            cursor.close()
    return 0

def main():
    """Main loop"""
    print("[INIT] Starting Ticker Sticker v5.0...")

    conn = get_connection()
    print("[INIT] PostgreSQL connected, starting loop...")

    cycle_count = 0

    try:
        while not shutdown_requested:
            start = time.time()
            start_dt = now_local()  # for tracer line

            try:
                now_ts = int(start)
                tickers = client.get_all_tickers()

                inserted = insert_rows(conn, now_ts, tickers)

                print(f"[{now_local().isoformat(timespec='seconds')}] "
                      f"Cycle complete - Inserted: {inserted} records")

                cycle_count += 1

                # Update heartbeat every 20 cycles (every minute at 3s intervals)
                hbs_str = "-"
                if cycle_count % 10 == 0:
                    update_heartbeat("alma", conn)
                    hbs_str = now_local().strftime("%H:%M:%S")

            except Exception as e:
                print(f"[LOOP ERROR] {e}")

            # Fixed 3s cycle, sleep accounts for execution time (no incremental sleep)
            elapsed = (time.time() - start)
            sleep_time = max(0.0, INTERVAL - elapsed)  # INTERVAL = 3.0s

            # Tracer line: Cycle No. | Start | Runtime | Sleep | HBS
            runtime_ms = int(round(elapsed * 1000))
            sleep_ms = int(round(sleep_time * 1000))
            _hb_logger.info(
                f"Cycle No. {cycle_count}  |  {start_dt.strftime('%Y-%m-%d %H:%M:%S')}  |  "
                f"Runtime: {runtime_ms}ms  |  Sleep Calculation: {sleep_ms}ms  | HBS: {hbs_str}"
            )

            if sleep_time > 0.0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("[SHUTDOWN] Keyboard interrupt received")
    except Exception as e:
        print(f"[FATAL ERROR] {e}")
    finally:
        _cleanup_pidfile()
        print("[SHUTDOWN] Closing database connection...")
        conn.close()
        print("[SHUTDOWN] Ticker Sticker shut down gracefully")

if __name__ == "__main__":
    main()
