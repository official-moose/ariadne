#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250918.01
#===================================================================
# last update: 2025 | Sept. 18                   Production ready ✅
#===================================================================
# Alma
# mm/utils/tickersticker/alma.py             
#
#🔺 THIS FILE IS MISSION CRITICAL 🔺
#
# Captures market data every 3 seconds and stores to db.
#
# [520] [741] [8]                   
#===================================================================
# 🔰 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

# 🔸 Standard Library Imports ======================================

import importlib
import smtplib
import ssl
import uuid 
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
from email.utils import formataddr
from zoneinfo import ZoneInfo
    
# 🔸 third-party imports ===========================================

from dotenv import load_dotenv

# 🔸 local application imports =====================================

import mm.config.marcus as marcus
from mm.utils.helpers.wintermute import send_email

# 🔸 load env for this process =====================================

load_dotenv("mm/data/secrets/.env")

# 🔸 heartbeat tracer logger =======================================

_hb_logger = logging.getLogger("alma_hb")
_hb_logger.setLevel(logging.INFO)
_log_path = "mm/utils/tickersticker/alma_heartbeats.log"
os.makedirs(os.path.dirname(_log_path), exist_ok=True)
if not _hb_logger.handlers:
    _fh = logging.FileHandler(_log_path)
    _fh.setFormatter(logging.Formatter('%(message)s'))
    _hb_logger.addHandler(_fh)

# 🔸 Global shutdown flag ==========================================

shutdown_requested = False

def signal_handler(signum, frame):
    """Handle SIGTERM and SIGINT for graceful shutdown"""
    global shutdown_requested
    print(f"[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT,  signal_handler)

# 🔸 PID support for MONIT =========================================

PID_FILE = "/root/Echelon/valentrix/mm/utils/tickersticker/alma.pid"

def _cleanup_pidfile():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

# 🔸 remove stale pid if dead ======================================

if os.path.exists(PID_FILE):
    try:
        with open(PID_FILE) as f:
            old = f.read().strip()
        if old.isdigit() and not os.path.exists(f"/proc/{old}"):
            os.remove(PID_FILE)
    except Exception:
        pass

# 🔸 write our pid =================================================

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
            
            # Bounds check for numeric overflow
            MAX_VAL = 999999999999
            skip = False
            for i, val in enumerate(row):
                if isinstance(val, (int, float)) and val > MAX_VAL:
                    print(f"[OVERFLOW SKIP] {symbol} field index {i} = {val}")
                    skip = True
                    break

            if not skip:
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
            
            #--- temp debug tracer ---
            for row in rows:
                for i, val in enumerate(row):
                    if isinstance(val, float) and abs(val) >= 1e12:
                        print(f"[DEBUG] Overflow risk → index: {i}, value: {val}")
            #--- end debug tracer ---
            
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
    
    conn = get_connection()
    
    cycle_count = 0

    try:
        while not shutdown_requested:
            start = time.time()
            start_dt = now_local()  # for tracer line

            try:
                now_ts = int(start)
                tickers = client.get_all_tickers()

                inserted = insert_rows(conn, now_ts, tickers)

                cycle_count += 1

                # Update heartbeat every 20 cycles (every minute at 3s intervals)
                hbs_str = "-"
                if cycle_count % 10 == 0:
                    update_heartbeat("alma", conn)
                    hbs_str = now_local().strftime("%H:%M:%S")

            except psycopg2.OperationalError as e:
                print(f"[DB FATAL] Database connection lost: {e}")
                try:
                    send_email(
                        subject="[ STATCON1 ] Alma executed a corrective exit.",
                        status="STATCON1",
                        title="Database Connection Error Triggering a Corrective Exit",
                        message=f"<p><b>Alma was unable to connect to the database, the reported error was:</b><br><i>{e}</i></p><p>This exit was coded in to prevent stalling, infinite loops, and other outcomes that prevent Monit from knowing Alma is stuck. Monit <b><i>should</i></b> restart Alma.</p><p>Please ensure that this is the case by logging onto the server and using the command:<br><i>sudo monit status alma</i></p>",
                        USERCODE="ALM",
                    )
                except:
                    pass  # Don't let email failure prevent exit
                sys.exit(1)
            except psycopg2.InterfaceError as e:
                print(f"[DB FATAL] Database interface error: {e}")
                try:
                    send_email(
                        subject="[ STATCON1 ] Alma executed a corrective exit.",
                        status="STATCON1",
                        title="Database Interface Error Triggering a Corrective Exit",
                        message=f"<p><b>Alma was unable to interface with the database, the reported error was:</b><br><i>{e}</i></p><p>This exit was coded in to prevent stalling, infinite loops, and other outcomes that prevent Monit from knowing Alma is stuck. Monit <b><i>should</i></b> restart Alma.</p><p>Please ensure that this is the case by logging onto the server and using the command:<br><i>sudo monit status alma</i></p>",
                        USERCODE="ALM",
                    )
                except:
                    pass
                sys.exit(1)
            except Exception as e:
                # Check for KuCoin connection errors
                error_str = str(e)
                if "SOCKSHTTPSConnectionPool" in error_str or "Max retries exceeded" in error_str:
                    print(f"[KUCOIN FATAL] API connection failed: {e}")
                    try:
                        send_email(
                            subject="[ STATCON1 ] Alma executed a corrective exit.",
                            status="STATCON1",
                            title="API Connection to KuCoin Failed",
                            message=f"<p><b>Alma was unable to fetch data from KuCoin via the API, the reported error was:</b><br><i>{e}</i></p><p>This exit was coded in to prevent stalling, infinite loops, and other outcomes that prevent Monit from knowing Alma is stuck. Monit <b><i>should</i></b> restart Alma.</p><p>Please ensure that this is the case by logging onto the server and using the command:<br><i>sudo monit status alma</i></p>",
                            USERCODE="ALM",
                        )
                    except:
                        pass
                    sys.exit(1)
                elif "connection already closed" in error_str:
                    print(f"[DB RECONNECT] Connection closed, attempting reconnect: {e}")
                    try:
                        conn.close()  # Clean up the dead connection
                    except:
                        pass

                    try:
                        conn = get_connection()  # Get new connection
                        print("[DB RECONNECT] Successfully reconnected to database")
                    except Exception as reconnect_error:
                        print(f"[DB FATAL] Reconnection failed: {reconnect_error}")
                        try:
                            send_email(
                                subject="[ STATCON1 ] Alma executed a corrective exit.",
                                status="STATCON1",
                                title="Database Connection was Closed, Reconnection Failed.",
                                message=f"<p><b>The connection to the database that Alma was using was closed. She attempted to open a new connection, but that failed as well. The reported error was:</b><br><i>{e}</i></p><p>This exit was coded in to prevent stalling, infinite loops, and other outcomes that prevent Monit from knowing Alma is stuck. Monit <b><i>should</i></b> restart Alma.</p><p>Please ensure that this is the case by logging onto the server and using the command:<br><i>sudo monit status alma</i></p>",
                                USERCODE="ALM",
                            )
                        except:
                            pass
                    sys.exit(1)
                else:
                    print(f"[LOOP ERROR] {e}")

            # Fixed 3s cycle, sleep accounts for execution time (no incremental sleep)
            elapsed = (time.time() - start)
            sleep_time = max(0.0, INTERVAL - elapsed)  # INTERVAL = 3.0s

            # Tracer line: Cycle No. | Start | Runtime | Sleep | HBS
            runtime_ms = int(round(elapsed * 1000))
            sleep_ms = int(round(sleep_time * 1000))
            
            if sleep_time > 0.0:
                time.sleep(sleep_time)

    except Exception as e:
        print(f"[FATAL ERROR] {e}")
    finally:
        _cleanup_pidfile()
        conn.close()
        
if __name__ == "__main__":
    main()
