#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250923.01
#===================================================================
# last update: 2025 | Sept. 23                  PRODUCTION READY ✅
#===================================================================
# Thea - Seldon Engine
# mm/utils/seldon_engine/thea.py
#
# Captures full KuCoin orderbook + meta signals every 5 minutes
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✔ PERSISTENT RUNTIME  ✔ MONIT MANAGED
#===================================================================

import os
import sys
import time
import signal
import atexit
import logging
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
import psycopg2
from psycopg2.extras import execute_values

from mm.conn.conn_kucoin import KucoinClient
from mm.utils.helpers.wintermute import update_heartbeat
import mm.config.marcus as marcus

# ── Constants ─────────────────────────────────────────────────────────
PID_FILE = "mm/config/pid/thea.pid"
LOG_FILE = "mm/logs/thea.log"
CYCLE_INTERVAL = 300  # 5 minutes
TIMEZONE = ZoneInfo("America/Toronto")

# ── Global shutdown flag ─────────────────────────────────────────────
shutdown_requested = False

# ── Signal handling ──────────────────────────────────────────────────
def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ── PID Handling ─────────────────────────────────────────────────────
def _cleanup_pidfile():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

if os.path.exists(PID_FILE):
    try:
        with open(PID_FILE) as f:
            old = f.read().strip()
        if old.isdigit() and not os.path.exists(f"/proc/{old}"):
            os.remove(PID_FILE)
    except Exception:
        pass

try:
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
except Exception as e:
    print(f"[PID ERROR] {e}", file=sys.stderr)
    sys.exit(1)

atexit.register(_cleanup_pidfile)

# ── Logging Setup ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("thea")

# ── Database Connection ───────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        dbname="ariadne",
        user="postgres",
        host="localhost"
    )

# ── Main Loop ─────────────────────────────────────────────────────────
def main():
    logger.info("="*70)
    logger.info("[INIT] THEA starting as persistent background process")

    conn = get_db_connection()
    kc = KucoinClient()

    while not shutdown_requested:
        start = time.time()
        try:
            cur = conn.cursor()
            now = datetime.now(TIMEZONE)
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

            tickers = kc.get_all_tickers()
            rows = []

            for t in tickers:
                try:
                    symbol = t.get("symbol")
                    best_bid = float(t.get("buy", 0))
                    best_ask = float(t.get("sell", 0))
                    last_price = float(t.get("last", 0))
                    spread_pct = abs(best_ask - best_bid) / best_ask * 100 if best_ask else 0.0
                    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else last_price

                    book = kc.order_book(symbol, depth=20)
                    bid_size_total = sum([b[1] for b in book.get("bids", [])])
                    ask_size_total = sum([a[1] for a in book.get("asks", [])])

                    rows.append((
                        str(uuid.uuid4()),
                        timestamp,
                        symbol,
                        "5m",
                        None, None, None, None, None,
                        None, None, None, None, None,
                        None, None,
                        bid_size_total,
                        ask_size_total,
                        mid_price,
                        spread_pct,
                        last_price,
                        best_bid,
                        best_ask,
                        None,
                        float(t.get("vol", 0)),
                        float(t.get("volValue", 0)),
                        float(t.get("changeRate", 0)),
                        None,
                        None,
                        None,
                    ))
                except Exception as e:
                    logger.warning(f"[SKIP] {symbol}: {e}")
                    continue

            insert_sql = """
                INSERT INTO signals_intel (
                    sigid, ts, symbol, interval,
                    open_15m, high_15m, low_15m, close_15m, vol_15m,
                    volval_15m, open_1h, high_1h, low_1h, close_1h,
                    vol_1h, volval_1h,
                    bid_size_total, ask_size_total, mid_price, spread_pct,
                    last_price, buy_price, sell_price,
                    average_price, vol, volval, change_rate,
                    avg_trade_price, avg_trade_size, buy_volume_ratio
                ) VALUES %s 
            """

            execute_values(cur, insert_sql, rows)
            conn.commit()
            cur.close()
            logger.info(f"[SAVE] Captured {len(rows)} signal rows @ {timestamp}")
            update_heartbeat("thea", conn)

        except Exception as e:
            logger.error(f"[ERROR] {e}")
            try:
                conn.rollback()
            except:
                pass

        elapsed = time.time() - start
        sleep_time = max(0, CYCLE_INTERVAL - elapsed)
        if sleep_time > 0 and not shutdown_requested:
            time.sleep(sleep_time)

    logger.info("[SHUTDOWN] Thea shutting down gracefully")
    _cleanup_pidfile()
    try:
        conn.close()
    except:
        pass

if __name__ == "__main__":
    main()
