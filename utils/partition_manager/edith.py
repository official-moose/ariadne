#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250914.01
#===================================================================
# last update: 2025 | Sept. 14                  Production ready ✅
#===================================================================
# Edith - Partition Manager
# mm/utils/partition_manager/edith.py
#
# Manages hourly partitions and sweeps expired proposals
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

import os
import sys
import time
import signal
import atexit
import psycopg2
import logging
import smtplib
import ssl
import uuid
import importlib
from datetime import datetime, timedelta
from pathlib import Path
from email.utils import formataddr
from zoneinfo import ZoneInfo

from mm.utils.helpers.wintermute import update_heartbeat
import mm.config.marcus as marcus
from mm.utils.helpers.wintermute import send_email

# ── Constants ─────────────────────────────────────────────────────────
LOG_FILE = "/root/Echelon/valentrix/mm/utils/partition_manager/edith.log"
PID_FILE = "/root/Echelon/valentrix/mm/utils/partition_manager/edith.pid"
CYCLE_INTERVAL = 600  # 10 minutes in seconds
PARTITION_CYCLES = 6  # Run partitioning every 6 cycles (1 hour)
FUTURE_HOURS = 3  # Create partitions for next 3 hours

# ── Global shutdown flag ─────────────────────────────────────────────
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle SIGTERM and SIGINT for graceful shutdown"""
    global shutdown_requested
    logger.info(f"[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ── PID Management ────────────────────────────────────────────────────
def _cleanup_pidfile():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

# Remove stale PID if dead
if os.path.exists(PID_FILE):
    try:
        with open(PID_FILE) as f:
            old = f.read().strip()
        if old.isdigit() and not os.path.exists(f"/proc/{old}"):
            os.remove(PID_FILE)
    except Exception:
        pass

# Write our PID
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
logger = logging.getLogger(__name__)

# ── Database Connection ───────────────────────────────────────────────
def get_db_connection():
    """Create and return a PostgreSQL connection"""
    return psycopg2.connect(
        dbname="ariadne",
        user="postgres",
        host="localhost"
    )

# ── Partition Management Functions ────────────────────────────────────
def create_future_partitions(cur, hours_ahead: int = 3) -> int:
    """Create partitions for the next N hours"""
    created_count = 0
    now = datetime.utcnow()
    
    for h in range(1, hours_ahead + 1):
        target_hour = (now + timedelta(hours=h)).replace(minute=0, second=0, microsecond=0)
        hour_after = target_hour + timedelta(hours=1)
        
        start_ts = int(target_hour.timestamp())
        end_ts = int(hour_after.timestamp())
        
        partition_name = f"tickstick_{target_hour.strftime('%Y_%m_%d_%H')}"
        
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF tickstick
                FOR VALUES FROM ({start_ts}) TO ({end_ts})
            """)
            created_count += 1
            logger.info(f"[CREATE] Created partition {partition_name}")
            
        except psycopg2.errors.DuplicateTable:
            logger.debug(f"[EXISTS] Partition {partition_name} already exists")
        except Exception as e:
            logger.error(f"[ERROR] Failed to create partition {partition_name}: {e}")
    
    return created_count

def drop_old_partitions(cur) -> int:
    """Drop partitions older than 1 hour"""
    dropped_count = 0
    cutoff_time = datetime.utcnow() - timedelta(hours=1)
    
    cur.execute("""
        SELECT 
            child.relname AS partition_name,
            pg_get_expr(child.relpartbound, child.oid) AS partition_range
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = 'tickstick'
        ORDER BY child.relname
    """)
    
    partitions = cur.fetchall()
    
    for partition_name, partition_range in partitions:
        try:
            to_str = partition_range.split('TO (')[1].split(')')[0].strip("'")
            to_ts = int(to_str)
            to_datetime = datetime.utcfromtimestamp(to_ts)
            
            if to_datetime < cutoff_time:
                cur.execute(f"DROP TABLE IF EXISTS {partition_name}")
                dropped_count += 1
                logger.info(f"[DROP] Dropped old partition {partition_name}")
                
        except Exception as e:
            logger.error(f"[ERROR] Failed to parse partition {partition_name}: {e}")
    
    return dropped_count

# ── Proposals Sweeper ─────────────────────────────────────────────────
def sweep_expired_proposals(cur, age_minutes: int = 10) -> int:
    """Mark proposals older than age_minutes and still pending as expired"""
    try:
        cur.execute(f"""
            UPDATE proposals
               SET status = 'expired',
                   decision_stamp = NOW(),
                   decision_notes = COALESCE(decision_notes, '') || 
                                    CASE WHEN decision_notes IS NULL OR decision_notes = '' 
                                         THEN 'auto-expired by housekeeping' 
                                         ELSE ' | auto-expired by housekeeping' 
                                    END
             WHERE status = 'pending'
               AND created_at < NOW() - INTERVAL '{age_minutes} minutes'
        """)
        updated = cur.rowcount or 0
        if updated > 0:
            logger.info(f"[SWEEP] Proposals expired (>{age_minutes}m): {updated}")
        return updated
    except Exception as e:
        logger.error(f"[ERROR] Proposal sweep failed: {e}")
        return 0

# ── Table Bootstrap ───────────────────────────────────────────────────
def ensure_table_structure(conn) -> None:
    """Ensure the tickstick table exists with proper partitioning"""
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename = 'tickstick'
            )
        """)
        
        if not cur.fetchone()[0]:
            logger.info("[INIT] Creating partitioned tickstick table...")
            cur.execute("""
                CREATE TABLE tickstick (
                    id SERIAL,
                    timestamp BIGINT NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT,
                    buy NUMERIC(20,8),
                    sell NUMERIC(20,8),
                    last NUMERIC(20,8),
                    best_bid_size NUMERIC(20,8),
                    best_ask_size NUMERIC(20,8),
                    change_rate NUMERIC(10,6),
                    change_price NUMERIC(20,8),
                    high NUMERIC(20,8),
                    low NUMERIC(20,8),
                    vol NUMERIC(24,8),
                    vol_value NUMERIC(24,8),
                    average_price NUMERIC(20,8),
                    taker_fee_rate NUMERIC(10,8),
                    maker_fee_rate NUMERIC(10,8),
                    taker_coefficient NUMERIC(10,8),
                    maker_coefficient NUMERIC(10,8)
                ) PARTITION BY RANGE (timestamp);
                
                CREATE INDEX idx_tickstick_latest ON tickstick(symbol, timestamp DESC);
            """)
            conn.commit()
            logger.info("[INIT] Created partitioned tickstick table")
            
    except Exception as e:
        logger.error(f"[ERROR] Failed to ensure table structure: {e}")
        conn.rollback()
    finally:
        cur.close()

# ── Main Loop ─────────────────────────────────────────────────────────
def main():
    """Main execution loop"""
    logger.info("="*70)
    logger.info("[INIT] EDITH starting as persistent background process")
    
    conn = None
    cycle_count = 0
    
    try:
        # Initial connection
        conn = get_db_connection()
        logger.info("[INIT] Database connection established")
        
        # Ensure table exists
        ensure_table_structure(conn)
        
        # Main loop
        while not shutdown_requested:
            start_time = time.time()
            cycle_count += 1
            
            try:
                cur = conn.cursor()
                
                # Sweep proposals every cycle (10 minutes)
                sweep_expired_proposals(cur, age_minutes=10)
                
                # Partition management every 6 cycles (1 hour)
                if cycle_count % PARTITION_CYCLES == 1 or cycle_count == 1:
                    logger.info(f"[PARTITION] Running partition management (cycle {cycle_count})")
                    
                    # Create future partitions
                    created = create_future_partitions(cur, hours_ahead=FUTURE_HOURS)
                    if created > 0:
                        logger.info(f"[PARTITION] Created {created} new partitions")
                    
                    # Drop old partitions
                    dropped = drop_old_partitions(cur)
                    if dropped > 0:
                        logger.info(f"[PARTITION] Dropped {dropped} old partitions")
                
                # Commit changes
                conn.commit()
                cur.close()
                
                # Update heartbeat every cycle
                update_heartbeat("edith", conn)
                
            except psycopg2.OperationalError as e:
                logger.error(f"[DB FATAL] Database connection lost: {e}")
                try:
                    send_email(
                        subject="[ STATCON1 ] Edith executed a corrective exit",
                        status="STATCON1",
                        title="Database Connection Error Triggering Exit",
                        message=f"<p><b>Edith lost database connection:</b><br><i>{e}</i></p><p>Monit should restart Edith.</p>",
                        USERCODE="EDI",
                    )
                except:
                    pass
                sys.exit(1)
                
            except psycopg2.InterfaceError as e:
                logger.error(f"[DB FATAL] Database interface error: {e}")
                try:
                    send_email(
                        subject="[ STATCON1 ] Edith executed a corrective exit",
                        status="STATCON1",
                        title="Database Interface Error Triggering Exit",
                        message=f"<p><b>Database interface error:</b><br><i>{e}</i></p><p>Monit should restart Edith.</p>",
                        USERCODE="EDI",
                    )
                except:
                    pass
                sys.exit(1)
                
            except Exception as e:
                error_str = str(e)
                if "connection already closed" in error_str:
                    logger.info("[DB RECONNECT] Connection closed, attempting reconnect")
                    try:
                        conn.close()
                    except:
                        pass
                    
                    try:
                        conn = get_db_connection()
                        logger.info("[DB RECONNECT] Successfully reconnected")
                    except Exception as reconnect_error:
                        logger.error(f"[DB FATAL] Reconnection failed: {reconnect_error}")
                        try:
                            send_email(
                                subject="[ STATCON1 ] Edith executed a corrective exit",
                                status="STATCON1",
                                title="Database Reconnection Failed",
                                message=f"<p><b>Failed to reconnect after connection loss:</b><br><i>{reconnect_error}</i></p>",
                                USERCODE="EDI",
                            )
                        except:
                            pass
                        sys.exit(1)
                else:
                    logger.error(f"[ERROR] Cycle error: {e}")
            
            # Calculate sleep time for accurate 10-minute cycles
            elapsed = time.time() - start_time
            sleep_time = max(0, CYCLE_INTERVAL - elapsed)
            
            if sleep_time > 0 and not shutdown_requested:
                logger.debug(f"[CYCLE] Completed in {elapsed:.2f}s, sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
        
        logger.info("[SHUTDOWN] Edith shutting down gracefully")
        
    except Exception as e:
        logger.error(f"[FATAL] {e}")
        sys.exit(1)
    finally:
        _cleanup_pidfile()
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    main()