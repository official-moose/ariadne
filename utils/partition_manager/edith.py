#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250924.02
#===================================================================
# Edith - Partition Manager
# mm/utils/partition_manager/edith.py
#
# Manages hourly tickstick partitions and 6h signals_intel partitions
# Sweeps expired proposals and updates heartbeat
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

import os
import sys
import time
import signal
import atexit
from datetime import datetime, timedelta

import psycopg2
from zoneinfo import ZoneInfo

from mm.utils.helpers.wintermute import (
    init_logging,
    now_local,
    get_db_connection,
    send_email,
    update_heartbeat,
    write_pid_file,
    cleanup_pid_file
)

# ── Constants ─────────────────────────────────────────────────────────
LOG_FILE = "/root/Echelon/valentrix/mm/utils/partition_manager/edith.log"
PID_FILE = "/root/Echelon/valentrix/mm/utils/partition_manager/edith.pid"
TZ = ZoneInfo("America/Toronto")
CYCLE_INTERVAL = 600
PARTITION_CYCLES = 6
SIGNALS_SPAN = 6  # hours

# ── Logging ───────────────────────────────────────────────────────────
logger = init_logging(
    LOG_SELF=True,
    LOG_MAIN=True,
    SCREEN_OUT=True,
    LOGGER="Edith"
)

# ── Shutdown Flag ─────────────────────────────────────────────────────
shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"[SHUTDOWN] Signal {signum} received. Will exit after current cycle.")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ── PID Handling ──────────────────────────────────────────────────────
def ensure_pid():
    try:
        write_pid_file(PID_FILE)
        atexit.register(lambda: cleanup_pid_file(PID_FILE))
    except Exception as e:
        logger.error(f"[PID ERROR] {e}")
        sys.exit(1)

# ── Time Rounding ─────────────────────────────────────────────────────
def round_to_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)

# ── Partition Helpers ─────────────────────────────────────────────────
def get_existing_partitions(cur, parent):
    cur.execute(f"""
        SELECT child.relname, pg_get_expr(child.relpartbound, child.oid)
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = %s
    """, (parent,))
    return {row[0] for row in cur.fetchall()}

def drop_partitions_older_than(cur, parent, cutoff_dt):
    dropped = 0
    cur.execute(f"""
        SELECT child.relname, pg_get_expr(child.relpartbound, child.oid)
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = %s
    """, (parent,))
    for name, bound in cur.fetchall():
        try:
            to_part = int(bound.split("TO (")[1].split(")")[0])
            to_dt = datetime.fromtimestamp(to_part, tz=TZ)
            if to_dt < cutoff_dt:
                cur.execute(f"DROP TABLE IF EXISTS {name}")
                logger.info(f"[DROP] {name}")
                dropped += 1
        except Exception as e:
            logger.error(f"[ERROR] Dropping {name}: {e}")
    return dropped

# ── Tickstick ─────────────────────────────────────────────────────────
def manage_tickstick_partitions(cur):
    now = now_local(TZ)
    base = round_to_hour(now)

    targets = [base - timedelta(hours=1), base, base + timedelta(hours=1), base + timedelta(hours=2)]
    existing = get_existing_partitions(cur, "tickstick")

    for dt in targets:
        name = f"tickstick_{dt.strftime('%Y_%m_%d_%H')}"
        start = dt
        end = dt + timedelta(hours=1)
        if name not in existing:
            try:
                cur.execute(f'''
                    CREATE TABLE IF NOT EXISTS {name}
                    PARTITION OF tickstick
                    FOR VALUES FROM ({int(start.timestamp())}) TO ({int(end.timestamp())})
                ''')
                logger.info(f"[CREATE] {name}")
            except Exception as e:
                logger.error(f"[ERROR] Creating {name}: {e}")

    drop_cutoff = targets[0]
    drop_partitions_older_than(cur, "tickstick", drop_cutoff)

# ── Signals_Intel ─────────────────────────────────────────────────────
def manage_signals_partitions(cur):
    now = now_local(TZ)
    base = round_to_hour(now)
    current_span_start = base - timedelta(hours=base.hour % SIGNALS_SPAN)

    keep = [current_span_start - timedelta(hours=SIGNALS_SPAN * i) for i in range(4, -2, -1)]
    keep.reverse()

    existing = get_existing_partitions(cur, "signals_intel")

    for dt in [current_span_start, current_span_start + timedelta(hours=SIGNALS_SPAN)]:
        name = f"signals_intel_{dt.strftime('%Y_%m_%d_%H')}"
        start = dt
        end = dt + timedelta(hours=SIGNALS_SPAN)
        if name not in existing:
            try:
                cur.execute(f'''
                    CREATE TABLE IF NOT EXISTS {name}
                    PARTITION OF signals_intel
                    FOR VALUES FROM ({int(start.timestamp())}) TO ({int(end.timestamp())})
                ''')
                logger.info(f"[CREATE] {name}")
            except Exception as e:
                logger.error(f"[ERROR] Creating {name}: {e}")

    drop_cutoff = keep[0]
    drop_partitions_older_than(cur, "signals_intel", drop_cutoff)

# ── Sweeper ───────────────────────────────────────────────────────────
def sweep_expired_proposals(cur, age_minutes=10):
    try:
        cur.execute(f'''
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
        ''')
        count = cur.rowcount or 0
        if count > 0:
            logger.info(f"[SWEEP] Proposals expired: {count}")
    except Exception as e:
        logger.error(f"[ERROR] Sweep failed: {e}")

# ── Main Loop ─────────────────────────────────────────────────────────
def main():
    logger.info("="*70)
    logger.info("[INIT] EDITH starting as persistent background process")

    ensure_pid()
    conn = get_db_connection()
    cycle_count = 0

    while not shutdown_requested:
        cycle_count += 1
        start = time.time()

        try:
            cur = conn.cursor()

            sweep_expired_proposals(cur)

            if cycle_count % PARTITION_CYCLES == 1 or cycle_count == 1:
                logger.info("[PARTITION] Running tickstick maintenance...")
                manage_tickstick_partitions(cur)

                logger.info("[PARTITION] Running signals_intel maintenance...")
                manage_signals_partitions(cur)

            update_heartbeat("edith", conn)
            conn.commit()
            cur.close()

        except Exception as e:
            logger.error(f"[ERROR] Runtime: {e}")
            conn.rollback()

        elapsed = time.time() - start
        sleep_time = max(0, CYCLE_INTERVAL - elapsed)
        if sleep_time > 0 and not shutdown_requested:
            time.sleep(sleep_time)

    try:
        conn.close()
    except:
        pass
    cleanup_pid_file(PID_FILE)
    logger.info("[SHUTDOWN] Edith has exited gracefully")

if __name__ == "__main__":
    main()
