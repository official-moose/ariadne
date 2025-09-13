#>> A R I A N D E v-6.1
#>> last update: 2025 | Sept. 06
#>>
#>> Partition Manager
#>> mm/utils/partition_manager/edith.py
#>>
#>> Manages PostgreSQL partitions for the tickstick table 
#>> Creates future partitions and drops old ones
#>> Sweeps proposals table for stale records and expires them
#>> Runs hourly via cron, with heartbeat monitoring
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]              💫 PERSISTANT RUNTIME  ⏱ CRONJOB
#>>────────────────────────────────────────────────────────────────

# Build|20250906.01

import os
import sys
import time
import psycopg2
import logging
from datetime import datetime, timedelta
from pathlib import Path

from mm.utils.helpers.wintermute import update_heartbeat

# ── Constants ─────────────────────────────────────────────────────────
LOG_FILE = "/root/Echelon/valentrix/mm/utils/partition_manager/partition_manager.log"

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
def create_next_hour_partition(cur) -> bool:
    """Create partition for the next hour"""
    # Get next hour
    now = datetime.utcnow()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    hour_after = next_hour + timedelta(hours=1)
    
    start_ts = int(next_hour.timestamp())
    end_ts = int(hour_after.timestamp())
    
    partition_name = f"tickstick_{next_hour.strftime('%Y_%m_%d_%H')}"
    
    try:
        # Create the partition
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {partition_name}
            PARTITION OF tickstick
            FOR VALUES FROM ({start_ts}) TO ({end_ts})
        """)
        
        logger.info(f"[CREATE] Created partition {partition_name} for next hour")
        return True
        
    except psycopg2.errors.DuplicateTable:
        logger.debug(f"[EXISTS] Partition {partition_name} already exists")
        return False
    except Exception as e:
        logger.error(f"[ERROR] Failed to create partition {partition_name}: {e}")
        return False

def drop_old_partitions(cur) -> int:
    """Drop partitions older than 1 hour"""
    dropped_count = 0
    cutoff_time = datetime.utcnow() - timedelta(hours=1)
    
    # Query for partition information
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
            # Extract the TO timestamp from the range expression
            # Format: "FOR VALUES FROM (timestamp) TO (timestamp)"
            to_str = partition_range.split('TO (')[1].split(')')[0].strip("'")
            to_ts = int(to_str)
            to_datetime = datetime.utcfromtimestamp(to_ts)
            
            # If partition end time is before cutoff, drop it
            if to_datetime < cutoff_time:
                cur.execute(f"DROP TABLE IF EXISTS {partition_name}")
                dropped_count += 1
                logger.info(f"[DROP] Dropped old partition {partition_name}")
                
        except Exception as e:
            logger.error(f"[ERROR] Failed to parse partition {partition_name}: {e}")
    
    return dropped_count

# ── Proposals Sweeper ─────────────────────────────────────────────────
def sweep_expired_proposals(cur, age_minutes: int = 10) -> int:
    """
    Mark proposals older than `age_minutes` and still 'pending' as 'expired'.
    Adds decision_stamp = NOW() and a housekeeping note.
    Returns number of rows updated.
    """
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
        else:
            logger.info(f"[SWEEP] No proposals to expire (>{age_minutes}m)")
        return updated
    except Exception as e:
        logger.error(f"[ERROR] Proposal sweep failed: {e}")
        return 0

# ── Table Bootstrap (safety) ─────────────────────────────────────────
def ensure_table_structure(conn) -> None:
    """Ensure the tickstick table exists with proper partitioning"""
    cur = conn.cursor()
    
    try:
        # Check if table exists
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
                    vol NUMERIC(20,8),
                    vol_value NUMERIC(20,8),
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

# ── Main ─────────────────────────────────────────────────────────────
def main():
    """Main execution function"""
    logger.info("="*70)
    logger.info("[INIT] Starting Partition Manager - Hourly Run")
    
    conn = None
    
    try:
        # Establish database connection
        conn = get_db_connection()
        logger.info("[INIT] Database connection established")
        
        # Ensure table structure exists
        ensure_table_structure(conn)
        
        # Create cursor for operations
        cur = conn.cursor()
        
        # Create partition for next hour
        if create_next_hour_partition(cur):
            logger.info("[SUCCESS] Next hour partition created")
        
        # Drop partitions older than 1 hour
        dropped = drop_old_partitions(cur)
        if dropped > 0:
            logger.info(f"[CLEANUP] Dropped {dropped} old partitions")
        else:
            logger.info("[CLEANUP] No old partitions to drop")
        
        # Sweep proposals older than 10 minutes → expire
        sweep_expired_proposals(cur, age_minutes=10)

        # Commit all changes (partitions + sweeps)
        conn.commit()
        cur.close()
        
        # Update heartbeat
        update_heartbeat("edith", conn)
        
        logger.info("[COMPLETE] Partition management + proposal sweep complete")
                
    except Exception as e:
        logger.error(f"[ERROR] Failed to manage partitions: {e}", exc_info=True)
        if conn:
            conn.rollback()
        
    finally:
        if conn:
            try:
                conn.close()
                logger.info("[CLEANUP] Database connection closed")
            except:
                pass
        
        logger.info("="*70)

if __name__ == "__main__":
    main()
