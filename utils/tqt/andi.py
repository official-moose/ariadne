#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250903.01
#===================================================================
# last update: 2025 | Sept. 3                   Production ready ❌
#===================================================================
# Andi - Transactional Query Table Processor
# mm/utils/tqt/andi.py
#
# Validates and queues database writes from managers
# Batches for efficiency, handles retries, maintains data 
# integrity
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

# 🔸 Standard Library Imports ======================================

import os
import sys
import json
import signal
import time
import threading
import queue
import uuid
from typing import Dict, Any, List, Optional, Tuple, Callable
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from collections import defaultdict
import psycopg2
import psycopg2.extras

# 🔸 Add project root for imports (deployment path) ================

sys.path.append('/root/Echelon/valentrix')

from mm.utils.helpers.wintermute import (
    now_pack,
    write_pid_file,
    cleanup_pid_file,
    get_db_connection,
    release_db_connection,
    update_heartbeat
)
from mm.utils.helpers.inara import get_mode

# 🔸 Configuration =================================================

SCHEMA_CACHE_PATH = "/root/Echelon/valentrix/mm/data/source/schemas.json"
PID_FILE = "/root/Echelon/valentrix/mm/utils/tqt/andi.pid"
LOG_FILE = "/root/Echelon/valentrix/mm/utils/tqt/andi.log"
NOTES_FILE = "/root/Echelon/valentrix/mm/utils/tqt/andi_notes.log"

# 🔸 Processing parameters =========================================

BATCH_SIZE = 50           # Max items per batch (mixed table/hold/asset)
BATCH_INTERVAL = 0.10     # seconds
RETRY_MAX = 3             # Max retries per item
RETRY_DELAY = 0.50        # Initial retry delay (per item)
DLQ_MAX_SIZE = 1000       # Max failed items to retain
HEARTBEAT_INTERVAL = 300  # seconds

# 🔸 Sweeping defaults =============================================

HOLD_SWEEP_AGE_MIN = 10     # minutes for holds.sweep
ASSET_SWEEP_AGE_MIN = 10    # minutes for assets.sweep

# 🔸 Global shutdown flag ==========================================

shutdown_requested = False

# 🔹 Advanced Logger ===============================================

from mm.utils.helpers.wintermute import init_logging

logger = init_logging(
    LOG_SELF=True,
    LOG_MAIN=True,
    SCREEN_OUT=True,
    LOGGER="Julius"  
)

# === End === 

# 🔸 Signal Handlers ===============================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    log.info(f"[SHUTDOWN] Received signal {signum}, flushing queues...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# 🔸 Schema Validator ==============================================

class SchemaValidator:
    """
    Validates data against schemas from KARIN's cache.
    """

    def __init__(self):
        self.schemas: Dict[str, Any] = {}
        self.defaults: Dict[str, Dict[str, Any]] = {}
        self.last_loaded: Optional[datetime] = None
        self.load_schemas()

    def load_schemas(self):
        """Load schemas from KARIN's cache file."""
        try:
            if not Path(SCHEMA_CACHE_PATH).exists():
                log.warning(f"Schema cache not found at {SCHEMA_CACHE_PATH}")
                return False

            with open(SCHEMA_CACHE_PATH, 'r') as f:
                cache = json.load(f)

            self.schemas = cache.get('schemas', {})
            self.defaults = cache.get('defaults', {})
            self.last_loaded = datetime.now(timezone.utc)

            log.info(f"Loaded schemas for {len(self.schemas)} tables")
            return True

        except Exception as e:
            log.error(f"Failed to load schemas: {e}")
            return False

    def should_reload(self) -> bool:
        if not self.last_loaded:
            return True
        try:
            cache_mtime = Path(SCHEMA_CACHE_PATH).stat().st_mtime
            cache_modified = datetime.fromtimestamp(cache_mtime, tz=timezone.utc)
            return cache_modified > self.last_loaded
        except Exception:
            return False

    def validate(self, table: str, data: Dict) -> Tuple[bool, Optional[str], Dict]:
        """Validate data against schema. Returns (ok, err, processed)."""
        if self.should_reload():
            self.load_schemas()

        if table not in self.schemas:
            return False, f"Unknown table: {table}", {}

        schema = self.schemas[table]
        columns = schema.get('columns', {})
        processed: Dict[str, Any] = {}

        for col_name, col_info in columns.items():
            value = data.get(col_name)

            if value is None:
                if not col_info.get('nullable', True):
                    default_map = self.defaults.get(table, {})
                    if col_name in default_map:
                        value = default_map[col_name]
                        notes.info(f"Applied default for {table}.{col_name}: {value}")
                    else:
                        return False, f"Missing required field: {col_name}", {}

            if value is not None:
                dtype = str(col_info.get('type', '')).upper()
                processed[col_name] = self._coerce_type(value, dtype)
            else:
                processed[col_name] = None

        extra = set(data.keys()) - set(columns.keys())
        if extra:
            notes.warning(f"Extra fields ignored for {table}: {extra}")

        return True, None, processed

    def _coerce_type(self, value: Any, dtype: str) -> Any:
        base = dtype.split('(')[0]
        if base in ('TEXT', 'VARCHAR', 'CHAR', 'UUID'):
            return str(value)
        if base in ('INTEGER', 'INT', 'SMALLINT', 'BIGINT'):
            return int(value)
        if base in ('DECIMAL', 'NUMERIC'):
            return Decimal(str(value))
        if base in ('REAL', 'DOUBLE'):
            return float(value)
        if base == 'BOOLEAN':
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'y')
            return bool(value)
        if base in ('TIMESTAMP', 'TIMESTAMPTZ', 'DATE', 'TIME', 'JSON', 'JSONB'):
            return value  # let PG handle
        return value

# 🔸 Write Queue Manager ===========================================

class WriteQueue:
    """
    Manages queued items with batching and retry logic.
    Supports:
      • Table writes: {'table', 'data', ...}
      • Topics:
          - holds.*   (Julius)
          - assets.*  (Helen)
    """

    def __init__(self, validator: SchemaValidator):
        self.validator = validator
        self.pending_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.dead_letter_queue: List[Dict[str, Any]] = []
        self.stats = defaultdict(int)
        self.consecutive_failures = 0
        # Topic handlers
        self.hold_handler: Optional[Callable[[Dict[str, Any]], None]] = None
        self.asset_handler: Optional[Callable[[Dict[str, Any]], None]] = None

    def set_hold_handler(self, fn: Callable[[Dict[str, Any]], None]) -> None:
        self.hold_handler = fn

    def set_asset_handler(self, fn: Callable[[Dict[str, Any]], None]) -> None:
        self.asset_handler = fn

    def enqueue(self, table: str, data: Dict, source: str = None) -> str:
        """Validated INSERT into a specific table."""
        queue_id = str(uuid.uuid4())
        ok, err, processed = self.validator.validate(table, data)
        if not ok:
            self.stats['validation_failures'] += 1
            notes.error(f"Validation failed for {table}: {err}")
            self.add_to_dlq({
                'queue_id': queue_id,
                'table': table,
                'data': data,
                'error': err,
                'source': source or 'unknown',
                'timestamp': now_pack().iso
            })
            return queue_id

        self.pending_queue.put({
            'queue_id': queue_id,
            'table': table,
            'data': processed,
            'source': source or 'unknown',
            'timestamp': datetime.now(timezone.utc),
            'attempts': 0
        })
        self.stats['enqueued'] += 1
        return queue_id

    def enqueue_topic(self, topic: str, payload: Dict, source: str = None) -> str:
        """Generic topic enqueue (holds.* or assets.*)."""
        queue_id = str(uuid.uuid4())
        self.pending_queue.put({
            'queue_id': queue_id,
            'topic': topic,
            'payload': payload,
            'source': source or 'unknown',
            'timestamp': datetime.now(timezone.utc),
            'attempts': 0
        })
        self.stats['enqueued'] += 1
        return queue_id

    # Back-compat sugar
    def enqueue_hold(self, topic: str, payload: Dict, source: str = None) -> str:
        return self.enqueue_topic(topic, payload, source)

    def process_batch(self) -> int:
        """Process up to BATCH_SIZE items (tables batched; topics 1x txn each)."""
        batch: List[Dict[str, Any]] = []
        while len(batch) < BATCH_SIZE and not self.pending_queue.empty():
            try:
                batch.append(self.pending_queue.get_nowait())
            except queue.Empty:
                break

        if not batch:
            return 0

        table_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        topic_items: List[Dict[str, Any]] = []

        for item in batch:
            if 'topic' in item:
                topic_items.append(item)
            else:
                table_groups[item['table']].append(item)

        success = 0

        # Process table groups
        for table, items in table_groups.items():
            try:
                written = self._write_batch_to_db(table, items)
                success += written
                self.consecutive_failures = 0
            except Exception as e:
                log.error(f"Batch write failed for {table}: {e}")
                self._handle_batch_failure(items)

        # Process topics one-by-one
        for item in topic_items:
            try:
                topic: str = item['topic']
                if topic.startswith("holds."):
                    if not self.hold_handler:
                        raise RuntimeError("No hold handler configured")
                    self.hold_handler(item)
                elif topic.startswith("assets."):
                    if not self.asset_handler:
                        raise RuntimeError("No asset handler configured")
                    self.asset_handler(item)
                else:
                    raise ValueError(f"Unknown topic namespace: {topic}")

                success += 1
                self.consecutive_failures = 0

            except Exception as e:
                log.error(f"Topic event failed ({item.get('topic')}): {e}")
                self._handle_write_failure(item, str(e))

        return success

    def _write_batch_to_db(self, table: str, items: List[Dict[str, Any]]) -> int:
        conn = None
        ok = 0
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            for item in items:
                try:
                    cols = list(item['data'].keys())
                    vals = [item['data'][c] for c in cols]
                    placeholders = ', '.join(['%s'] * len(cols))
                    col_names = ', '.join(cols)
                    cur.execute(f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})", vals)
                    item['attempts'] += 1
                    self.stats['written'] += 1
                    self.stats[f'{table}_written'] += 1
                    ok += 1
                    notes.info(f"Written to {table}: {item['queue_id']}")
                except Exception as e:
                    log.error(f"Failed to write {item['queue_id']}: {e}")
                    self._handle_write_failure(item, str(e))
            conn.commit()
        except Exception as e:
            log.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                release_db_connection(conn)
        return ok

    def _handle_write_failure(self, item: Dict, error: str):
        item['attempts'] += 1
        if item['attempts'] < RETRY_MAX:
            self.pending_queue.put(item)
            self.stats['retries'] += 1
            notes.info(f"Re-queued {item.get('queue_id')} (attempt {item['attempts']})")
        else:
            self.add_to_dlq({**item, 'error': error, 'failed_at': now_pack().iso})

    def _handle_batch_failure(self, items: List[Dict]):
        self.consecutive_failures += 1
        for item in items:
            item['attempts'] += 1
            if item['attempts'] < RETRY_MAX:
                self.pending_queue.put(item)
            else:
                self.add_to_dlq({**item, 'error': 'Batch failure', 'failed_at': now_pack().iso})
        if self.consecutive_failures >= 10:
            self._send_failure_alert()

    def add_to_dlq(self, item: Dict):
        self.dead_letter_queue.append(item)
        self.stats['dlq_items'] += 1
        if len(self.dead_letter_queue) > DLQ_MAX_SIZE:
            self.dead_letter_queue = self.dead_letter_queue[-DLQ_MAX_SIZE:]
        notes.error(f"DLQ: {item.get('queue_id')} - {item.get('error', 'Unknown error')}")

    def _send_failure_alert(self):
        try:
            tp = now_pack()
            message = f"""Database Write Failures Detected

Time: {tp.human}
Mode: {get_mode()}
Consecutive Failures: {self.consecutive_failures}

Queue Stats:
- Pending: {self.pending_queue.qsize()}
- Dead Letter Queue: {len(self.dead_letter_queue)}
- Total Written: {self.stats.get('written', 0)}
- Total Failed: {self.stats.get('dlq_items', 0)}

Recent DLQ Entries:
"""
            for item in self.dead_letter_queue[-5:]:
                message += f"\n- {item.get('table') or item.get('topic')}: {item.get('error', 'Unknown')}"

            send_alert(
                subject="[ANDI] Write Failures Alert",
                message=message,
                process_name="andi"
            )
            self.consecutive_failures = 0
        except Exception as e:
            log.error(f"Failed to send alert: {e}")

    def get_stats(self) -> Dict:
        return {
            'pending': self.pending_queue.qsize(),
            'dlq_size': len(self.dead_letter_queue),
            'stats': dict(self.stats)
        }

# 🔸 Andi Core =====================================================

class ANDI:
    """Main ANDI process - provides queue methods and processes items."""

    def __init__(self):
        self.validator = SchemaValidator()
        self.queue = WriteQueue(self.validator)
        self.queue.set_hold_handler(self._process_hold_event)
        self.queue.set_asset_handler(self._process_asset_event)

        self.processing_thread: Optional[threading.Thread] = None
        self.running = False
        self.cycle_count = 0
        self.last_heartbeat = time.time()

    # 🔸 Table-specific queue methods ==============================

    def queue_order(self, data: Dict, source: str = None) -> str:
        return self.queue.enqueue('sim_orders', data, source or 'unknown')

    def queue_trade(self, data: Dict, source: str = None) -> str:
        return self.queue.enqueue('sim_trades', data, source or 'unknown')

    def queue_balance_update(self, data: Dict, source: str = None) -> str:
        return self.queue.enqueue('sim_balances', data, source or 'unknown')

    def queue_ticker(self, data: Dict, source: str = None) -> str:
        return self.queue.enqueue('tickstick', data, source or 'unknown')

    def queue_heartbeat(self, data: Dict, source: str = None) -> str:
        return self.queue.enqueue('heartbeats', data, source or 'unknown')

    def queue_realism(self, data: Dict, source: str = None) -> str:
        return self.queue.enqueue('realism_history', data, source or 'unknown')

    def queue_generic(self, table: str, data: Dict, source: str = None) -> str:
        return self.queue.enqueue(table, data, source or 'unknown')

    # 🔸 Topic queue methods =======================================

    def queue_hold(self, topic: str, payload: Dict, source: str = None) -> str:
        """Julius → holds.*"""
        return self.queue.enqueue_topic(topic, payload, source or 'julius')

    def queue_asset(self, topic: str, payload: Dict, source: str = None) -> str:
        """Helen → assets.*"""
        return self.queue.enqueue_topic(topic, payload, source or 'helen')

    # 🔸 Processing loop ===========================================

    def _process_loop(self):
        log.info("[PROCESSOR] Write processing thread started")

        while self.running:
            try:
                start_time = time.time()
                written = self.queue.process_batch()
                if written > 0:
                    notes.info(f"Batch processed: {written} items")
                self.cycle_count += 1
                if time.time() - self.last_heartbeat > HEARTBEAT_INTERVAL:
                    self._update_heartbeat()
                    self.last_heartbeat = time.time()
                elapsed = time.time() - start_time
                time.sleep(max(BATCH_INTERVAL - elapsed, 0.01))
            except Exception as e:
                log.error(f"Processing error: {e}")
                time.sleep(BATCH_INTERVAL)

        self._flush_queue()
        log.info("[PROCESSOR] Write processing thread stopped")

    def _flush_queue(self):
        log.info("Flushing queue...")
        flushed = 0
        while not self.queue.pending_queue.empty():
            written = self.queue.process_batch()
            flushed += written
            if written == 0:
                break
        log.info(f"Flushed {flushed} items")

    def _update_heartbeat(self):
        try:
            stats = self.queue.get_stats()
            update_heartbeat('andi')
            log.info(f"[HEARTBEAT] Cycles: {self.cycle_count}, "
                     f"Pending: {stats['pending']}, "
                     f"Written: {stats['stats'].get('written', 0)}, "
                     f"DLQ: {stats['dlq_size']}")
        except Exception as e:
            log.error(f"Heartbeat update failed: {e}")

    # 🔸 Lifecycle management ======================================

    def start(self):
        if self.running:
            return
        log.info(f"[INIT] ANDI starting in {get_mode()} mode")
        log.info(f"[INIT] Batch size: {BATCH_SIZE}, Interval: {BATCH_INTERVAL}s")
        self.running = True
        self.processing_thread = threading.Thread(target=self._process_loop, daemon=True)
        self.processing_thread.start()
        log.info("[INIT] ANDI ready to process writes")

    def stop(self):
        log.info("[SHUTDOWN] Stopping ANDI...")
        self.running = False
        if self.processing_thread:
            self.processing_thread.join(timeout=5)
        stats = self.queue.get_stats()
        log.info(f"[SHUTDOWN] Final stats: {stats}")

    def get_status(self) -> Dict:
        return {
            'running': self.running,
            'mode': get_mode(),
            'cycles': self.cycle_count,
            'queue_stats': self.queue.get_stats(),
            'validator': {
                'tables_loaded': len(self.validator.schemas),
                'last_loaded': self.validator.last_loaded.isoformat() if self.validator.last_loaded else None
            }
        }

    # 🔸 Hold event processing (Julius) ============================

    def _process_hold_event(self, item: Dict[str, Any]) -> None:
        topic = item['topic']
        payload = item['payload']
        src = item.get('source', 'julius')

        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            if topic == "holds.create":
                hold_id = payload['hold_id']
                symbol  = payload['symbol']
                side    = payload['side']
                asset   = payload['asset']
                amount  = Decimal(str(payload['amount']))
                cur.execute("""
                    INSERT INTO wallet_holds
                      (hold_id, symbol, side, asset, amount_total, amount_remaining, status, origin, reason, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,'active',%s,%s, NOW(), NOW())
                """, (hold_id, symbol, side, asset, amount, amount, payload.get('origin'), payload.get('reason')))
                cur.execute("""
                    UPDATE sim_balances
                       SET available = GREATEST(0, available - %s),
                           hold = hold + %s
                     WHERE asset = %s
                """, (amount, amount, asset))
                cur.execute("""
                    INSERT INTO wallet_hold_ledger
                      (hold_id, event_type, delta_amount, amount_remaining_after, actor, source, reason, created_at)
                    VALUES (%s,'create',%s,%s,'Julius',%s,%s, NOW())
                """, (hold_id, amount, amount, src, payload.get('reason')))

            elif topic == "holds.link":
                cur.execute("""
                    UPDATE wallet_holds
                       SET order_id=%s, updated_at=NOW()
                     WHERE hold_id=%s AND status='active' AND deleted=FALSE
                """, (payload['order_id'], payload['hold_id']))
                cur.execute("""
                    INSERT INTO wallet_hold_ledger
                      (hold_id, event_type, actor, source, order_id, reason, created_at)
                    VALUES (%s,'link','Julius',%s,%s,%s, NOW())
                """, (payload['hold_id'], src, payload['order_id'], payload.get('reason')))

            elif topic == "holds.release":
                hold_id = payload['hold_id']
                asset   = payload['asset']
                amount  = Decimal(str(payload['amount']))
                cur.execute("""
                    UPDATE wallet_holds
                       SET amount_remaining = GREATEST(0, amount_remaining - %s),
                           status = CASE WHEN amount_remaining - %s <= 0 THEN 'released' ELSE status END,
                           updated_at=NOW()
                     WHERE hold_id=%s AND deleted=FALSE
                """, (amount, amount, hold_id))
                cur.execute("""
                    UPDATE sim_balances
                       SET available = available + %s,
                           hold = GREATEST(0, hold - %s)
                     WHERE asset=%s
                """, (amount, amount, asset))
                cur.execute("""
                    INSERT INTO wallet_hold_ledger
                      (hold_id, event_type, delta_amount, actor, source, reason, created_at)
                    VALUES (%s,'release',%s,'Julius',%s,%s, NOW())
                """, (hold_id, amount, src, payload.get('reason')))

            elif topic == "holds.settle":
                hold_id    = payload['hold_id']
                order_id   = payload['order_id']
                asset_out  = payload['asset_out']
                amount_out = Decimal(str(payload['amount_out']))
                asset_in   = payload['asset_in']
                amount_in  = Decimal(str(payload['amount_in']))
                fee_paid   = Decimal(str(payload.get('fee_paid', 0)))
                # side from hold
                cur.execute("SELECT side FROM wallet_holds WHERE hold_id=%s AND deleted=FALSE LIMIT 1", (hold_id,))
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(f"holds.settle: hold_id {hold_id} not found")
                side = row['side']
                cur.execute("""
                    UPDATE wallet_holds
                       SET amount_remaining = GREATEST(0, amount_remaining - %s),
                           status = CASE WHEN amount_remaining - %s <= 0 THEN 'settled' ELSE status END,
                           updated_at=NOW()
                     WHERE hold_id=%s AND deleted=FALSE
                """, (amount_out, amount_out, hold_id))
                if side == 'buy':
                    cur.execute("""UPDATE sim_balances SET hold = GREATEST(0, hold - %s) WHERE asset=%s""",
                                (amount_out, asset_out))
                    cur.execute("""
                        INSERT INTO sim_balances (asset, available, hold)
                        VALUES (%s, %s, 0)
                        ON CONFLICT (asset) DO UPDATE
                        SET available = sim_balances.available + EXCLUDED.available
                    """, (asset_in, amount_in))
                else:
                    cur.execute("""UPDATE sim_balances SET hold = GREATEST(0, hold - %s) WHERE asset=%s""",
                                (amount_out, asset_out))
                    cur.execute("""UPDATE sim_balances SET available = available + %s WHERE asset=%s""",
                                (amount_in, asset_in))
                cur.execute("""
                    INSERT INTO wallet_hold_ledger
                      (hold_id, event_type, delta_amount, amount_remaining_after, actor, source, order_id, reason, created_at)
                    VALUES (%s,'settle',%s,NULL,'Julius',%s,%s,%s, NOW())
                """, (hold_id, amount_out, src, order_id, payload.get('reason')))

            elif topic == "holds.sweep":
                cur.execute(f"""
                    UPDATE wallet_holds
                       SET status='released', updated_at=NOW()
                     WHERE status='active'
                       AND order_id IS NULL
                       AND deleted=FALSE
                       AND created_at < NOW() - INTERVAL '{HOLD_SWEEP_AGE_MIN} minutes'
                """)
                cur.execute("""
                    INSERT INTO wallet_hold_ledger
                      (hold_id, event_type, actor, source, reason, created_at)
                    VALUES (NULL,'sweep','Julius',%s,%s, NOW())
                """, (src, payload.get('reason', 'stale sweep')))

            else:
                raise ValueError(f"Unknown hold topic: {topic}")

            conn.commit()
            cur.close()

        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                release_db_connection(conn)

    # 🔸 Asset event processing (Helen) ============================

    def _process_asset_event(self, item: Dict[str, Any]) -> None:
        """
        Execute a single asset-topic event as one transaction:
          - asset_holds (qty state)
          - asset_hold_ledger (audit)
        Helen never touches sim_balances; this is BASE qty reservation only.
        """
        topic = item['topic']
        payload = item['payload']
        src = item.get('source', 'helen')

        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            if topic == "assets.create":
                # Required: hold_id, symbol, asset, qty
                hold_id = payload['hold_id']
                symbol  = payload['symbol']
                asset   = payload['asset']
                qty     = Decimal(str(payload['qty']))
                cur.execute("""
                    INSERT INTO asset_holds
                      (hold_id, symbol, asset, qty_total, qty_remaining, status, origin, reason, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,'active',%s,%s, NOW(), NOW())
                """, (hold_id, symbol, asset, qty, qty, payload.get('origin'), payload.get('reason')))
                cur.execute("""
                    INSERT INTO asset_hold_ledger
                      (hold_id, event_type, delta_qty, qty_remaining_after, actor, source, reason, created_at)
                    VALUES (%s,'create',%s,%s,'Helen',%s,%s, NOW())
                """, (hold_id, qty, qty, src, payload.get('reason')))

            elif topic == "assets.link":
                cur.execute("""
                    UPDATE asset_holds
                       SET order_id=%s, updated_at=NOW()
                     WHERE hold_id=%s AND status='active' AND deleted=FALSE
                """, (payload['order_id'], payload['hold_id']))
                cur.execute("""
                    INSERT INTO asset_hold_ledger
                      (hold_id, event_type, actor, source, order_id, reason, created_at)
                    VALUES (%s,'link','Helen',%s,%s,%s, NOW())
                """, (payload['hold_id'], src, payload['order_id'], payload.get('reason')))

            elif topic == "assets.release":
                hold_id = payload['hold_id']
                qty     = Decimal(str(payload['qty']))
                cur.execute("""
                    UPDATE asset_holds
                       SET qty_remaining = GREATEST(0, qty_remaining - %s),
                           status = CASE WHEN qty_remaining - %s <= 0 THEN 'released' ELSE status END,
                           updated_at=NOW()
                     WHERE hold_id=%s AND deleted=FALSE
                """, (qty, qty, hold_id))
                cur.execute("""
                    INSERT INTO asset_hold_ledger
                      (hold_id, event_type, delta_qty, actor, source, reason, created_at)
                    VALUES (%s,'release',%s,'Helen',%s,%s, NOW())
                """, (hold_id, qty, src, payload.get('reason')))

            elif topic == "assets.settle":
                # Required: hold_id, order_id, asset, qty
                hold_id  = payload['hold_id']
                order_id = payload['order_id']
                qty      = Decimal(str(payload['qty']))
                cur.execute("""
                    UPDATE asset_holds
                       SET qty_remaining = GREATEST(0, qty_remaining - %s),
                           status = CASE WHEN qty_remaining - %s <= 0 THEN 'settled' ELSE status END,
                           updated_at=NOW()
                     WHERE hold_id=%s AND deleted=FALSE
                """, (qty, qty, hold_id))
                cur.execute("""
                    INSERT INTO asset_hold_ledger
                      (hold_id, event_type, delta_qty, qty_remaining_after, actor, source, order_id, reason, created_at)
                    VALUES (%s,'settle',%s,NULL,'Helen',%s,%s,%s, NOW())
                """, (hold_id, qty, src, order_id, payload.get('reason')))

            elif topic == "assets.sweep":
                # Release orphaned asset holds older than threshold (no order_id)
                cur.execute(f"""
                    UPDATE asset_holds
                       SET status='released', updated_at=NOW()
                     WHERE status='active'
                       AND order_id IS NULL
                       AND deleted=FALSE
                       AND created_at < NOW() - INTERVAL '{ASSET_SWEEP_AGE_MIN} minutes'
                """)
                cur.execute("""
                    INSERT INTO asset_hold_ledger
                      (hold_id, event_type, actor, source, reason, created_at)
                    VALUES (NULL,'sweep','Helen',%s,%s, NOW())
                """, (src, payload.get('reason', 'stale asset sweep')))

            else:
                raise ValueError(f"Unknown asset topic: {topic}")

            conn.commit()
            cur.close()

        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                release_db_connection(conn)

# 🔸 Singleton Instance ============================================

_andi_instance: Optional[ANDI] = None

def get_andi() -> ANDI:
    """Get singleton ANDI instance."""
    global _andi_instance
    if _andi_instance is None:
        _andi_instance = ANDI()
        _andi_instance.start()
    return _andi_instance

# 🔸 Main Entry Point (for standalone operation) ===================

def main():
    """Run ANDI as standalone process."""
    try:
        write_pid_file(PID_FILE)
        andi = get_andi()
        while not shutdown_requested:
            time.sleep(1)
        andi.stop()
    except KeyboardInterrupt:
        log.info("[SHUTDOWN] Interrupted by user")
    except Exception as e:
        log.error(f"[FATAL] {e}")
    finally:
        cleanup_pid_file(PID_FILE)

if __name__ == "__main__":
    main()
