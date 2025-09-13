#>> A R I A N D E v6
#>> last update: 2025 | Sept. 03
#>>
#>> KARIN (Kinetic Automated Relay Interface Node)
#>> mm/utils/tqt/karin.py
#>>
#>> Schema discovery and monitoring for Andi's validation
#>> Discovers and monitors ALL database tables
#>> Updates cache for Andi, alerts on changes
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250903.01
 
import os
import sys
import json
import signal
import time
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.append('/root/Echelon/valentrix')

from mm.utils.helpers.wintermute import (
    get_logger,
    now_pack,
    write_pid_file,
    cleanup_pid_file,
    get_db_connection,
    release_db_connection,
    EmailClient
)
from mm.utils.helpers.inara import get_mode
from mm.config.marcus import ALERT_EMAIL_RECIPIENT

# ──────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────

SCHEMA_CACHE_PATH = "/root/Echelon/valentrix/mm/data/source/schemas.json"
PID_FILE = "/root/Echelon/valentrix/mm/utils/tqt/karin.pid"
LOG_FILE = "/root/Echelon/valentrix/mm/utils/tqt/karin.log"
CHECK_INTERVAL = 30  # seconds

# Default values for nullable->not null transitions
DEFAULT_VALUES = {
    'TEXT': "''",
    'VARCHAR': "''",
    'CHARACTER VARYING': "''",
    'CHAR': "''",
    'CHARACTER': "''",
    'INTEGER': "0",
    'INT': "0",
    'SMALLINT': "0",
    'BIGINT': "0",
    'DECIMAL': "0.0",
    'NUMERIC': "0.0",
    'REAL': "0.0",
    'DOUBLE PRECISION': "0.0",
    'BOOLEAN': "false",
    'BOOL': "false",
    'TIMESTAMP': "NOW()",
    'TIMESTAMPTZ': "NOW()",
    'TIMESTAMP WITHOUT TIME ZONE': "NOW()",
    'TIMESTAMP WITH TIME ZONE': "NOW()",
    'DATE': "CURRENT_DATE",
    'TIME': "CURRENT_TIME",
    'JSONB': "'{}'::jsonb",
    'JSON': "'{}'::json",
    'UUID': "gen_random_uuid()",
    'BYTEA': "''::bytea",
    'ARRAY': "'{}'",
    'INTERVAL': "'0 seconds'::interval"
}

# Global shutdown flag
shutdown_requested = False

# Logger
log = get_logger("karin", LOG_FILE)

# Email client
mailer = EmailClient('karin')

# ──────────────────────────────────────────────────────────────────
# Signal Handlers
# ──────────────────────────────────────────────────────────────────

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    log.info(f"[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ──────────────────────────────────────────────────────────────────
# Schema Discovery
# ──────────────────────────────────────────────────────────────────

class SchemaDiscovery:
    """Discovers and compares ALL database schemas."""
    
    def __init__(self):
        self.conn = None
        self.last_schemas = {}
        self.load_cache()
    
    def load_cache(self):
        """Load cached schemas from JSON file."""
        try:
            if Path(SCHEMA_CACHE_PATH).exists():
                with open(SCHEMA_CACHE_PATH, 'r') as f:
                    cache = json.load(f)
                    self.last_schemas = cache.get('schemas', {})
                    log.info(f"Loaded schema cache with {len(self.last_schemas)} tables")
        except Exception as e:
            log.warning(f"Could not load schema cache: {e}")
            self.last_schemas = {}
    
    def save_cache(self, schemas: Dict):
        """Save schemas to JSON cache file."""
        try:
            # Ensure directory exists
            Path(SCHEMA_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
            
            tp = now_pack()
            cache = {
                'version': '1.0',
                'updated_at': tp.iso,
                'updated_epoch_ms': tp.epoch_ms,
                'mode': get_mode(),
                'table_count': len(schemas),
                'schemas': schemas,
                'defaults': self._generate_defaults(schemas)
            }
            
            # Write atomically (write to temp, then rename)
            temp_path = f"{SCHEMA_CACHE_PATH}.tmp"
            with open(temp_path, 'w') as f:
                json.dump(cache, f, indent=2, default=str)
            
            # Atomic rename
            os.rename(temp_path, SCHEMA_CACHE_PATH)
            log.info(f"Schema cache updated with {len(schemas)} tables")
            
        except Exception as e:
            log.error(f"Failed to save schema cache: {e}")
    
    def get_all_tables(self) -> List[str]:
        """Get ALL tables from the database (excluding system tables)."""
        try:
            cur = self.conn.cursor()
            
            # Get all user tables (excluding PostgreSQL system schemas)
            cur.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            
            tables = [row[0] for row in cur.fetchall()]
            cur.close()
            
            log.info(f"Found {len(tables)} tables in database")
            return tables
            
        except Exception as e:
            log.error(f"Failed to get table list: {e}")
            return []
    
    def discover_table_schema(self, table_name: str) -> Dict:
        """Discover schema for a single table."""
        try:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Get column information
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable,
                    column_default,
                    udt_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = {}
            for row in cur.fetchall():
                col_name = row['column_name']
                
                # Build type string (use udt_name for more precise type info)
                dtype = row['udt_name'].upper() if row['udt_name'] else row['data_type'].upper()
                
                # Add precision/length info if available
                if row['character_maximum_length']:
                    dtype = f"{dtype}({row['character_maximum_length']})"
                elif row['numeric_precision'] and row['numeric_scale']:
                    dtype = f"{dtype}({row['numeric_precision']},{row['numeric_scale']})"
                elif row['numeric_precision']:
                    dtype = f"{dtype}({row['numeric_precision']})"
                
                columns[col_name] = {
                    'type': dtype,
                    'base_type': row['data_type'].upper(),
                    'nullable': row['is_nullable'] == 'YES',
                    'default': row['column_default']
                }
            
            # Get primary key
            cur.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (table_name,))
            
            pk_columns = [row[0] for row in cur.fetchall()]
            
            # Get foreign keys
            cur.execute("""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name = %s
            """, (table_name,))
            
            foreign_keys = {}
            for row in cur.fetchall():
                foreign_keys[row['column_name']] = {
                    'references': f"{row['foreign_table']}.{row['foreign_column']}"
                }
            
            # Get indexes
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s
                AND schemaname = 'public'
            """, (table_name,))
            
            indexes = {row[0]: row[1] for row in cur.fetchall()}
            
            # Get constraints
            cur.execute("""
                SELECT conname, pg_get_constraintdef(c.oid)
                FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
                JOIN pg_class cl ON cl.oid = c.conrelid
                WHERE cl.relname = %s
                AND n.nspname = 'public'
            """, (table_name,))
            
            constraints = {row[0]: row[1] for row in cur.fetchall()}
            
            cur.close()
            
            return {
                'columns': columns,
                'primary_key': pk_columns,
                'foreign_keys': foreign_keys,
                'indexes': indexes,
                'constraints': constraints
            }
            
        except Exception as e:
            log.error(f"Failed to discover schema for {table_name}: {e}")
            return {}
    
    def discover_all_schemas(self) -> Dict:
        """Discover schemas for ALL tables in the database."""
        schemas = {}
        
        try:
            self.conn = get_db_connection()
            
            # Get ALL tables
            tables = self.get_all_tables()
            
            # Discover schema for each table
            for table in tables:
                schema = self.discover_table_schema(table)
                if schema:
                    schemas[table] = schema
                    log.debug(f"Discovered schema for {table}: {len(schema['columns'])} columns")
            
            log.info(f"Discovered schemas for {len(schemas)} tables")
            
        except Exception as e:
            log.error(f"Failed to discover schemas: {e}")
        finally:
            if self.conn:
                release_db_connection(self.conn)
                self.conn = None
        
        return schemas
    
    def compare_schemas(self, old: Dict, new: Dict) -> List[Dict]:
        """Compare two schema dictionaries and return changes."""
        changes = []
        
        # Check for new tables
        for table in new:
            if table not in old:
                changes.append({
                    'type': 'new_table',
                    'table': table,
                    'details': f"New table with {len(new[table]['columns'])} columns"
                })
        
        # Check for removed tables
        for table in old:
            if table not in new:
                changes.append({
                    'type': 'removed_table',
                    'table': table,
                    'details': f"Table removed"
                })
        
        # Check for changes in existing tables
        for table in set(old.keys()) & set(new.keys()):
            old_cols = old[table]['columns']
            new_cols = new[table]['columns']
            
            # New columns
            for col in new_cols:
                if col not in old_cols:
                    changes.append({
                        'type': 'new_column',
                        'table': table,
                        'column': col,
                        'details': f"New column: {new_cols[col]}"
                    })
            
            # Removed columns
            for col in old_cols:
                if col not in new_cols:
                    changes.append({
                        'type': 'removed_column',
                        'table': table,
                        'column': col,
                        'details': f"Column removed"
                    })
            
            # Modified columns
            for col in set(old_cols.keys()) & set(new_cols.keys()):
                old_def = old_cols[col]
                new_def = new_cols[col]
                
                # Type change
                if old_def['type'] != new_def['type']:
                    changes.append({
                        'type': 'type_change',
                        'table': table,
                        'column': col,
                        'details': f"Type changed from {old_def['type']} to {new_def['type']}"
                    })
                
                # Nullable change
                if old_def['nullable'] != new_def['nullable']:
                    if old_def['nullable'] and not new_def['nullable']:
                        changes.append({
                            'type': 'nullable_to_not_null',
                            'table': table,
                            'column': col,
                            'details': f"Column now NOT NULL",
                            'needs_default': True
                        })
                    else:
                        changes.append({
                            'type': 'not_null_to_nullable',
                            'table': table,
                            'column': col,
                            'details': f"Column now NULLABLE"
                        })
                
                # Default change
                if old_def.get('default') != new_def.get('default'):
                    changes.append({
                        'type': 'default_change',
                        'table': table,
                        'column': col,
                        'details': f"Default changed from {old_def.get('default')} to {new_def.get('default')}"
                    })
            
            # Check primary key changes
            if old[table]['primary_key'] != new[table]['primary_key']:
                changes.append({
                    'type': 'primary_key_change',
                    'table': table,
                    'details': f"Primary key changed from {old[table]['primary_key']} to {new[table]['primary_key']}"
                })
            
            # Check foreign key changes
            if old[table]['foreign_keys'] != new[table]['foreign_keys']:
                changes.append({
                    'type': 'foreign_key_change',
                    'table': table,
                    'details': f"Foreign key constraints changed"
                })
            
            # Check index changes
            old_indexes = set(old[table].get('indexes', {}).keys())
            new_indexes = set(new[table].get('indexes', {}).keys())
            
            for idx in new_indexes - old_indexes:
                changes.append({
                    'type': 'new_index',
                    'table': table,
                    'index': idx,
                    'details': f"New index created: {idx}"
                })
            
            for idx in old_indexes - new_indexes:
                changes.append({
                    'type': 'removed_index',
                    'table': table,
                    'index': idx,
                    'details': f"Index removed: {idx}"
                })
        
        return changes
    
    def _generate_defaults(self, schemas: Dict) -> Dict:
        """Generate default values for stop-gap handling."""
        defaults = {}
        
        for table, schema in schemas.items():
            table_defaults = {}
            
            for col, info in schema['columns'].items():
                if not info['nullable']:
                    # Use column default if available
                    if info.get('default'):
                        table_defaults[col] = info['default']
                    else:
                        # Map PostgreSQL types to default values
                        base_type = info.get('base_type', info['type']).split('(')[0].upper()
                        if base_type in DEFAULT_VALUES:
                            table_defaults[col] = DEFAULT_VALUES[base_type]
                        else:
                            # Generic default
                            table_defaults[col] = "NULL"
            
            if table_defaults:
                defaults[table] = table_defaults
        
        return defaults
    
    def format_changes_email(self, changes: List[Dict]) -> str:
        """Format schema changes for email alert."""
        if not changes:
            return "No schema changes detected."
        
        tp = now_pack()
        lines = [
            f"Schema Changes Detected",
            f"Time: {tp.human}",
            f"Mode: {get_mode()}",
            f"Database: ariadne",
            f"",
            f"Changes ({len(changes)} total):",
            "=" * 60
        ]
        
        # Group changes by type
        by_type = {}
        for change in changes:
            change_type = change['type']
            if change_type not in by_type:
                by_type[change_type] = []
            by_type[change_type].append(change)
        
        # Format each type of change
        for change_type, items in by_type.items():
            lines.append(f"\n{change_type.upper().replace('_', ' ')} ({len(items)} items):")
            lines.append("-" * 40)
            
            for item in items[:10]:  # Limit to first 10 of each type
                lines.append(f"  Table: {item.get('table', 'N/A')}")
                if 'column' in item:
                    lines.append(f"  Column: {item['column']}")
                if 'index' in item:
                    lines.append(f"  Index: {item['index']}")
                lines.append(f"  Details: {item['details']}")
                
                if item.get('needs_default'):
                    lines.append(f"  ⚠️ ACTION: Default value will be applied")
                lines.append("")
            
            if len(items) > 10:
                lines.append(f"  ... and {len(items) - 10} more\n")
        
        lines.extend([
            "=" * 60,
            "",
            "Schema cache has been updated.",
            "Andi will use new schemas for validation.",
            "",
            "To pause trading for maintenance:",
            "  1. Use dashboard pause function",
            "  2. Wait for confirmation",
            "  3. Make schema changes",
            "  4. Resume trading"
        ])
        
        return "\n".join(lines)

# ──────────────────────────────────────────────────────────────────
# Main Process
# ──────────────────────────────────────────────────────────────────

class KARIN:
    """Main KARIN process - monitors ALL schemas continuously."""
    
    def __init__(self):
        self.discovery = SchemaDiscovery()
        self.check_count = 0
        self.changes_detected = 0
    
    def check_schemas(self):
        """Single schema check cycle."""
        self.check_count += 1
        
        try:
            # Discover current schemas for ALL tables
            current_schemas = self.discovery.discover_all_schemas()
            
            if not current_schemas:
                log.warning("No schemas discovered, database may be unavailable")
                return
            
            # Compare with cached schemas
            if self.discovery.last_schemas:
                changes = self.discovery.compare_schemas(
                    self.discovery.last_schemas, 
                    current_schemas
                )
                
                if changes:
                    self.changes_detected += len(changes)
                    log.info(f"Detected {len(changes)} schema changes")
                    
                    # Send email alert
                    try:
                        email_body = self.discovery.format_changes_email(changes)
                        mailer.send_email(
                            to=ALERT_EMAIL_RECIPIENT,
                            subject=f"[KARIN] Schema Changes Detected - {get_mode()} mode",
                            text=email_body
                        )
                        log.info(f"Alert sent to {ALERT_EMAIL_RECIPIENT}")
                    except Exception as e:
                        log.error(f"Failed to send email alert: {e}")
                    
                    # Update cache for Andi
                    self.discovery.save_cache(current_schemas)
                    self.discovery.last_schemas = current_schemas
                else:
                    log.debug(f"Check #{self.check_count}: No schema changes")
            else:
                # First run, just save
                log.info(f"Initial schema discovery complete - {len(current_schemas)} tables")
                self.discovery.save_cache(current_schemas)
                self.discovery.last_schemas = current_schemas
        
        except Exception as e:
            log.error(f"Schema check failed: {e}")
    
    def update_heartbeat(self):
        """Update heartbeat in database."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO heartbeats (process_name, last_heartbeat, status, pid, cycle_count)
                VALUES ('karin', NOW(), 'monitoring', %s, %s)
                ON CONFLICT (process_name)
                DO UPDATE SET 
                    last_heartbeat = NOW(),
                    status = 'monitoring',
                    pid = %s,
                    cycle_count = %s
            """, (os.getpid(), self.check_count, os.getpid(), self.check_count))
            
            conn.commit()
            cur.close()
            release_db_connection(conn)
            
        except Exception as e:
            log.error(f"Failed to update heartbeat: {e}")
    
    async def run_async(self):
        """Async main loop for schema checking."""
        log.info(f"[INIT] KARIN starting in {get_mode()} mode")
        log.info(f"[INIT] Will monitor ALL tables in database")
        log.info(f"[INIT] Check interval: {CHECK_INTERVAL} seconds")
        
        # Initial check
        self.check_schemas()
        
        while not shutdown_requested:
            try:
                start = time.time()
                
                # Check schemas
                self.check_schemas()
                
                # Update heartbeat every 10 checks (5 minutes)
                if self.check_count % 10 == 0:
                    self.update_heartbeat()
                    log.info(f"[HEARTBEAT] Checks: {self.check_count}, Changes: {self.changes_detected}")
                
                # Calculate sleep time
                elapsed = time.time() - start
                sleep_time = max(CHECK_INTERVAL - elapsed, 1)
                
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                log.error(f"Error in main loop: {e}")
                await asyncio.sleep(CHECK_INTERVAL)
        
        log.info("[SHUTDOWN] KARIN shutting down gracefully")
    
    def run(self):
        """Synchronous wrapper for the async run method."""
        try:
            write_pid_file(PID_FILE)
            asyncio.run(self.run_async())
        finally:
            cleanup_pid_file(PID_FILE)

# ──────────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    karin = KARIN()
    karin.run()