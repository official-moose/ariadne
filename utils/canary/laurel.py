#>> 🍁 A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 6                ✅ Production ready
#>>
#>> LAUREL - Heartbeat Monitor                           
#>> mm/utils/canary/laurel.py
#>>
#>> Monitors all process heartbeats, alerts on failures
#>> Sends hourly status report as own heartbeat
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]        💫 PERSISTANT RUNTIME  ➰ MONIT MANAGED
#>>────────────────────────────────────────────────────────────────

# Build|20250906.01

import os
import sys
import signal
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.append('/root/Echelon/valentrix')

from mm.utils.helpers.wintermute import (
    get_logger,
    EmailClient,
    now_pack,
    write_pid_file,
    cleanup_pid_file,
    get_db_connection,
    release_db_connection
)
from mm.utils.helpers.inara import get_mode
from mm.config.marcus import ALERT_EMAIL_RECIPIENT

# ──────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────

PID_FILE = "/root/Echelon/valentrix/mm/utils/canary/laurel.pid"
LOG_FILE = "/root/Echelon/valentrix/mm/utils/canary/laurel.log"
NOTES_FILE = "/root/Echelon/valentrix/mm/utils/canary/laurel_notes.log"

# Monitoring parameters
CHECK_INTERVAL = 30  # Check every 30 seconds
HOURLY_REPORT_INTERVAL = 3600  # Send status email every hour

# Process-specific heartbeat thresholds (in seconds)
# How long before we consider a process missing
HEARTBEAT_THRESHOLDS = {
    'ariadne': 120,      # Main bot - 2 minutes
    'hari': 60,          # SOC - 1 minute
    'alma': 30,          # Ticker sticker - 30 seconds  
    'karin': 60,         # Schema monitor - 1 minute
    'andi': 360,         # TQT processor - 6 minutes (updates every 5)
    'edith': 1800,       # Partition manager - 30 minutes
    'default': 300       # Default - 5 minutes
}

# Alert levels
ALERT_LEVELS = {
    1: "WATCH",      # Missed 1 check
    2: "WARNING",    # Missed 2 checks  
    3: "CRITICAL"    # Missed 3+ checks - process down
}

# Global shutdown flag
shutdown_requested = False

# Loggers
log = get_logger("laurel", LOG_FILE)
notes = get_logger("laurel.notes", NOTES_FILE)

# Email client
mailer = EmailClient('laurel')

# ──────────────────────────────────────────────────────────────────
# Signal Handlers
# ──────────────────────────────────────────────────────────────────

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    log.info(f"[SHUTDOWN] Received signal {signum}, shutting down...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ──────────────────────────────────────────────────────────────────
# Heartbeat Checker
# ──────────────────────────────────────────────────────────────────

class HeartbeatMonitor:
    """Monitors process heartbeats and sends alerts."""
    
    def __init__(self):
        self.missed_checks = defaultdict(int)  # Track consecutive misses
        self.last_alert_level = defaultdict(int)  # Track alert escalation
        self.process_status = {}  # Current status of each process
        self.check_count = 0
        self.alerts_sent = 0
        self.last_hourly_report = time.time()
    
    def check_heartbeats(self) -> Dict[str, Dict]:
        """
        Check all process heartbeats.
        Returns dict of process statuses.
        """
        self.check_count += 1
        conn = None
        statuses = {}
        
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Get all heartbeats
            cur.execute("""
                SELECT 
                    process_name,
                    last_heartbeat,
                    status,
                    pid,
                    cycle_count,
                    EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) as seconds_since
                FROM heartbeats
            """)
            
            rows = cur.fetchall()
            cur.close()
            
            # Check each process
            for row in rows:
                process = row['process_name']
                seconds_since = float(row['seconds_since']) if row['seconds_since'] else 999999
                threshold = HEARTBEAT_THRESHOLDS.get(process, HEARTBEAT_THRESHOLDS['default'])
                
                # Determine if heartbeat is stale
                is_stale = seconds_since > threshold
                
                statuses[process] = {
                    'last_heartbeat': row['last_heartbeat'],
                    'seconds_since': seconds_since,
                    'threshold': threshold,
                    'pid': row['pid'],
                    'status': row['status'],
                    'cycle_count': row['cycle_count'],
                    'is_stale': is_stale
                }
                
                # Handle stale heartbeats
                if is_stale:
                    self.handle_stale_heartbeat(process, seconds_since, threshold)
                else:
                    # Reset if back to normal
                    if self.missed_checks[process] > 0:
                        notes.info(f"{process} recovered after {self.missed_checks[process]} missed checks")
                        self.missed_checks[process] = 0
                        self.last_alert_level[process] = 0
            
            # Check for processes that have never reported
            self.check_missing_processes(statuses)
            
        except Exception as e:
            log.error(f"Failed to check heartbeats: {e}")
        finally:
            if conn:
                release_db_connection(conn)
        
        self.process_status = statuses
        return statuses
    
    def handle_stale_heartbeat(self, process: str, seconds_since: float, threshold: float):
        """Handle a stale heartbeat with tiered alerting."""
        self.missed_checks[process] += 1
        misses = self.missed_checks[process]
        
        # Determine alert level
        if misses >= 3:
            alert_level = 3
        elif misses == 2:
            alert_level = 2
        else:
            alert_level = 1
        
        # Only alert if escalating
        if alert_level > self.last_alert_level[process]:
            self.send_process_alert(process, alert_level, seconds_since, threshold)
            self.last_alert_level[process] = alert_level
        
        # Log for tracking
        level_name = ALERT_LEVELS[alert_level]
        notes.warning(f"[{level_name}] {process}: {seconds_since:.0f}s since heartbeat (threshold: {threshold}s)")
    
    def check_missing_processes(self, current_statuses: Dict):
        """Check for expected processes that have never reported."""
        expected = set(HEARTBEAT_THRESHOLDS.keys()) - {'default'}
        reporting = set(current_statuses.keys())
        missing = expected - reporting
        
        for process in missing:
            # Only alert once per missing process
            if process not in self.missed_checks:
                self.missed_checks[process] = 3  # Treat as critical
                self.send_process_alert(process, 3, None, None, missing=True)
    
    def send_process_alert(self, process: str, alert_level: int, 
                          seconds_since: Optional[float], threshold: Optional[float],
                          missing: bool = False):
        """Send alert for process issues."""
        try:
            tp = now_pack()
            level_name = ALERT_LEVELS[alert_level]
            
            # Build subject based on level
            if alert_level == 3:
                subject = f"[PRDW] {process} DOWN - {get_mode()}"
            elif alert_level == 2:
                subject = f"[WARNING] {process} heartbeat delayed - {get_mode()}"
            else:
                subject = f"[WATCH] {process} heartbeat check - {get_mode()}"
            
            # Build message
            if missing:
                message = f"""
CRITICAL: Process Never Started

Process: {process}
Status: NEVER REPORTED
Mode: {get_mode()}
Time: {tp.human}

Action Required: Start {process} immediately
"""
            else:
                minutes_since = seconds_since / 60.0 if seconds_since else 0
                message = f"""
{level_name}: Process Heartbeat Issue

Process: {process}
Last Heartbeat: {minutes_since:.1f} minutes ago
Threshold: {threshold} seconds
Consecutive Misses: {self.missed_checks[process]}
Alert Level: {alert_level}/3
Mode: {get_mode()}
Time: {tp.human}

"""
                if alert_level == 3:
                    message += "ACTION REQUIRED: Process appears to be DOWN. Restart immediately.\n"
                elif alert_level == 2:
                    message += "WARNING: Process may be struggling. Check logs.\n"
                else:
                    message += "NOTICE: Monitoring closely, no action required yet.\n"
            
            # Add current system status
            message += self._format_system_status()
            
            # Send alert
            mailer.send_email(
                to=ALERT_EMAIL_RECIPIENT,
                subject=subject,
                text=message
            )
            self.alerts_sent += 1
            log.info(f"Alert sent: {subject}")
            
        except Exception as e:
            log.error(f"Failed to send alert for {process}: {e}")
    
    def send_hourly_report(self):
        """Send hourly status report (Laurel's own heartbeat)."""
        try:
            tp = now_pack()
            
            # Check if all systems normal
            all_normal = all(
                not status.get('is_stale', False) 
                for status in self.process_status.values()
            )
            
            if all_normal:
                subject = f"Reporting in: All systems normal. {tp.dt.strftime('%H:%M:%S')}"
                message = f"""
Hourly Status Report

Time: {tp.human}
Mode: {get_mode()}
Status: ALL SYSTEMS OPERATIONAL

Monitoring {len(self.process_status)} processes:
"""
            else:
                problem_count = sum(1 for s in self.process_status.values() if s.get('is_stale', False))
                subject = f"Reporting in: {problem_count} issues detected. {tp.dt.strftime('%H:%M:%S')}"
                message = f"""
Hourly Status Report - ISSUES DETECTED

Time: {tp.human}
Mode: {get_mode()}
Status: {problem_count} PROCESS(ES) WITH ISSUES

Issues:
"""
                for process, status in self.process_status.items():
                    if status.get('is_stale', False):
                        message += f"- {process}: {status['seconds_since']:.0f}s since heartbeat\n"
                
                message += "\nAll Processes:\n"
            
            # Add status details
            message += self._format_system_status()
            
            # Add Laurel's own stats
            message += f"""
Laurel Statistics:
- Checks Performed: {self.check_count}
- Alerts Sent: {self.alerts_sent}
- Uptime: {(time.time() - start_time) / 3600:.1f} hours
"""
            
            mailer.send_email(
                to=ALERT_EMAIL_RECIPIENT,
                subject=subject,
                text=message
            )
            log.info(f"Hourly report sent: {'All normal' if all_normal else f'{problem_count} issues'}")
            
        except Exception as e:
            log.error(f"Failed to send hourly report: {e}")
    
    def _format_system_status(self) -> str:
        """Format current system status for reports."""
        lines = ["\nCurrent System Status:"]
        lines.append("-" * 50)
        
        for process, status in sorted(self.process_status.items()):
            if status.get('is_stale', False):
                state = "⚠️ STALE"
            else:
                state = "✓ OK"
            
            seconds = status.get('seconds_since', 0)
            if seconds < 60:
                time_str = f"{seconds:.0f}s ago"
            else:
                time_str = f"{seconds/60:.1f}m ago"
            
            lines.append(f"{process:12} {state:8} Last: {time_str:10} PID: {status.get('pid', 'N/A')}")
        
        lines.append("-" * 50)
        return "\n".join(lines) + "\n"
    
    def update_own_heartbeat(self):
        """Update Laurel's own heartbeat."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO heartbeats (process_name, last_heartbeat, status, pid, cycle_count)
                VALUES ('laurel', NOW(), 'monitoring', %s, %s)
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
            log.error(f"Failed to update own heartbeat: {e}")

# ──────────────────────────────────────────────────────────────────
# Main Process Loop
# ──────────────────────────────────────────────────────────────────

def main():
    """Main process loop."""
    global start_time
    start_time = time.time()
    
    try:
        write_pid_file(PID_FILE)
        log.info(f"[INIT] LAUREL starting in {get_mode()} mode")
        log.info(f"[INIT] Monitoring {len(HEARTBEAT_THRESHOLDS)-1} processes")
        log.info(f"[INIT] Check interval: {CHECK_INTERVAL}s, Report interval: {HOURLY_REPORT_INTERVAL}s")
        
        monitor = HeartbeatMonitor()
        
        # Initial check
        monitor.check_heartbeats()
        monitor.update_own_heartbeat()
        
        # Send initial report
        monitor.send_hourly_report()
        monitor.last_hourly_report = time.time()
        
        # Main loop
        while not shutdown_requested:
            # Sleep for check interval
            time.sleep(CHECK_INTERVAL)
            
            # Check all heartbeats
            monitor.check_heartbeats()
            
            # Update own heartbeat every 2 checks (1 minute)
            if monitor.check_count % 2 == 0:
                monitor.update_own_heartbeat()
            
            # Send hourly report
            if time.time() - monitor.last_hourly_report >= HOURLY_REPORT_INTERVAL:
                monitor.send_hourly_report()
                monitor.last_hourly_report = time.time()
        
        log.info("[SHUTDOWN] LAUREL shutting down gracefully")
        
    except KeyboardInterrupt:
        log.info("[SHUTDOWN] Interrupted by user")
    except Exception as e:
        log.error(f"[FATAL] {e}")
    finally:
        cleanup_pid_file(PID_FILE)

# ──────────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()