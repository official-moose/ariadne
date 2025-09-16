#===================================================================
# 🍁 A R I A N D E                    version 6.1 build 20250913.03
#===================================================================
# last update: 2025 | Sept. 13                  Production ready ✅
#===================================================================
# LAUREL - Heartbeat Monitor
# mm/utils/canary/laurel.py
#
# Monitors all process heartbeats, alerts on failures
# Sends hourly status report as own heartbeat
#
# [520] [741] [8]
#===================================================================
# 🔰 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

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
import smtplib
import ssl
import uuid
from email.message import EmailMessage
from email.utils import formataddr
from zoneinfo import ZoneInfo
import importlib

# 🔸 Load environment variables ====================================

load_dotenv()

# 🔸 Add parent directory to path for imports ======================

sys.path.append('/root/Echelon/valentrix')

from mm.utils.helpers.wintermute import (
    get_logger,
    now_pack,
    write_pid_file,
    cleanup_pid_file,
    get_db_connection,
    release_db_connection
)
from mm.utils.helpers.inara import get_mode
from mm.config.marcus import ALERT_EMAIL_RECIPIENT
import mm.config.marcus as marcus

# 🔸 Email Function ================================================

def send_email(subject: str, status: str, title: str, message: str) -> str:
    import importlib
    import os
    import smtplib
    import ssl
    import uuid
    from email.message import EmailMessage
    from email.utils import formataddr
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import mm.config.marcus as marcus

    importlib.reload(marcus)
    if not bool(getattr(marcus, "ALERT_EMAIL_ENABLED", False)):
        return "disabled"
    if str(getattr(marcus, "ALERT_EMAIL_ENCRYPT", "SSL")).upper() != "SSL":
        return "Simple Mail Transfer Protocol not established. No conn."

    host = getattr(marcus, "ALERT_EMAIL_SMTP_SERVER", None)
    port = getattr(marcus, "ALERT_EMAIL_SMTP_PORT", None)
    recipient = getattr(marcus, "ALERT_EMAIL_RECIPIENT", None)

    USERCODE = "LAU"  # hardcode per file

    # ---- Edit Sender Info (per file) ----
    user = os.getenv(f"{USERCODE}_USR")
    pwd = os.getenv(f"{USERCODE}_PWD")
    sender_email = user
    sender_name = os.getenv(f"{USERCODE}_NAME")
    # -------------------------------------

    # status color map
    STATUS_COLORS = {
        "STATCON3": "#F1C232",	# on the first missing heartbeat 
        "STATCON2": "#E69138",	# on the second missing heartbeat
        "STATCON1": "#CC0000",	# on the third missing heartbeat
        "SIGCON1": 	"#FB6D8B",	# Process never started
		"OPSCON5": 	"#F5F5F5",	# Normal, all systems nominal
        "OPSCON1": 	"#990000",	# Issues detected
    }    
    
    status_text = str(status).upper()
    status_color = STATUS_COLORS.get(status_text, "#BE644C")

    msg = EmailMessage()
    domain = sender_email.split("@")[1] if "@" in sender_email else "hodlcorp.io"
    msg_id = f"<{uuid.uuid4()}@{domain}>"
    msg["Message-ID"] = msg_id
    msg["From"] = formataddr((sender_name, sender_email))
    msg["To"] = recipient
    msg["Subject"] = subject
    msg["X-Priority"] = "1"
    msg["X-MSMail-Priority"] = "High"
    msg["Importance"] = "High"

    # footer fields
    now_tz = datetime.now(ZoneInfo("America/Toronto"))
    sent_str = now_tz.strftime("%Y-%m-%d %H:%M:%S America/Toronto")
    epoch_ms = int(now_tz.timestamp() * 1000)
    mid_clean = msg_id.strip("<>").split("@", 1)[0]

    # full HTML body (single block)
    html_body = f"""
<div style="font-family: monospace;">
  <table role="presentation" width="100%" height="20px" cellpadding="8px" cellspacing="0" border="0">
    <!-- Top Banner -->
    <tr style="font-family: Georgia, 'Times New Roman', Times, serif;font-size:20px;font-weight:600;background-color:#333;">
      <td align="left" style="color:#EFEFEF;letter-spacing:12px;">INTCOMM</td>
      <td align="right" style="color:{status_color};letter-spacing:4px;">{status_text}</td>
    </tr>

    <!-- Message Title -->
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:14px;font-weight:600;color:#333;">
      <td colspan="2">
        {title}
      </td>
    </tr>

    <!-- Message Content -->
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:11px;font-weight:400;line-height:1.5;color:#333;">
      <td colspan="2">
        {message}
      </td>
    </tr>

    <!-- UNUSED SPACER ROW -->
    <tr width="100%" height="25px"><td colspan="2">&nbsp;</td></tr>
  </table>

  <!-- Footer -->
  <table role="presentation" width="400px" height="20px" cellpadding="4" cellspacing="0" border="0" style="font-family: Tahoma, Geneva, sans-serif;">
    <!-- DOCINT -->
    <tr style="background-color:#333;">
      <td colspan="2" style="color:#efefef;font-size:12px;font-weight:600;">DOCINT</td>
    </tr>

    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">SENT</td>

      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">&rarr;</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{sent_str}</td>
    </tr>

    <tr style="background-color:#F2F2F0;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">EPOCH</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">&rarr;</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{epoch_ms} (ms since 1970/01/01 0:00 UTC)</td>
    </tr>

    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">m.ID</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">&rarr;</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{mid_clean}</td>
    </tr>
  </table>
</div>
"""

    msg.add_alternative(html_body, subtype="html")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(host, port, context=ctx, timeout=10) as s:
        if user and pwd:
            s.login(user, pwd)
        s.send_message(msg)

    return msg_id

# 🔸 Configuration =================================================

PID_FILE = "/root/Echelon/valentrix/mm/utils/canary/laurel.pid"
LOG_FILE = "/root/Echelon/valentrix/mm/utils/canary/laurel.log"
NOTES_FILE = "/root/Echelon/valentrix/mm/utils/canary/laurel_notes.log"

# 🔸 Monitoring parameters =========================================

CHECK_INTERVAL = 30  # Check every 30 seconds
HOURLY_REPORT_INTERVAL = 3600  # Send status email every hour

# 🔸 Process-specific heartbeat thresholds (in seconds) ============

HEARTBEAT_THRESHOLDS = {
    #'ariadne': 120,      # Main bot - 2 minutes
    #'hari': 60,          # SOC - 1 minute
    'alma': 60,          # Ticker sticker - 60 seconds
    #'karin': 60,         # Schema monitor - 1 minute
    #'andi': 360,         # TQT processor - 6 minutes (updates every 5)
    'edith': 1800,       # Partition manager - 30 minutes
    #'default': 300       # Default - 5 minutes
}

# 🔸 Alert levels for email alerts =================================

ALERT_LEVELS = {
    1: "STATCON3",	# on the first missing heartbeat 
    2: "STATCON2",	# on the second missing heartbeat
    3: "STATCON1",	# on the third missing heartbeat
    4: "SIGCON1",	# Process never started
    5: "OPSCON5",	# Normal, all systems nominal
    6: "OPSCON1",	# Issues detected
}

# 🔸 Global shutdown flag ==========================================

shutdown_requested = False

# 🔸 Loggers =======================================================

log = get_logger("laurel", LOG_FILE)
notes = get_logger("laurel.notes", NOTES_FILE)

# 🔸 Signal Handlers ===============================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    log.info(f"[SHUTDOWN] Received signal {signum}, shutting down...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# 🔸 Heartbeat Checker =============================================

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
            
            # Build subject, status, and title based on condition
            if missing:
                subject = f"[ SIGCON1 ] {process} missing | Mode -> {get_mode()}"
                status = "SIGCON1"
                title = f"SIGCON1 | Critical Alert | {process} cannot be found."
            elif alert_level == 3:
                subject = f"[ STATCON1 ] !PROCESS DOWN! Flatline on {process} | Mode -> {get_mode()}"
                status = "STATCON1"
                title = f"STATCON1 | Critical Alert | {process} is not running."
            elif alert_level == 2:
                subject = f"[ STATCON2 ] Second missed heartbeat for {process} | Mode -> {get_mode()}"
                status = "STATCON2"
                title = f"STATCON2 | {process} has missed it's second check-in."
            else:  # alert_level == 1
                subject = f"[ STATCON3 ] Missed heartbeat for {process} | Mode -> {get_mode()}"
                status = "STATCON3"
                title = f"STATCON3 | {process} has missed it's first check-in."
            
            # Build message
            if missing:
                message_html = f"""<b>Process:</b> {process}<br>
<b>Status:</b> NEVER REPORTED<br>
<b>Mode:</b> {get_mode()}<br>
<b>Time:</b> {tp.human}<br>

<b>Action Required:</b> Start {process} immediately"""
            else:
                minutes_since = seconds_since / 60.0 if seconds_since else 0
                
                if alert_level == 3:
                    action_text = "ACTION REQUIRED: Process appears to be DOWN. Restart immediately."
                elif alert_level == 2:
                    action_text = "WARNING: Process may be struggling. Check logs."
                else:
                    action_text = "NOTICE: Monitoring closely, no action required yet."
                
                system_status = self._format_system_status()
                
                message_html = f"""<b>Process:</b> {process}<br>
<b>Last Heartbeat:</b> {minutes_since:.1f} minutes ago<br>
<b>Threshold:</b> {threshold} seconds<br>
<b>Consecutive Misses:</b> {self.missed_checks[process]}<br>
<b>Alert Level:</b> {alert_level}/3<br>
<b>Mode:</b> {get_mode()}<br>
<b>Time:</b> {tp.human}<br><br>

<b>{action_text}</b><br>
<b>{system_status}</b>"""

            # Send alert
            send_email(
                subject=subject,
                status=status,
                title=title,
                message=message_html
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
                subject = f"[ OPSCON5 ] All systems nominal | Mode -> {get_mode()}"
                status = "OPSCON5"
                title = "SITREP WHITE"

                # Add status details
                system_status = self._format_system_status()

                message_html = f"""
                                <b>Time:</b> {tp.human}<br>
                                <b>Mode:</b> {get_mode()}<br>
                                <b>Status:</b> ALL SYSTEMS OPERATIONAL<br>

                                <b>Monitoring {len(self.process_status)} processes:</b><br>
                                {system_status}<br><br>

                                <b>Laurel Statistics:</b><br>
                                - <b>Checks Performed:</b> {self.check_count}<br>
                                - <b>Alerts Sent:</b> {self.alerts_sent}<br>
                                - <b>Uptime:</b> {(time.time() - start_time) / 3600:.1f} hours<br>
                                """
            else:
                problem_count = sum(1 for s in self.process_status.values() if s.get('is_stale', False))
                subject = f"[ OPSCON1 ] PRIORITY SITREP -> {problem_count} issues detected. {tp.dt.strftime('%H:%M:%S')}"
                status = "OPSCON1"
                title = f"[ OPSCON1 ] Priority Situation Report - ISSUES DETECTED"

                # Build issues list
                issues_text = ""
                for process, stat in self.process_status.items():
                    if stat.get('is_stale', False):
                        issues_text += f"- {process}: {stat['seconds_since']:.0f}s since heartbeat\n"

                # Add status details
                system_status = self._format_system_status()

                message_html = f"""
                                <b>Time:</b> {tp.human}<br>
                                <b>Mode:</b> {get_mode()}<br>
                                <b>Status:</b> {problem_count} PROCESS(ES) WITH ISSUES<br><br>

                                <b>Issues:</b><br>
                                {issues_text}<br><br>

                                <b>All Processes:</b><br>
                                {system_status}<br><br>

                                <b>Laurel Statistics:</b><br>
                                - <b>Checks Performed:</b> {self.check_count}<br>
                                - <b>Alerts Sent:</b> {self.alerts_sent}<br>
                                - <b>Uptime:</b> {(time.time() - start_time) / 3600:.1f} hours<br>
                                """

            # Send report
            send_email(
                subject=subject,
                status=status,
                title=title,
                message=message_html
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
                state = "âš ï¸ STALE"
            else:
                state = "âœ“ OK"

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

# 🔸 Main Process Loop =============================================

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

# 🔸 Entry Point ===================================================

if __name__ == "__main__":
    main()