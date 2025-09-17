#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250917.01
#===================================================================
# last update: 2025 | Sept. 17                  Production ready ✅
#===================================================================
# Inara
# mm/utils/helpers/inara.py
#
# Determines what mode to operate in - simulation or live
# Central logic disseminating her decision to the other files
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✖ PERSISTANT RUNTIME  ✖ MONIT MANAGED
#===================================================================

# 🔸 Standard Library Imports ======================================

import logging
import os
import json
import time
import importlib
import smtplib
import ssl
import uuid
from email.message import EmailMessage
from email.utils import formataddr
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Literal, Optional
from functools import lru_cache
from dotenv import load_dotenv

# 🔸 local application imports =====================================

import mm.config.marcus as marcus

# 🔸 load env for this process =====================================
load_dotenv("mm/data/secrets/.env")

logger = logging.getLogger("ariadne")

USERCODE = "INA"  # this file's identity

# 🔸 Allowed operational modes =====================================

ALLOWED_MODES: set[str] = {
    "simulation",
    "live",
    "halted",
    "drain",
    "maintenance",
    "shadow",
}

# 🔸 Current runtime mode (mutable) ================================
_mode: str = "simulation"

STATE_DIR = os.path.join(os.path.dirname(__file__), "../../data/state")
MODE_FILE = os.path.join(STATE_DIR, "mode.json")


def send_email(subject: str, status: str, title: str, message: str) -> str:
    importlib.reload(marcus)
    if not bool(getattr(marcus, "ALERT_EMAIL_ENABLED", False)):
        return "disabled"
    if str(getattr(marcus, "ALERT_EMAIL_ENCRYPT", "SSL")).upper() != "SSL":
        return "Simple Mail Transfer Protocol not established. No conn."

    host = getattr(marcus, "ALERT_EMAIL_SMTP_SERVER", None)
    port = getattr(marcus, "ALERT_EMAIL_SMTP_PORT", None)
    recipient = getattr(marcus, "ALERT_EMAIL_RECIPIENT", None)

    user = os.getenv(f"{USERCODE}_USR")
    pwd = os.getenv(f"{USERCODE}_PWD")
    sender_email = user
    sender_name = os.getenv(f"{USERCODE}_NAME")

    STATUS_COLORS = {
        "STATCON3": "#F1C232",
        "STATCON2": "#E69138",
        "STATCON1": "#CC0000",
        "SIGCON1":  "#FB6D8B",
        "OPSCON5":  "#F5F5F5",
        "OPSCON1":  "#990000",
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

    now_tz = datetime.now(ZoneInfo("America/Toronto"))
    sent_str = now_tz.strftime("%Y-%m-%d %H:%M:%S America/Toronto")
    epoch_ms = int(now_tz.timestamp() * 1000)
    mid_clean = msg_id.strip("<>").split("@", 1)[0]

    html_body = f"""
<div style="font-family: monospace;">
  <table role="presentation" width="100%" height="20px" cellpadding="8px" cellspacing="0" border="0">
    <tr style="font-family: Georgia, 'Times New Roman', Times, serif;font-size:20px;font-weight:600;background-color:#333;">
      <td align="left" style="color:#EFEFEF;letter-spacing:12px;">INTCOMM</td>
      <td align="right" style="color:{status_color};letter-spacing:4px;">{status_text}</td>
    </tr>
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:14px;font-weight:600;color:#333;">
      <td colspan="2">{title}</td>
    </tr>
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:11px;font-weight:400;line-height:1.5;color:#333;">
      <td colspan="2">{message}</td>
    </tr>
    <tr width="100%" height="25px"><td colspan="2">&nbsp;</td></tr>
  </table>

  <table role="presentation" width="400px" height="20px" cellpadding="4" cellspacing="0" border="0" style="font-family: Tahoma, Geneva, sans-serif;">
    <tr style="background-color:#333;">
      <td colspan="2" style="color:#efefef;font-size:12px;font-weight:600;">DOCINT</td>
    </tr>
    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">SENT</td>
      <td width="10px">→</td>
      <td style="color:#333;font-size:11px;">{sent_str}</td>
    </tr>
    <tr style="background-color:#F2F2F0;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">EPOCH</td>
      <td width="10px">→</td>
      <td style="color:#333;font-size:11px;">{epoch_ms} (ms since 1970/01/01 0:00 UTC)</td>
    </tr>
    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">m.ID</td>
      <td width="10px">→</td>
      <td style="color:#333;font-size:11px;">{mid_clean}</td>
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


def _write_prev_mode(mode: str):
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        if mode not in ("simulation", "live"):
            return
        tmpfile = MODE_FILE + ".tmp"
        with open(tmpfile, "w") as f:
            json.dump({"prev_mode": mode}, f)
        os.replace(tmpfile, MODE_FILE)
        logger.debug(f"prev_mode updated to {mode}")
    except Exception as e:
        logger.error(f"Failed to write prev_mode file: {e}")


def _read_prev_mode() -> str:
    try:
        with open(MODE_FILE, "r") as f:
            data = json.load(f)
            prev = data.get("prev_mode")
            if prev in ("simulation", "live"):
                return prev
    except FileNotFoundError:
        logger.warning("mode.json not found, defaulting prev_mode=simulation")
    except Exception as e:
        logger.error(f"Failed to read prev_mode file: {e}")
    return "simulation"


@lru_cache(maxsize=1)
def get_mode() -> str:
    global _mode
    if _mode:
        return _mode
    try:
        from mm.config.marcus import MODE
        m = str(MODE).lower()
        if m in ALLOWED_MODES:
            _mode = m
            if _mode in ("simulation", "live"):
                _write_prev_mode(_mode)
            return _mode
    except Exception:
        logger.exception("Error importing marcus.MODE")

    _mode = "halted"
    try:
        send_email(
            subject="[Ariadne] Mode defaulted to HALTED",
            status="OPSCON1",
            title="Mode Fallback Triggered",
            message=("Inara could not determine a valid MODE. "
                     "System has defaulted to HALTED. "
                     "Please investigate marcus.py and restart."),
        )
    except Exception as e:
        logger.error("Failed to send halt alert: %s", e)

    return _mode


def require_mode(required_mode: str) -> None:
    current = get_mode()
    if current != required_mode:
        logger.error(f"[ABORT] This process requires {required_mode} mode but system is in {current} mode")
        raise RuntimeError(f"Mode mismatch: requires {required_mode}, got {current}")


def override_mode(new_mode: str, origin: Optional[str] = None, reason: Optional[str] = None) -> None:
    global _mode
    nm = str(new_mode).lower()
    if nm not in ALLOWED_MODES:
        logger.error(f"Invalid override mode: {new_mode}")
        return

    get_mode.cache_clear()
    _mode = nm
    get_mode()

    try:
        send_email(
            subject=f"[Ariadne] Mode overridden to {new_mode.upper()}",
            status="STATCON2",
            title="Manual Mode Override",
            message=f"Mode changed by {origin or 'unknown'} for reason: {reason or 'unspecified'}",
        )
    except Exception as e:
        logger.error("Failed to send override alert: %s", e)


def get_trading_client():
    counter = 0
    last_exc = None

    while counter < 3:
        counter += 1
        try:
            mode = get_mode()
            if mode == "simulation":
                from mm.conn.sim_kucoin import SimClient as TradingClient
                client = TradingClient()
                logger.info("Trading client initialized (simulation)")
                return client
            elif mode == "live":
                from mm.conn.conn_kucoin import KucoinClient as TradingClient
                client = TradingClient()
                logger.info("Trading client initialized (live)")
                return client
            elif mode == "halted":
                prev_mode = _read_prev_mode()
                if prev_mode == "simulation":
                    from mm.conn.sim_kucoin import SimClient as TradingClient
                    client = TradingClient()
                    logger.info("Trading client initialized in halted mode (prev=simulation)")
                    return client
                elif prev_mode == "live":
                    from mm.conn.conn_kucoin import KucoinClient as TradingClient
                    client = TradingClient()
                    logger.info("Trading client initialized in halted mode (prev=live)")
                    return client
                else:
                    from mm.conn.sim_kucoin import SimClient as TradingClient
                    client = TradingClient()
                    logger.warning("prev_mode could not be determined, defaulting to simulation")
                    send_email("inara.get_trading_client", "STATCON3", "Fallback to Simulation", "prev_mode could not be determined, defaulting to simulation")
                    return client
            else:
                logger.warning(f"Mode {mode} does not support client initialization")
                return None

        except Exception as e:
            last_exc = e
            logger.error(f"Client init failed (attempt {counter}/3): {e}")
            time.sleep(2)

    send_email("inara.get_trading_client", "STATCON1", "Trading Client Failure", f"Trading client failed after retries: {last_exc}")
    logger.critical("Trading client failed after retries, forcing halted mode")
    override_mode("halted", origin="inara.get_trading_client", reason="client init failure")

    try:
        from mm.conn.sim_kucoin import SimClient as TradingClient
        client = TradingClient()
        logger.warning("Returning simulation client as safe fallback")
        return client
    except Exception as e:
        logger.error(f"Failed to init simulation client fallback: {e}")
        return None
