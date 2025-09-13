#>> 🍁 A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 11                ❌ PRODUCTION READY
#>>
#>> mode checker
#>> mm/utils/helpers/inara.py
#>>
#>> determines what mode to operatate it - simulation or live
#>> central logic disseminating her decision to the other files 
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]                                       🤖 DROID
#>>────────────────────────────────────────────────────────────────

# Build|20250910.02

import logging
import os 
import json
import time
from typing import Literal, Optional

from mm.utils.helpers.wintermute import send_alert

logger = logging.getLogger(__name__)

# Allowed operational modes
ALLOWED_MODES: set[str] = {
    "simulation",
    "live",
    "halted",
    "drain",
    "maintenance",
    "shadow",
}

# Current runtime mode (mutable)
_mode: str = "simulation"

STATE_DIR = os.path.join(os.path.dirname(__file__), "../../data/state")
MODE_FILE = os.path.join(STATE_DIR, "mode.json")


def _write_prev_mode(mode: str):
    """Write last non-halted mode to mode.json."""
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
    """Read prev_mode from mode.json, default to simulation if missing/broken."""
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


def get_mode() -> str:
    """
    Central mode determination.
    Check order:
      1. override (if set via override_mode)
      2. marcus config (MODE)
      3. default to 'halted' (with alert)
    """
    global _mode
    if _mode:
        return _mode

    try:
        from mm.config.marcus import MODE  # type: ignore
        m = str(MODE).lower()
        if m in ALLOWED_MODES:
            _mode = m
            if _mode in ("simulation", "live"):
                _write_prev_mode(_mode)
            return _mode
    except Exception:
        logger.exception("Error importing marcus.MODE")

    # Fallback: invalid config → HALTED
    _mode = "halted"
    try:
        send_alert(
            subject="[Ariadne] Mode defaulted to HALTED",
            message="Inara could not determine a valid MODE. "
                    "System has defaulted to HALTED. "
                    "Please investigate marcus.py and restart.",
            process_name="inara"
        )
    except Exception as e:
        logger.error("Failed to send halt alert: %s", e)

    return _mode


def require_mode(required_mode: str) -> None:
    """
    Enforcement function. Raises if not in required mode.
    Usage: require_mode('simulation') at start of SOC
    """
    current = get_mode()
    if current != required_mode:
        logger.error(
            f"[ABORT] This process requires {required_mode} mode but system is in {current} mode"
        )
        raise RuntimeError(
            f"Mode mismatch: requires {required_mode}, got {current}"
        )


def override_mode(new_mode: str, origin: Optional[str] = None, reason: Optional[str] = None) -> None:
    """
    Override the current runtime mode.
    Sends an alert email so ops is aware.
    """
    global _mode
    nm = str(new_mode).lower()
    if nm not in ALLOWED_MODES:
        logger.error(f"Invalid override mode: {new_mode}")
        return

    _mode = nm
    try:
        send_alert(
            subject=f"[Ariadne] Mode overridden to {new_mode.upper()}",
            message=f"Mode changed by {origin or 'unknown'} for reason: {reason or 'unspecified'}",
            process_name="inara"
        )
    except Exception as e:
        logger.error("Failed to send override alert: %s", e)
        
def get_trading_client():
    """
    Initialize and return the correct trading client based on mode.
    Retries 3x with 2s sleep. On failure: alert + force halted + safe fallback.
    """
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
                    send_alert("inara.get_trading_client", "prev_mode could not be determined, defaulting to simulation")
                    return client

            else:
                logger.warning(f"Mode {mode} does not support client initialization")
                return None

        except Exception as e:
            last_exc = e
            logger.error(f"Client init failed (attempt {counter}/3): {e}")
            time.sleep(2)

    # If we reach here, failed after retries
    send_alert("inara.get_trading_client", f"Trading client failed after retries: {last_exc}")
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

