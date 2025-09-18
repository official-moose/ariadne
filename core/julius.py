#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250917.03
#===================================================================
# last update: 2025 | Sept. 17                  Production ready ✅
#===================================================================
# Julius - Banker
# mm/core/julius.py
#
# The single point of truth for cash/wallet state inside 
# the system.
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✔ PERSISTANT RUNTIME  ✔ MONIT MANAGED
#===================================================================

import os
import sys
import time
import signal
import pathlib
import json
from dataclasses import dataclass
from typing import Dict, Optional, Any

from mm.config import marcus
from mm.utils.helpers.inara import get_mode, get_trading_client
from mm.utils.helpers.wintermute import init_logging
from mm.utils.tqt.andi import SchemaValidator

import psycopg2

BALANCE_CACHE_PATH = "/root/Echelon/valentrix/mm/data/source/balances.json"
PID_PATH = pathlib.Path("mm/utils/soc/julius.pid")
HEARTBEAT_SEC = 5

# Logging: already set up, do not touch.
logger = init_logging(
    LOG_SELF=True,
    LOG_MAIN=True,
    SCREEN_OUT=True,
    LOGGER="Julius"
)

DSN = os.getenv("PG_DSN", "dbname=ariadne user=postgres host=localhost")

@dataclass
class JuliusCfg:
    quote: str = getattr(marcus, "QUOTE_CURRENCY", "USDT")
    min_trade: float = getattr(marcus, "MIN_TRADE_SIZE", 10.0)

class Julius:
    def __init__(self, cfg: JuliusCfg = JuliusCfg()):
        self.cfg = cfg
        self.client = get_trading_client()
        self.running = False
        self.pid_path = PID_PATH
        self._setup_db()
        self._write_pid()
        signal.signal(signal.SIGINT,  self._sig_term)
        signal.signal(signal.SIGTERM, self._sig_term)

    def _setup_db(self):
        self.conn = psycopg2.connect(DSN)
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()

    def _write_pid(self):
        self.pid_path.parent.mkdir(parents=True, exist_ok=True)
        self.pid_path.write_text(str(os.getpid()))

    def _remove_pid(self):
        if self.pid_path.exists():
            self.pid_path.unlink()

    def _heartbeat(self, cycle: int):
        try:
            queue_op("heartbeat", {
                "process": "julius",
                "cycle": cycle,
                "pid": os.getpid(),
                "ts": int(time.time())
            })
        except Exception as e:
            logger.warning(f"Heartbeat failed: {e}")

    def _expire_old_proposals(self):
        now = int(time.time())
        self.cur.execute(
            """
            UPDATE proposals
               SET status = 'expired', decision_stamp = NOW(), decision_notes = 'auto-expired (Julius)'
             WHERE status IN ('pending', 'approved')
               AND (EXTRACT(EPOCH FROM (NOW() - created_at))) > 30
            """
        )
        # proposals is the ONLY table Julius updates directly

    def get_balances(self) -> Dict[str, Dict[str, float]]:
        """
        Reads balances from Ash's JSON cache (live or simulation, depending on mode).
        """
        try:
            with open(BALANCE_CACHE_PATH, "r") as f:
                data = json.load(f)
            mode = current_mode()
            key = "simulation" if mode == "simulation" else "live"
            return data.get(key, {}).get("balances", {})
        except Exception as e:
            logger.error(f"[Julius] Failed to read Ash balance cache: {e}")
            return {}

    def vet_bank(self, proposal_id: str) -> str:
        # Approve/deny based on available funds
        prop = self._fetch_proposal(proposal_id)
        if not prop or prop["side"] != "buy":
            return "denied"
        notional = float(prop["price_intent"]) * float(prop["size_intent"])
        if notional < float(self.cfg.min_trade):
            self._set_vet(proposal_id, "bank_vet", "denied")
            self._route_log(proposal_id, "bank_denied", {"reason": "min_trade"})
            return "denied"
        ok = self._has_funds(notional)
        status = "approved" if ok else "denied"
        self._set_vet(proposal_id, "bank_vet", status)
        self._route_log(proposal_id, f"bank_{status}", {"notional": notional, "quote": self.cfg.quote})
        return status

    def _has_funds(self, notional: float) -> bool:
        bals = self.get_balances() or {}
        q = bals.get(self.cfg.quote, {})
        avail = float(q.get("available", 0.0))
        return avail >= float(notional)

    def _fetch_proposal(self, pid: str) -> Optional[Dict[str, Any]]:
        self.cur.execute(
            "SELECT prop_id, symbol, side, price_intent, size_intent FROM proposals WHERE prop_id = %s",
            (pid,)
        )
        row = self.cur.fetchone()
        if not row:
            return None
        keys = ("prop_id", "symbol", "side", "price_intent", "size_intent")
        return dict(zip(keys, row))

    def _set_vet(self, pid: str, col: str, status: str) -> None:
        self.cur.execute(
            f"UPDATE proposals SET {col} = %s, decision_stamp = NOW() WHERE prop_id = %s",
            (status, pid)
        )

    def _route_log(self, proposal_id: str, status: str, info: Dict) -> None:
        try:
            queue_op("proposal_router_log", {
                "proposal_id": proposal_id,
                "status": status,
                "info": info,
                "ts": int(time.time())
            })
        except Exception as e:
            logger.warning(f"Router log failed: {e}")

    def run_forever(self):
        logger.info("Julius started persistent loop.")
        self.running = True
        cycle = 0
        while self.running:
            try:
                self._heartbeat(cycle)
                self._expire_old_proposals()
                time.sleep(HEARTBEAT_SEC)
                cycle += 1
            except Exception as e:
                logger.error(f"Main loop error: {e}")

    def _sig_term(self, *_):
        self.running = False
        self._remove_pid()
        logger.info("Julius stopped (SIGTERM/SIGINT).")
        sys.exit(0)

if __name__ == "__main__":
    Julius().run_forever()

