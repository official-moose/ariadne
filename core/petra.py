#>> A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 9                ❌ PRODUCTION READY
#>>
#>> Sales Manager
#>> mm/core/petra.py
#>>
#>> Responsible for selling inventory positions
#>> Identifies profitable exit opportunities  
#>> Places limit sell orders with proper spread
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]        💫 PERSISTANT RUNTIME  ➰ MONIT MANAGED     
#>>----------------------------------------------------------------

# Build|20250909.01

import os
import json
import time
import select
import signal
import pathlib
import logging
import logging.handlers
from dataclasses import dataclass
from typing import Optional, Dict, Any

import psycopg2

from mm.config import marcus
from mm.core.grayson import RiskOps
from mm.conn.conn_kucoin import KucoinClient
from mm.utils.tqt import andi

# Helen: inventory manager (asset holds)
try:
    from mm.core.helen import Helen
except Exception:
    Helen = None  # graceful fallback; guarded calls below

# Inara helpers (fallbacks keep Petra runnable if inara.py is older)
try:
    from mm.utils.helpers.inara import current_mode, can_place_orders, is_live_mode
except Exception:
    def current_mode() -> str:
        return getattr(marcus, "MODE", "simulation")
    def can_place_orders() -> bool:
        return current_mode() in ("live", "simulation", "shadow")
    def is_live_mode() -> bool:
        return current_mode() == "live"

# ────────────────────────── Config / Paths ────────────────────────────
DSN = os.getenv("PG_DSN", "dbname=ariadne user=postgres host=localhost")
CHANNEL_READY  = "proposals.ready.petra"
CHANNEL_DENIED = "proposals.denied.petra"
PROCESS_NAME   = "petra"

LOG_PATH = pathlib.Path("mm/logs/petra.log")
PID_PATH = pathlib.Path("mm/utils/soc/petra.pid")

HEARTBEAT_SEC = 5
LOOP_SLEEP    = 0.25

logger = logging.getLogger("petra")
logger.setLevel(logging.INFO)

def _setup_logging():
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(message)s'))
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(ch)

def _write_pid():
    PID_PATH.parent.mkdir(parents=True, exist_ok=True)
    PID_PATH.write_text(str(os.getpid()))

@dataclass
class PetraCfg:
    quote: str = marcus.QUOTE_CURRENCY
    min_trade: float = getattr(marcus, "MIN_TRADE_SIZE", 10.0)

class Petra:
    """
    SELL Originator:
      - Maintains two DB connections (LISTEN vs ops)
      - Reacts to proposals.ready.petra / proposals.denied.petra
      - On READY: place limit order (mode-gated); link/unwind asset holds via Helen (sim), queue to Andi
      - On DENIED/EXPIRED: log and stop progression
      - Generates lightweight sell proposals opportunistically (stub; DrCalvin/Lamar usually drive)
    """

    def __init__(self, client: Optional[KucoinClient] = None, cfg: PetraCfg = PetraCfg()):
        _setup_logging()
        _write_pid()

        self.cfg = cfg
        self.client = client or KucoinClient()
        self.grayson = Grayson()
        self.helen = Helen(self.client) if Helen else None

        # LISTEN connection (autocommit ON)
        self.listen_conn = psycopg2.connect(DSN)
        self.listen_conn.set_session(autocommit=True)
        self.listen_cur = self.listen_conn.cursor()
        self.listen_cur.execute(f"LISTEN {CHANNEL_READY};")
        self.listen_cur.execute(f"LISTEN {CHANNEL_DENIED};")

        # OPS connection (for queries/writes)
        self.ops_conn = psycopg2.connect(DSN)
        self.ops_cur  = self.ops_conn.cursor()

        self.running = False
        signal.signal(signal.SIGINT,  self._sig_term)
        signal.signal(signal.SIGTERM, self._sig_term)
        try:
            signal.signal(signal.SIGHUP, self._sig_hup)
        except Exception:
            pass

        logger.info("Petra initialized | mode=%s | quote=%s", current_mode(), self.cfg.quote)

    # ─────────────────────────── Main loop ────────────────────────────
    def run_forever(self):
        self.running = True
        last_hb = 0.0
        cycle = 0

        logger.info("Petra loop starting…")
        while self.running:
            now = time.time()
            if now - last_hb >= HEARTBEAT_SEC:
                self._heartbeat(cycle)
                last_hb = now

            # Non-blocking wait for notifications
            r, _, _ = select.select([self.listen_conn], [], [], LOOP_SLEEP)
            if r:
                self.listen_conn.poll()
                while self.listen_conn.notifies:
                    note = self.listen_conn.notifies.pop(0)
                    self._route_notify(note.channel, note.payload)

            # Opportunistic proposal generation (kept light)
            try:
                self._maybe_generate_proposals()
            except Exception as e:
                logger.debug("proposal-gen skipped: %s", e)

            cycle += 1

        logger.info("Petra loop stopped.")

    # ────────────────────── Notifications routing ─────────────────────
    def _route_notify(self, channel: str, payload: str):
        data = self._parse_payload(payload)

        if channel == CHANNEL_READY:
            self._on_ready(data)
        elif channel == CHANNEL_DENIED:
            self._on_denied(data)
        else:
            logger.debug("Ignoring channel=%s payload=%r", channel, payload)

    def _parse_payload(self, payload: str) -> Dict[str, Any]:
        try:
            return json.loads(payload)
        except Exception:
            d = {"raw": payload}
            s = payload.strip()
            if s.startswith("id:"):
                try:
                    d["proposal_id"] = int(s.split(":", 1)[1])
                except Exception:
                    pass
            return d

    # ──────────────────────── Handlers ────────────────────────────────
    def _on_ready(self, data: Dict[str, Any]):
        pid = data.get("proposal_id")
        if not pid:
            logger.error("READY missing proposal_id: %r", data)
            return

        prop = self._fetch_proposal(pid)
        if not prop:
            logger.error("proposal %s not found or not in APPROVED/READY state", pid)
            return

        mode = current_mode()
        # Gate: if mode doesn't allow real placement, just record a shadow finalize
        if not can_place_orders() or mode in ("shadow", "halted", "maintenance", "drain"):
            logger.info("[mode:%s] would SELL %s @%s x%s (shadow only)",
                        mode, prop["symbol"], prop["price_intent"], prop["size_intent"])
            self._mark_finalized(pid, ghost=True)
            self._log_route(pid, status="shadow-finalized", info={"mode": mode})
            return

        # Risk sanity (light; full vet happened upstream)
        if not self._risk_ok(prop):
            self._fail_proposal(pid, reason="risk_blocked")
            return

        # Place limit order on exchange
        try:
            order_id = self.client.create_limit_order(
                symbol=prop["symbol"],
                side="sell",
                price=float(prop["price_intent"]),
                size=float(prop["size_intent"]),
            )
        except Exception as e:
            logger.error("order placement failed proposal=%s: %s", pid, e)
            # Unwind any sim asset hold via Helen (live mode → no-op)
            self._safe_helen_cancel(reason="placement_failed", proposal_id=pid)
            self._fail_proposal(pid, reason="placement_failed")
            return

        # Link asset hold → order in SIM (live reserves are internal to exchange)
        self._safe_helen_link(order_id=order_id, proposal_id=pid)

        # Persist order intent via Andi (TQT)
        self._safe_andi_queue(
            proposal_id=pid,
            symbol=prop["symbol"],
            side="sell",
            price=float(prop["price_intent"]),
            size=float(prop["size_intent"]),
            order_id=order_id,
            mode=mode,
            origin="petra",
        )

        # finalize
        self._mark_finalized(pid)
        self._log_route(pid, status="finalized", info={"order_id": order_id})

    def _on_denied(self, data: Dict[str, Any]):
        pid = data.get("proposal_id")
        typ = data.get("type", "denied")  # "denied" | "expired"
        if not pid:
            logger.error("DENIED missing proposal_id: %r", data)
            return
        self._log_route(pid, status=typ, info=data)
        # no further action

    # ─────────────────────── Proposal plumbing ────────────────────────
    def _fetch_proposal(self, pid: int) -> Optional[Dict[str, Any]]:
        self.ops_cur.execute("""
            SELECT id, symbol, side, price_intent, size_intent, status
            FROM proposals
            WHERE id = %s AND side = 'sell' AND status IN ('approved','ready')
        """, (pid,))
        row = self.ops_cur.fetchone()
        if not row:
            return None
        keys = ("id", "symbol", "side", "price_intent", "size_intent", "status")
        return dict(zip(keys, row))

    def _mark_finalized(self, pid: int, ghost: bool = False):
        new_status = "shadow_finalized" if ghost else "finalized"
        self.ops_cur.execute("UPDATE proposals SET status = %s WHERE id = %s", (new_status, pid))
        self.ops_conn.commit()

    def _fail_proposal(self, pid: int, reason: str):
        self.ops_cur.execute("UPDATE proposals SET status = 'failed' WHERE id = %s", (pid,))
        self.ops_conn.commit()
        self._log_route(pid, status="failed", info={"reason": reason})

    def _log_route(self, pid: int, status: str, info: Dict[str, Any]):
        try:
            self.ops_cur.execute("""
                INSERT INTO proposal_router_log (proposal_id, timestamp, status, details)
                VALUES (%s, NOW(), %s, %s)
            """, (pid, status, json.dumps(info)))
            self.ops_conn.commit()
        except Exception as e:
            logger.warning("router log insert failed p=%s: %s", pid, e)

    # ────────────────────────── Helpers ───────────────────────────────
    def _risk_ok(self, prop: Dict[str, Any]) -> bool:
        try:
            notional = float(prop["price_intent"]) * float(prop["size_intent"])
            if notional < float(self.cfg.min_trade):
                return False
            # Provide equity externally if you want stricter checks; use 0.0 here as placeholder
            return self.grayson.can_trade_pair(prop["symbol"], 0.0)
        except Exception:
            return True

    def _maybe_generate_proposals(self):
        """
        Placeholder: normally call DrCalvin to rank & insert SELL intents based on inventory signals.
        Respect modes; skip in maintenance/halted.
        """
        m = current_mode()
        if m in ("halted", "maintenance"):
            return
        # Intentionally left minimal; Lamar handles routing → vets → approvals.

    def _safe_andi_queue(self, **kw):
        try:
            andi.queue_order(**kw)
        except Exception as e:
            logger.warning("andi.queue_order warn: %s", e)

    def _safe_helen_link(self, order_id: str, proposal_id: int):
        """
        Prefer correlation-based linking (no hold_id in proposals).
        Falls back quietly if your Helen expects (hold_id, order_id).
        """
        if not self.helen:
            return
        try:
            # Preferred: correlation id
            if hasattr(self.helen, "link_asset_hold_to_order"):
                try:
                    # new signature with correlation_id
                    self.helen.link_asset_hold_to_order(order_id=order_id, correlation_id=f"proposal:{proposal_id}")
                    return
                except TypeError:
                    pass
                # legacy: requires hold_id (Helen maps proposal→hold on her side or ignores)
                try:
                    self.helen.link_asset_hold_to_order(None, order_id)  # best-effort
                except Exception:
                    pass
        except Exception as e:
            logger.debug("helen link skipped: %s", e)

    def _safe_helen_cancel(self, reason: str, proposal_id: int):
        if not self.helen:
            return
        try:
            if hasattr(self.helen, "on_cancel"):
                try:
                    self.helen.on_cancel(reason=reason, correlation_id=f"proposal:{proposal_id}")
                except TypeError:
                    self.helen.on_cancel(order_id=None, reason=reason)
        except Exception as e:
            logger.debug("helen on_cancel skipped: %s", e)

    # ───────────────────────── Heartbeats ─────────────────────────────
    def _heartbeat(self, cycle_count: int):
        try:
            self.ops_cur.execute("""
                INSERT INTO heartbeats (process_name, last_heartbeat, status, pid, cycle_count)
                VALUES (%s, NOW(), 'ok', %s, %s)
                ON CONFLICT (process_name)
                DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat,
                              status         = EXCLUDED.status,
                              pid            = EXCLUDED.pid,
                              cycle_count    = EXCLUDED.cycle_count
            """, (PROCESS_NAME, os.getpid(), cycle_count))
            self.ops_conn.commit()
        except Exception as e:
            logger.warning("heartbeat failed: %s", e)

    # ─────────────────────── Signal handlers ──────────────────────────
    def _sig_term(self, *_):
        self.running = False
        try:
            self.ops_conn.commit()
        except Exception:
            pass
        try:
            if PID_PATH.exists():
                PID_PATH.unlink()
        except Exception:
            pass
        logger.info("Petra stopped (SIGTERM/SIGINT).")

    def _sig_hup(self, *_):
        # lightweight “reload”: toggle log level; mode is read on each use via current_mode()
        new = logging.DEBUG if logger.level != logging.DEBUG else logging.INFO
        logger.setLevel(new)
        logger.info("Petra received SIGHUP → log level now %s", logging.getLevelName(new))

# ─────────────────────────── Entrypoint ───────────────────────────────
if __name__ == "__main__":
    Petra().run_forever()
