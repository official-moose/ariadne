#>> A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 9                ❌ PRODUCTION READY
#>>
#>> Inventory Manager
#>> mm/core/helen.py
#>>
#>> Ensures orders are legitimate assets we own. 
#>> Approves orders via the proposals table.
#>> Manages assets.
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]        💫 PERSISTANT RUNTIME  ➰ MONIT MANAGED  
#>>────────────────────────────────────────────────────────────────

# Build|20250909.01

import os
import json
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import psycopg2

from mm.config import marcus
from mm.conn.conn_kucoin import KucoinClient
from mm.utils.helpers.wintermute import parse_symbol  # expects "BTC-USDT" -> ("BTC","USDT")

# Inara (mode gating). Safe fallbacks if helpers not present/newer.
try:
    from mm.utils.helpers.inara import current_mode, is_live_mode
except Exception:
    def current_mode() -> str:
        return getattr(marcus, "MODE", "simulation")
    def is_live_mode() -> bool:
        return current_mode() == "live"

DSN = os.getenv("PG_DSN", "dbname=ariadne user=postgres host=localhost")

logger = logging.getLogger("helen")
logger.setLevel(logging.INFO)


@dataclass
class HelenCfg:
    quote: str = marcus.QUOTE_CURRENCY  # e.g., "USDT"


class Helen:
    """
    Responsibilities
      • Phase 1 (vet): set proposals.invt_vet ← 'approved' | 'denied' based on inventory.
      • Phase 2 (finalize for SELL):
          - SIM: reserve by incrementing sim_positions.hold.
          - LIVE: no-op (exchange reserves), optional availability recheck.
      • Interfaces for Petra:
          - link_asset_hold_to_order(...): no-op in LIVE, SIM just logs (reservation already done).
          - on_cancel(...): SIM releases reservation (decrement sim_positions.hold); LIVE no-op.

    Notes
      • No hold_id is ever written to proposals (by design).
      • We use proposal_id as correlation to compute/rescind reserved qty.
      • get_positions(): used by main bot to read current inventory.
    """

    def __init__(self, client: Optional[KucoinClient] = None, cfg: HelenCfg = HelenCfg()):
        self.cfg = cfg
        self.client = client or KucoinClient()
        self.conn = psycopg2.connect(DSN)
        self.cur = self.conn.cursor()
        logger.info("Helen ready | mode=%s | quote=%s", current_mode(), self.cfg.quote)

    # ───────────────────────── Public API ─────────────────────────

    def get_positions(self) -> Dict[str, float]:
        """Return dict of { 'BTC-USDT': available_qty, ... }.
        SIM → from sim_positions (qty - hold); LIVE → from exchange balances (base assets)."""
        if is_live_mode():
            bals = self.client.get_account_balances_detailed()  # { 'BTC': {'available': x, 'hold': y}, ... }
            out: Dict[str, float] = {}
            for cur, v in (bals or {}).items():
                if cur == self.cfg.quote:
                    continue
                sym = f"{cur}-{self.cfg.quote}"
                out[sym] = float(v.get("available", 0.0))
            return out
        else:
            self.cur.execute("SELECT symbol, COALESCE(qty,0)::numeric, COALESCE(hold,0)::numeric FROM sim_positions")
            out: Dict[str, float] = {}
            for symbol, qty, hold in self.cur.fetchall():
                try:
                    avail = float(qty) - float(hold)
                except Exception:
                    avail = 0.0
                if avail > 0:
                    out[symbol] = avail
            return out

    def vet_inventory(self, proposal_id: int) -> str:
        """Phase 1: set invt_vet = 'approved' or 'denied' for SELL proposal."""
        prop = self._fetch_proposal(proposal_id)
        if not prop or prop["side"] != "sell":
            return "denied"

        ok = self._has_inventory(prop["symbol"], float(prop["size_intent"]))
        status = "approved" if ok else "denied"

        self.cur.execute("UPDATE proposals SET invt_vet = %s WHERE id = %s", (status, proposal_id))
        self.conn.commit()
        self._route_log(proposal_id, f"invt_vet.{status}", {"symbol": prop["symbol"], "size": float(prop["size_intent"])})
        return status

    def finalize_for_sell(self, proposal_id: int) -> bool:
        """
        Phase 2 for Petra-originated proposals: after Lamar sees all vets approved,
        he calls Helen to reserve inventory before signaling Petra.
        SIM: increment sim_positions.hold; LIVE: no DB hold (exchange will reserve at order).
        Returns True if reserved/validated OK.
        """
        prop = self._fetch_proposal(proposal_id, for_finalize=True)
        if not prop or prop["side"] != "sell":
            self._route_log(proposal_id, "invt_finalize.failed", {"reason": "not_found_or_wrong_side"})
            return False

        symbol = prop["symbol"]
        size = float(prop["size_intent"])

        if is_live_mode():
            # Re-validate availability on exchange
            if not self._has_inventory(symbol, size):
                self._route_log(proposal_id, "invt_finalize.denied", {"reason": "insufficient_live_bal"})
                return False
            # Optional: set status=approved here or let Lamar do it; we only log.
            self._route_log(proposal_id, "invt_finalize.live_ok", {"symbol": symbol, "size": size})
            return True

        # SIM mode: place a reservation by bumping sim_positions.hold atomically
        base, _ = parse_symbol(symbol)
        if not self._reserve_sim(symbol, size):
            self._route_log(proposal_id, "invt_finalize.denied", {"reason": "insufficient_sim_bal"})
            return False

        # Optional: status update to 'approved' can be Lamar's responsibility; keep idempotent if present.
        try:
            self.cur.execute("UPDATE proposals SET status='approved' WHERE id=%s", (proposal_id,))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
        self._route_log(proposal_id, "invt_finalize.approved", {"symbol": symbol, "size": size})
        return True

    # Called by Petra after successful order placement
    def link_asset_hold_to_order(self, hold_id: Optional[str] = None, order_id: Optional[str] = None,
                                 correlation_id: Optional[str] = None) -> None:
        """
        SIM: reservation was already made; just log the linkage.
        LIVE: exchange manages reservation; just log.
        Supports both legacy signature (hold_id, order_id) and correlation_id="proposal:{id}".
        """
        info = {"hold_id": hold_id, "order_id": order_id, "correlation_id": correlation_id}
        pid = self._pid_from_corr(correlation_id)
        if pid:
            self._route_log(pid, "invt_linked", info)
        else:
            logger.info("Helen link (no pid): %s", info)

    # Called by Petra on placement failure to unwind a SIM reservation
    def on_cancel(self, order_id: Optional[str] = None, reason: str = "placement_failed",
                  correlation_id: Optional[str] = None) -> None:
        pid = self._pid_from_corr(correlation_id)
        prop = self._fetch_proposal(pid) if pid else None
        if not prop or prop["side"] != "sell":
            self._route_log(pid or -1, "invt_cancel.ignored", {"reason": "no_sell_proposal", "order_id": order_id})
            return

        symbol = prop["symbol"]
        size = float(prop["size_intent"])

        if not is_live_mode():
            self._release_sim(symbol, size)

        # Mark proposal failed (idempotent)
        try:
            self.cur.execute("UPDATE proposals SET status='failed' WHERE id=%s", (pid,))
            self.conn.commit()
        except Exception:
            self.conn.rollback()

        self._route_log(pid, "invt_cancel", {"order_id": order_id, "reason": reason, "symbol": symbol, "size": size})

    # ───────────────────────── Internals ─────────────────────────

    def _fetch_proposal(self, pid: Optional[int], for_finalize: bool = False) -> Optional[Dict]:
        if not pid:
            return None
        if for_finalize:
            # ensure all vets are approved (risk/bank/invt)
            self.cur.execute("""
                SELECT id, symbol, side, price_intent, size_intent, risk_vet, bank_vet, invt_vet
                FROM proposals WHERE id=%s
            """, (pid,))
            row = self.cur.fetchone()
            if not row:
                return None
            d = dict(zip(
                ("id","symbol","side","price_intent","size_intent","risk_vet","bank_vet","invt_vet"), row
            ))
            if (d.get("risk_vet") != "approved") or (d.get("bank_vet") != "approved") or (d.get("invt_vet") != "approved"):
                return None
            return d
        else:
            self.cur.execute("""
                SELECT id, symbol, side, price_intent, size_intent
                FROM proposals WHERE id=%s
            """, (pid,))
            row = self.cur.fetchone()
            return dict(zip(("id","symbol","side","price_intent","size_intent"), row)) if row else None

    def _has_inventory(self, symbol: str, size: float) -> bool:
        """Check available inventory for SELL."""
        base, quote = parse_symbol(symbol)

        if is_live_mode():
            bals = self.client.get_account_balances_detailed()  # LIVE balances per currency
            have = float((bals.get(base) or {}).get("available", 0.0))
            return have >= size

        # SIM: available = qty - hold
        self.cur.execute("SELECT COALESCE(qty,0)::numeric, COALESCE(hold,0)::numeric FROM sim_positions WHERE symbol=%s", (symbol,))
        row = self.cur.fetchone()
        if not row:
            return False
        qty, hold = [float(x or 0.0) for x in row]
        return (qty - hold) >= size

    def _reserve_sim(self, symbol: str, size: float) -> bool:
        """Atomically reserve inventory in SIM by bumping hold; ensure no negative available."""
        # Try optimistic update guarded by availability check in one statement
        self.cur.execute("""
            UPDATE sim_positions
               SET hold = hold + %s
             WHERE symbol = %s
               AND (COALESCE(qty,0) - COALESCE(hold,0)) >= %s
            RETURNING hold
        """, (size, symbol, size))
        ok = self.cur.fetchone() is not None
        if ok:
            self.conn.commit()
        else:
            self.conn.rollback()
        return ok

    def _release_sim(self, symbol: str, size: float) -> None:
        """Release a previous reservation in SIM (best-effort; never go below zero)."""
        self.cur.execute("""
            UPDATE sim_positions
               SET hold = GREATEST(0, COALESCE(hold,0) - %s)
             WHERE symbol = %s
        """, (size, symbol))
        self.conn.commit()

    def _route_log(self, proposal_id: int, status: str, info: Dict) -> None:
        """Append to proposal_router_log for observability."""
        try:
            self.cur.execute("""
                INSERT INTO proposal_router_log (proposal_id, timestamp, status, details)
                VALUES (%s, NOW(), %s, %s)
            """, (proposal_id, status, json.dumps(info)))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning("router log insert failed p=%s: %s", proposal_id, e)

    @staticmethod
    def _pid_from_corr(correlation_id: Optional[str]) -> Optional[int]:
        """Parse 'proposal:{id}' → id."""
        if not correlation_id:
            return None
        s = str(correlation_id).strip()
        if s.startswith("proposal:"):
            try:
                return int(s.split(":", 1)[1])
            except Exception:
                return None
        return None
