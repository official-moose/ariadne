#>> A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 9                ❌ PRODUCTION READY
#>>
#>> The Banker
#>> mm/core/julius.py
#>>
#>> The single point of truth for cash/wallet state inside 
#>> the system.
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

# Inara (mode gating). Safe fallbacks if helpers are older.
try:
    from mm.utils.helpers.inara import current_mode, is_live_mode
except Exception:
    def current_mode() -> str:
        return getattr(marcus, "MODE", "simulation")
    def is_live_mode() -> bool:
        return current_mode() == "live"

DSN = os.getenv("PG_DSN", "dbname=ariadne user=postgres host=localhost")

logger = logging.getLogger("julius")
logger.setLevel(logging.INFO)


@dataclass
class JuliusCfg:
    quote: str = getattr(marcus, "QUOTE_CURRENCY", "USDT")
    min_trade: float = getattr(marcus, "MIN_TRADE_SIZE", 10.0)


class Julius:
    """
    Responsibilities
      • Phase 1 (vet): set proposals.bank_vet ← 'approved' | 'denied' based on QUOTE wallet availability.
      • Phase 2 (finalize for BUY):
          - SIM: reserve by incrementing sim_balances.hold for QUOTE.
          - LIVE: no DB hold (exchange will reserve at order); optional recheck.
          - Set proposals.status = 'approved' (idempotent) and log.
      • Interfaces for Malcolm:
          - link_hold_to_order(order_id, correlation_id="proposal:{id}") → log linkage (SIM reserve already done).
          - on_cancel(reason, correlation_id="proposal:{id}") → release SIM reservation + mark proposal failed (idempotent).
      • get_balances(): unified balances for UI/loop (LIVE via exchange; SIM via sim_balances).
    """

    def __init__(self, client: Optional[KucoinClient] = None, cfg: JuliusCfg = JuliusCfg()):
        self.cfg = cfg
        self.client = client or KucoinClient()
        self.conn = psycopg2.connect(DSN)
        self.cur = self.conn.cursor()
        logger.info("Julius ready | mode=%s | quote=%s", current_mode(), self.cfg.quote)

    # ───────────────────────── Public API ─────────────────────────

    def get_balances(self) -> Dict[str, Dict[str, float]]:
        """
        Returns { 'USDT': {'available': x, 'hold': y, 'total': z}, ... }.
        LIVE → KuCoin accounts; SIM → sim_balances (best-effort across common schemas).
        """
        if is_live_mode():
            return self._live_balances()
        return self._sim_balances()

    def vet_bank(self, proposal_id: int) -> str:
        """Phase 1: set bank_vet = 'approved' or 'denied' for BUY proposal."""
        prop = self._fetch_proposal(proposal_id)
        if not prop or prop["side"] != "buy":
            return "denied"

        notional = float(prop["price_intent"]) * float(prop["size_intent"])
        if notional < float(self.cfg.min_trade):
            self._set_vet(proposal_id, "bank_vet", "denied")
            self._route_log(proposal_id, "bank.denied", {"reason": "min_trade"})
            return "denied"

        ok = self._has_funds(notional)
        status = "approved" if ok else "denied"
        self._set_vet(proposal_id, "bank_vet", status)
        self._route_log(proposal_id, f"bank.{status}", {"notional": notional, "quote": self.cfg.quote})
        return status

    def finalize_for_buy(self, proposal_id: int) -> bool:
        """
        Phase 2 for Malcolm-originated proposals: after Lamar sees all vets approved,
        Lamar calls Julius to reserve funds before signaling Malcolm.
        SIM: increment sim_balances.hold atomically; LIVE: re-validate only.
        """
        prop = self._fetch_proposal(proposal_id, for_finalize=True)
        if not prop or prop["side"] != "buy":
            self._route_log(proposal_id, "bank_finalize.failed", {"reason": "not_found_or_wrong_side"})
            return False

        notional = float(prop["price_intent"]) * float(prop["size_intent"])

        if is_live_mode():
            if not self._has_funds(notional):
                self._route_log(proposal_id, "bank_finalize.denied", {"reason": "insufficient_live_funds"})
                return False
            # Approved in LIVE (exchange will reserve later)
            self._safe_set_status(proposal_id, "approved")
            self._route_log(proposal_id, "bank_finalize.live_ok", {"notional": notional, "quote": self.cfg.quote})
            return True

        # SIM: place a reservation by bumping sim_balances.hold atomically
        if not self._reserve_sim_funds(notional):
            self._route_log(proposal_id, "bank_finalize.denied", {"reason": "insufficient_sim_funds"})
            return False

        self._safe_set_status(proposal_id, "approved")
        self._route_log(proposal_id, "bank_finalize.approved", {"notional": notional, "quote": self.cfg.quote})
        return True

    # Called by Malcolm after successful order placement
    def link_hold_to_order(self, order_id: Optional[str] = None,
                           correlation_id: Optional[str] = None,
                           hold_id: Optional[str] = None) -> None:
        """
        SIM: reservation already made; just log linkage.
        LIVE: exchange manages reservation; just log.
        """
        pid = self._pid_from_corr(correlation_id)
        info = {"order_id": order_id, "hold_id": hold_id, "correlation_id": correlation_id}
        if pid:
            self._route_log(pid, "bank_linked", info)
        else:
            logger.info("Julius link (no pid): %s", info)

    # Called by Malcolm on placement failure to unwind a SIM reservation
    def on_cancel(self, order_id: Optional[str] = None, reason: str = "placement_failed",
                  correlation_id: Optional[str] = None, hold_id: Optional[str] = None) -> None:
        pid = self._pid_from_corr(correlation_id)
        prop = self._fetch_proposal(pid) if pid else None
        if not prop or prop["side"] != "buy":
            self._route_log(pid or -1, "bank_cancel.ignored", {"reason": "no_buy_proposal", "order_id": order_id})
            return

        notional = float(prop["price_intent"]) * float(prop["size_intent"])
        if not is_live_mode():
            self._release_sim_funds(notional)

        # Mark proposal failed (idempotent)
        self._safe_set_status(pid, "failed")
        self._route_log(pid, "bank_cancel", {
            "order_id": order_id, "reason": reason, "notional": notional, "quote": self.cfg.quote
        })

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

    def _set_vet(self, pid: int, col: str, status: str) -> None:
        try:
            self.cur.execute(f"UPDATE proposals SET {col} = %s WHERE id = %s", (status, pid))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning("vet update failed (%s) p=%s: %s", col, pid, e)

    def _safe_set_status(self, pid: int, status: str) -> None:
        try:
            self.cur.execute("UPDATE proposals SET status=%s WHERE id=%s", (status, pid))
            self.conn.commit()
        except Exception:
            self.conn.rollback()

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

    # ─────────────── Balances / availability helpers ───────────────

    def _has_funds(self, notional: float) -> bool:
        bals = self.get_balances() or {}
        q = bals.get(self.cfg.quote, {})
        avail = float(q.get("available", 0.0))
        return avail >= float(notional)

    def _live_balances(self) -> Dict[str, Dict[str, float]]:
        # { 'USDT': {'available': x, 'hold': y, 'total': x+y}, ... }
        try:
            d = self.client.get_account_balances_detailed()  # KuCoin private accounts
        except Exception:
            d = {}
        out: Dict[str, Dict[str, float]] = {}
        for cur, vals in (d or {}).items():
            avail = float(vals.get("available", 0.0) or 0.0)
            hold  = float(vals.get("hold", 0.0) or 0.0)
            out[cur] = {"available": avail, "hold": hold, "total": avail + hold}
        return out

    def _sim_balances(self) -> Dict[str, Dict[str, float]]:
        """
        Best-effort across common sim_balances schemas:
          (asset, available, hold)   OR (asset, total, hold)   OR (asset, balance, hold)
        """
        # Try #1: available+hold
        try:
            self.cur.execute("SELECT asset, COALESCE(available,0)::numeric, COALESCE(hold,0)::numeric FROM sim_balances")
            rows = self.cur.fetchall()
            out = {}
            for asset, avail, hold in rows:
                a = float(avail or 0.0); h = float(hold or 0.0)
                out[str(asset).upper()] = {"available": a, "hold": h, "total": a + h}
            return out
        except Exception:
            self.conn.rollback()

        # Try #2: total+hold
        try:
            self.cur.execute("SELECT asset, COALESCE(total,0)::numeric, COALESCE(hold,0)::numeric FROM sim_balances")
            rows = self.cur.fetchall()
            out = {}
            for asset, total, hold in rows:
                t = float(total or 0.0); h = float(hold or 0.0)
                out[str(asset).upper()] = {"available": max(0.0, t - h), "hold": h, "total": t}
            return out
        except Exception:
            self.conn.rollback()

        # Try #3: balance+hold
        try:
            self.cur.execute("SELECT asset, COALESCE(balance,0)::numeric, COALESCE(hold,0)::numeric FROM sim_balances")
            rows = self.cur.fetchall()
            out = {}
            for asset, bal, hold in rows:
                t = float(bal or 0.0); h = float(hold or 0.0)
                out[str(asset).upper()] = {"available": max(0.0, t - h), "hold": h, "total": t}
            return out
        except Exception:
            self.conn.rollback()
            return {}

    def _reserve_sim_funds(self, notional: float) -> bool:
        """
        Atomically bump sim_balances.hold for QUOTE if sufficient available remains.
        Tries common schemas in order.
        """
        q = self.cfg.quote

        # available+hold
        try:
            self.cur.execute("""
                UPDATE sim_balances
                   SET hold = hold + %s
                 WHERE asset = %s
                   AND (COALESCE(available,0) - COALESCE(hold,0)) >= %s
                RETURNING hold
            """, (notional, q, notional))
            if self.cur.fetchone():
                self.conn.commit()
                return True
            self.conn.rollback()
        except Exception:
            self.conn.rollback()

        # total+hold
        try:
            self.cur.execute("""
                UPDATE sim_balances
                   SET hold = hold + %s
                 WHERE asset = %s
                   AND (COALESCE(total,0) - COALESCE(hold,0)) >= %s
                RETURNING hold
            """, (notional, q, notional))
            if self.cur.fetchone():
                self.conn.commit()
                return True
            self.conn.rollback()
        except Exception:
            self.conn.rollback()

        # balance+hold
        try:
            self.cur.execute("""
                UPDATE sim_balances
                   SET hold = hold + %s
                 WHERE asset = %s
                   AND (COALESCE(balance,0) - COALESCE(hold,0)) >= %s
                RETURNING hold
            """, (notional, q, notional))
            if self.cur.fetchone():
                self.conn.commit()
                return True
            self.conn.rollback()
        except Exception:
            self.conn.rollback()

        return False

    def _release_sim_funds(self, notional: float) -> None:
        """Release a previous reservation in SIM (best-effort; never go below zero)."""
        q = self.cfg.quote
        try:
            self.cur.execute("""
                UPDATE sim_balances
                   SET hold = GREATEST(0, COALESCE(hold,0) - %s)
                 WHERE asset = %s
            """, (notional, q))
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    # ───────────────────────── Utilities ─────────────────────────

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
