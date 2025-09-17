#>> A R I A N D E [v 6.1]
#>> last update: 2025 | Sept. 9                ❌ PRODUCTION READY
#>>
#>> Risk Manager
#>> mm/core/grayson.py  
#>>
#>> Analytical layer for market analysis and decision logic. 
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

# Inara mode helpers
try:
    from mm.utils.helpers.inara import current_mode, is_live_mode
except Exception:
    def current_mode() -> str:
        return getattr(marcus, "MODE", "simulation")
    def is_live_mode() -> bool:
        return current_mode() == "live"

DSN = os.getenv("PG_DSN", "dbname=ariadne user=postgres host=localhost")
logger = logging.getLogger("grayson")
logger.setLevel(logging.INFO)


@dataclass
class Caps:
    quote: str = getattr(marcus, "QUOTE_CURRENCY", "USDT")
    min_trade: float = getattr(marcus, "MIN_TRADE_SIZE", 10.0)
    max_per_pair: float = getattr(marcus, "MAX_EXPOSURE_PER_PAIR", 0.10)      # share of equity
    max_total: float = getattr(marcus, "MAX_TOTAL_EXPOSURE", 0.60)            # share of equity
    max_asset_pct: float = getattr(marcus, "MAX_ASSET_PCT", 0.10)             # share of portfolio per asset
    cap_margin: float = getattr(marcus, "CAP_MARGIN", 0.01)                   # tolerance band
    max_active_pairs: int = getattr(marcus, "MAX_ACTIVE_PAIRS", 10)
    daily_loss_limit: float = getattr(marcus, "DAILY_LOSS_LIMIT", 0.05)
    max_drawdown_pct: float = getattr(marcus, "MAX_DRAWDOWN_PCT", 0.10)


class RiskOps:
    def __init__(self, client: Optional[KucoinClient] = None, cfg: Caps = Caps()):
        self.cfg = cfg
        self.client = client or KucoinClient()
        self.conn = psycopg2.connect(DSN)
        self.cur = self.conn.cursor()

        # Sticky debug for last decision reason (handy in logs/UI)
        self.last_reason: str = ""
        logger.info("Grayson online | mode=%s | quote=%s", current_mode(), self.cfg.quote)

    # ─────────────────────────── External API ───────────────────────────

    def can_trade_pair(self, symbol: str, total_equity: float, *, side: str = "buy",
                       notional: Optional[float] = None) -> bool:
        """Quick gate used by selection/loop. Sets self.last_reason."""
        mode = current_mode()
        if not self._mode_allows(side):
            self.last_reason = f"mode_block:{mode}:{side}"
            return False

        # Notional unknown? Use small probe via last price × some min size (treated as pass-through).
        if notional is None:
            px = self._last_price(symbol)
            if px <= 0:
                self.last_reason = "no_price"
                return False
            # Minimalistic notional (won't be used for enforcement except min_trade check)
            notional = px * max(0.0, float(getattr(marcus, "MIN_TRADE_SIZE", self.cfg.min_trade)) / max(px, 1e-9))

        if not self._min_trade_ok(notional):
            self.last_reason = "min_trade"
            return False

        # Load exposures
        pair_exp, tot_exp, active_pairs = self._exposures(symbol)

        # Per-pair limit (include proposed notional)
        per_cap = (self.cfg.max_per_pair + self.cfg.cap_margin) * max(1e-9, total_equity)
        if pair_exp + notional > per_cap:
            self.last_reason = f"per_pair_cap:{pair_exp + notional:.2f}>{per_cap:.2f}"
            return False

        # Total exposure limit
        tot_cap = (self.cfg.max_total + self.cfg.cap_margin) * max(1e-9, total_equity)
        if tot_exp + notional > tot_cap:
            self.last_reason = f"total_cap:{tot_exp + notional:.2f}>{tot_cap:.2f}"
            return False

        # Max active pairs (BUY only increases count if position/order absent)
        if side.lower() == "buy" and active_pairs >= self.cfg.max_active_pairs and pair_exp <= 0:
            self.last_reason = "max_active_pairs"
            return False

        self.last_reason = "ok"
        return True

    def check_risk_limits(self, total_equity: float) -> Dict[str, str]:
        """
        Global guard called from main loop.
        Returns: {"trading_allowed": bool, "reason": str}
        """
        mode = current_mode()
        if mode in ("halted", "maintenance"):
            return {"trading_allowed": False, "reason": f"mode:{mode}"}

        # Daily loss / drawdown checks (best-effort, fail-open if no data)
        if not self._daily_loss_ok(total_equity):
            return {"trading_allowed": False, "reason": "daily_loss_limit"}
        if not self._drawdown_ok(total_equity):
            return {"trading_allowed": False, "reason": "max_drawdown"}

        return {"trading_allowed": True, "reason": "ok"}

    def vet_risk(self, proposal_id: int, total_equity: Optional[float] = None) -> str:
        """
        Phase-1 vetting for Lamar: sets proposals.risk_vet ← 'approved'|'denied'
        Side-aware: BUY/Sell both go through the same rules; mode gates applied.
        """
        prop = self._fetch_proposal(proposal_id)
        if not prop:
            return "denied"

        # Mode gating first
        if not self._mode_allows(prop["side"]):
            self._set_vet(proposal_id, "risk_vet", "denied")
            self._route_log(proposal_id, "risk.denied", {"reason": "mode_gate", "mode": current_mode(), "side": prop["side"]})
            return "denied"

        # Determine notional and equity
        px = float(prop["price_intent"])
        sz = float(prop["size_intent"])
        notional = px * sz
        eq = float(total_equity if total_equity is not None else getattr(marcus, "INITIAL_CAPITAL", 1000.0))

        ok = self.can_trade_pair(prop["symbol"], eq, side=prop["side"], notional=notional)
        status = "approved" if ok else "denied"
        self._set_vet(proposal_id, "risk_vet", status)
        self._route_log(proposal_id, f"risk.{status}", {
            "symbol": prop["symbol"], "side": prop["side"],
            "notional": notional, "reason": self.last_reason
        })
        return status

    # ─────────────────────────── Internals ────────────────────────────

    def _mode_allows(self, side: str) -> bool:
        side = side.lower()
        m = current_mode()
        if m in ("halted", "maintenance"):
            return False
        if m == "drain" and side == "buy":
            return False
        return True

    def _min_trade_ok(self, notional: float) -> bool:
        try:
            return float(notional) >= float(self.cfg.min_trade)
        except Exception:
            return False

    def _fetch_proposal(self, pid: int) -> Optional[Dict]:
        self.cur.execute("""
            SELECT id, symbol, side, price_intent, size_intent
            FROM proposals
            WHERE id = %s
        """, (pid,))
        row = self.cur.fetchone()
        if not row:
            return None
        return dict(zip(("id","symbol","side","price_intent","size_intent"), row))

    def _set_vet(self, pid: int, col: str, status: str) -> None:
        try:
            self.cur.execute(f"UPDATE proposals SET {col} = %s WHERE id = %s", (status, pid))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning("vet update failed (%s) p=%s: %s", col, pid, e)

    def _route_log(self, proposal_id: int, status: str, info: Dict) -> None:
        try:
            self.cur.execute("""
                INSERT INTO proposal_router_log (proposal_id, timestamp, status, details)
                VALUES (%s, NOW(), %s, %s)
            """, (proposal_id, status, json.dumps(info)))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.warning("router log insert failed p=%s: %s", proposal_id, e)

    # ───────────── Exposure accounting (SIM vs LIVE) ─────────────

    def _exposures(self, symbol: str) -> Tuple[float, float, int]:
        """
        Returns (pair_exposure, total_exposure, active_pairs_count) in QUOTE terms.
        Exposure = (long position notional + open BUY orders notional). SELL orders don't increase exposure.
        """
        # Positions
        if is_live_mode():
            pos_map = self._live_positions_notional()
            ord_pair, ord_total = self._open_buy_orders_notional(live=True, symbol=symbol)
        else:
            pos_map = self._sim_positions_notional()
            ord_pair, ord_total = self._open_buy_orders_notional(live=False, symbol=symbol)

        pair_exp = pos_map.get(symbol, 0.0) + ord_pair
        tot_exp = sum(pos_map.values()) + ord_total

        # Active pairs = symbols with positive (position notional OR open orders notional)
        active_pairs = set([s for s, v in pos_map.items() if v > 0.0])
        if ord_pair > 0:
            active_pairs.add(symbol)
        return (pair_exp, tot_exp, len(active_pairs))

    def _sim_positions_notional(self) -> Dict[str, float]:
        self.cur.execute("SELECT symbol, COALESCE(qty,0)::numeric FROM sim_positions")
        out: Dict[str, float] = {}
        rows = self.cur.fetchall() or []
        for sym, qty in rows:
            q = float(qty or 0.0)
            if q <= 0:
                continue
            px = self._last_price(sym)
            if px > 0:
                out[sym] = q * px
        return out

    def _live_positions_notional(self) -> Dict[str, float]:
        # Minimal assumption: positions table with (symbol, qty) or (symbol, quantity)
        try:
            self.cur.execute("SELECT symbol, COALESCE(qty,0)::numeric FROM positions")
        except Exception:
            try:
                self.conn.rollback()
                self.cur.execute("SELECT symbol, COALESCE(quantity,0)::numeric FROM positions")
            except Exception:
                self.conn.rollback()
                return {}
        out: Dict[str, float] = {}
        for sym, qty in (self.cur.fetchall() or []):
            q = float(qty or 0.0)
            if q <= 0:
                continue
            px = self._last_price(sym)
            if px > 0:
                out[sym] = q * px
        return out

    def _open_buy_orders_notional(self, live: bool, symbol: str) -> Tuple[float, float]:
        table = "orders" if live else "sim_orders"
        # status may be 'open' or 'active'; include both
        try:
            self.cur.execute(f"""
                SELECT symbol, COALESCE(price,0)::numeric, COALESCE(size,0)::numeric
                FROM {table}
                WHERE (status = 'open' OR status = 'active') AND side = 'buy'
            """)
        except Exception:
            self.conn.rollback()
            return (0.0, 0.0)

        pair_sum = 0.0
        tot_sum = 0.0
        for sym, price, size in (self.cur.fetchall() or []):
            n = float(price or 0.0) * float(size or 0.0)
            tot_sum += n
            if str(sym).upper() == str(symbol).upper():
                pair_sum += n
        return (pair_sum, tot_sum)

    # ─────────────── Global loss / drawdown checks ───────────────

    def _daily_loss_ok(self, total_equity: float) -> bool:
        """
        Best-effort: check simulated or logged PnL for the current day.
        If unavailable, allow trading (fail-open).
        """
        try:
            # Try a risk_metrics table for 'daily_return' or 'daily_pnl'
            self.cur.execute("""
                SELECT metric, value
                FROM risk_metrics
                WHERE metric IN ('daily_return','daily_pnl')
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            row = self.cur.fetchone()
            if not row:
                return True
            metric, value = row
            v = float(value or 0.0)
            if metric == "daily_return":
                return v >= -self.cfg.daily_loss_limit
            if metric == "daily_pnl":
                # Interpret as absolute loss cap against equity
                return (v / max(1e-9, total_equity)) >= -self.cfg.daily_loss_limit
            return True
        except Exception:
            self.conn.rollback()
            return True

    def _drawdown_ok(self, total_equity: float) -> bool:
        """
        Best-effort: check last recorded drawdown in risk_metrics or performance_metrics.
        Fail-open if not available.
        """
        try:
            self.cur.execute("""
                SELECT metric, value
                FROM risk_metrics
                WHERE metric IN ('max_drawdown','drawdown')
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            row = self.cur.fetchone()
            if not row:
                return True
            metric, value = row
            dd = float(value or 0.0)
            # If already a fraction (0..1) use directly; if looks like %, normalize
            if dd > 1.5:  # treat as percent
                dd = dd / 100.0
            return dd <= self.cfg.max_drawdown_pct + self.cfg.cap_margin
        except Exception:
            self.conn.rollback()
            return True

    # ───────────────────── Market pricing helpers ─────────────────────

    def _last_price(self, symbol: str) -> float:
        try:
            p = float(self.client.last_trade_price(symbol) or 0.0)
            if p > 0:
                return p
            # Fallback: mid from best bid/ask
            bid = float(self.client.best_bid_price(symbol) or 0.0)
            ask = float(self.client.best_ask_price(symbol) or 0.0)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2.0
            return 0.0
        except Exception:
            return 0.0
