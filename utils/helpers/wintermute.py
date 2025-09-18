#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250917.01
#===================================================================
# last update: 2025 | Sept. 17                  Production ready ✅
#===================================================================
# wintermute
# mm/utils/helpers/wintermute.py
#
# Central hub for all common helpers, methods, and facilitators
# The one source of truth for shared functionality
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✖ PERSISTANT RUNTIME  ✖ MONIT MANAGED
#===================================================================

from __future__ import annotations

import os
import ssl
import json
import signal
import smtplib
import logging
import hashlib
import uuid
import random
from dataclasses import dataclass
from decimal import Decimal, getcontext, ROUND_DOWN, ROUND_UP, InvalidOperation
from email.message import EmailMessage
from logging.handlers import RotatingFileHandler
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Protocol, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv

import psycopg2
import psycopg2.extras
import psycopg2.pool

# Explicitly load secrets file
dotenv_path = os.path.join(os.path.dirname(__file__), "../../data/secrets/.env")
load_dotenv(dotenv_path=dotenv_path)

# ──────────────────────────────────────────────────────────────────────────
# Global settings
# ──────────────────────────────────────────────────────────────────────────
DEFAULT_TZ = "America/Toronto"  # DST-aware local default
getcontext().prec = 28          # safe precision for crypto P&L/fees

# Database connection pool (lazy-initialized)
_db_pool = None

# 🔶 Logging =======================================================

import logging
import tqdm

class TqdmLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)

def init_logging(LOG_SELF=True, LOG_MAIN=True, SCREEN_OUT=True, LOGGER="Julius"):
    fmt = '%(asctime)s    [ %s ] [%%(levelname)s] %%(message)s' % LOGGER
    formatter = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger(LOGGER)
    logger.setLevel(logging.DEBUG)

    # 🔹 Output to the page's log ==================================
    
    if LOG_SELF and not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(f"{LOGGER.lower()}.log") for h in logger.handlers):
        fh = logging.FileHandler(f"mm/logs/{LOGGER.lower()}.log")
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)

    # 🔹 Output to the main log (ariadne.log) ======================
    
    if LOG_MAIN:
        ariadne_logger = logging.getLogger("ariadne")
        ariadne_logger.setLevel(logging.DEBUG)
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith("ariadne.log") for h in ariadne_logger.handlers):
            fh2 = logging.FileHandler("mm/logs/ariadne.log")
            fh2.setFormatter(formatter)
            fh2.setLevel(logging.DEBUG)
            ariadne_logger.addHandler(fh2)
    else:
        ariadne_logger = None

    # 🔹 Output to the screen ======================================
    
    if SCREEN_OUT and not any(isinstance(h, TqdmLogHandler) for h in logger.handlers):
        sh = TqdmLogHandler()
        sh.setFormatter(formatter)
        sh.setLevel(logging.DEBUG)
        logger.addHandler(sh)

    logger.propagate = False
    if LOG_MAIN and ariadne_logger:
        ariadne_logger.propagate = False

    return logger

    #🛑

# 🔶 Email Sender ==================================================

import os
import importlib
import smtplib
import ssl
import uuid
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

import mm.config.marcus as marcus

# Load .env (only once per process, safe here)
load_dotenv("mm/data/secrets/.env")

def send_email(subject: str, status: str, title: str, message: str, USERCODE: str = "ARI") -> str:
    """
    Send a styled HTML alert email via SMTP, using config/marcus and env for secrets.

    Params:
        subject:   Email subject
        status:    Status code/tag (for coloring: STATCON1, STATCON2, etc.)
        title:     Big bold message in the email
        message:   Main body text
        USERCODE:  3-letter user/app code (for env vars: ARI_USR, ARI_PWD, ARI_NAME)
    Returns:
        msg_id: Message-ID string (on success), or string error/"disabled" as fallback
    """

    importlib.reload(marcus)
    if not bool(getattr(marcus, "ALERT_EMAIL_ENABLED", False)):
        return "disabled"
    if str(getattr(marcus, "ALERT_EMAIL_ENCRYPT", "SSL")).upper() != "SSL":
        return "Simple Mail Transfer Protocol not established. No conn."

    host = getattr(marcus, "ALERT_EMAIL_SMTP_SERVER", None)
    port = getattr(marcus, "ALERT_EMAIL_SMTP_PORT", None)
    recipient = getattr(marcus, "ALERT_EMAIL_RECIPIENT", None)

    # Sender Info from env
    user = os.getenv(f"{USERCODE}_USR")
    pwd = os.getenv(f"{USERCODE}_PWD")
    sender_email = user
    sender_name = os.getenv(f"{USERCODE}_NAME", USERCODE)

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
    <tbody><tr style="font-family: Georgia, 'Times New Roman', Times, serif;font-size:20px;font-weight:600;background-color:#333;">
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
  </tbody></table>
  <table role="presentation" width="400px" height="20px" cellpadding="4" cellspacing="0" border="0" style="font-family: Tahoma, Geneva, sans-serif;">
    <tbody><tr style="background-color:#333;">
      <td colspan="2" style="color:#efefef;font-size:12px;font-weight:600;">DOCINT</td>
    </tr>
    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">SENT</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">→</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{sent_str}</td>
    </tr>
    <tr style="background-color:#F2F2F0;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">EPOCH</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">→</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{epoch_ms} (ms since 1970/01/01 0:00 UTC)</td>
    </tr>
    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">m.ID</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">→</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{mid_clean}</td>
    </tr>
  </tbody></table>
</div>
"""

    msg.add_alternative(html_body, subtype="html")
    ctx = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=10) as s:
            if user and pwd:
                s.login(user, pwd)
            s.send_message(msg)
        return msg_id
    except Exception as e:
        return f"email_failed: {e}"

    #🛑

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 1: TIME & TIMEZONE
# ══════════════════════════════════════════════════════════════════════════

def _tz(tz: Optional[str]) -> ZoneInfo:
    return ZoneInfo(tz or DEFAULT_TZ)

def _offset_colon(dt: datetime) -> str:
    # Convert -0400 -> -04:00 for DB-friendly formatting
    z = dt.strftime("%z")
    return f"{z[:3]}:{z[3:]}" if z and len(z) == 5 else z

@dataclass(frozen=True)
class TimePack:
    """
    Multi-format timestamp bundle at the *local* timezone (default America/Toronto).

    Fields:
      dt       : timezone-aware datetime (local tz)
      epoch_ms : integer milliseconds since Unix epoch
      iso      : ISO-8601 with local offset (e.g., 2025-09-02T01:23:45-04:00)
      human    : "YYYY-MM-DD HH:MM:SS America/Toronto"
      db       : "YYYY-MM-DD HH:MM:SS-04:00"
    """
    dt: datetime
    epoch_ms: int
    iso: str
    human: str
    db: str

def now_local(tz: Optional[str] = None) -> datetime:
    """Current time as timezone-aware datetime in the given tz (default: America/Toronto)."""
    return datetime.now(_tz(tz))

def to_pack(dt: Optional[datetime] = None, tz: Optional[str] = None) -> TimePack:
    """Build a TimePack from a datetime at local tz."""
    Z = _tz(tz)
    if dt is None:
        dt = datetime.now(Z)
    else:
        dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=Z)
        dt = dt.astimezone(Z)
    epoch_ms = int(dt.timestamp() * 1000)
    iso = dt.isoformat()
    human = f"{dt.strftime('%Y-%m-%d %H:%M:%S')} {Z.key}"
    db = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}{_offset_colon(dt)}"
    return TimePack(dt=dt, epoch_ms=epoch_ms, iso=iso, human=human, db=db)

def now_pack(tz: Optional[str] = None) -> TimePack:
    """Convenience wrapper returning a local TimePack (default America/Toronto)."""
    return to_pack(None, tz)

def parse_epoch_ms(epoch_ms: int, tz: Optional[str] = None) -> TimePack:
    """Convert epoch ms to a local TimePack (default America/Toronto)."""
    Z = _tz(tz)
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=Z)
    return to_pack(dt, tz)

def get_utc_timestamp() -> int:
    """Consistent UTC timestamp generation (seconds since epoch)."""
    return int(datetime.utcnow().timestamp())

def get_email_date() -> str:
    """RFC 2822 formatted date for email headers."""
    from email.utils import formatdate
    return formatdate(localtime=True)

def is_market_hours(symbol: str = None) -> bool:
    """Check if within trading hours (crypto trades 24/7, this is for future use)."""
    # For crypto, always True. For stocks, implement market hours logic
    return True

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 2: DECIMAL MATH & FORMATTING
# ══════════════════════════════════════════════════════════════════════════

def _coerce_str_no_sci(v: Any, dp: int) -> str:
    """Turn inputs into a fixed-point string with EXACT dp decimals and NO scientific notation."""
    if isinstance(v, Decimal):
        s = format(v, f"f")
    elif isinstance(v, int):
        s = f"{v:.{dp}f}"
    elif isinstance(v, float):
        s = f"{v:.{dp}f}"
    elif isinstance(v, str):
        if "e" in v.lower():
            raise ValueError(f"Scientific notation not allowed: {v!r}")
        try:
            d = Decimal(v)
        except InvalidOperation as e:
            raise ValueError(f"Invalid decimal string: {v!r}") from e
        s = format(d, f"f")
    else:
        s = format(Decimal(str(v)), f"f")
    d = Decimal(s)
    q = Decimal("1." + "0" * dp)
    return format(d.quantize(q, rounding=ROUND_DOWN), f"f")

def dec2(v: Any) -> Decimal:
    """Return Decimal with exactly 2 dp; no sci-notation accepted."""
    return Decimal(_coerce_str_no_sci(v, 2))

def dec8(v: Any) -> Decimal:
    """Return Decimal with exactly 8 dp; no sci-notation accepted."""
    return Decimal(_coerce_str_no_sci(v, 8))

def fmt2(d: Any) -> str:
    """String with exactly 2 dp, non-scientific."""
    return _coerce_str_no_sci(d, 2)

def fmt8(d: Any) -> str:
    """String with exactly 8 dp, non-scientific."""
    return _coerce_str_no_sci(d, 8)

def quantize_step(d: Decimal, step: Decimal, mode: str = "down") -> Decimal:
    """Round to tradable increment (tick/lot)."""
    d = Decimal(str(d)); step = Decimal(str(step))
    q = (ROUND_DOWN if mode == "down" else ROUND_UP)
    return (d / step).to_integral_value(rounding=q) * step

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 3: SYMBOL OPERATIONS
# ══════════════════════════════════════════════════════════════════════════

def parse_symbol(sym: str) -> Tuple[str, str]:
    """
    Parse symbol into (BASE, QUOTE) tuple.
    Accepts: 'BTC-USDT', 'BTC/USDT', 'btcusdt', 'BTC_USDT'
    This is the primary symbol parser - norm_symbol is an alias.
    """
    s = sym.strip().upper().replace("/", "-").replace("_", "-")
    if "-" in s:
        base, quote = s.split("-", 1)
        return base, quote
    # Try common quote currencies
    for q in ("USDT", "USDC", "BTC", "ETH", "USD", "EUR", "CAD"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    raise ValueError(f"Unrecognized symbol format: {sym!r}")

# Alias for compatibility
norm_symbol = parse_symbol

def format_pair(base: str, quote: str) -> str:
    """Format base/quote into standard symbol format (BASE-QUOTE)."""
    return f"{base.upper()}-{quote.upper()}"

# Alias for compatibility
join_symbol = format_pair

def validate_symbol(symbol: str, valid_symbols: Optional[List[str]] = None) -> bool:
    """
    Validate symbol format and optionally check against list of valid symbols.
    """
    try:
        base, quote = parse_symbol(symbol)
        if not base or not quote:
            return False
        if valid_symbols:
            formatted = format_pair(base, quote)
            return formatted in valid_symbols
        return True
    except ValueError:
        return False

def get_symbol_info(conn, symbol: str) -> Dict[str, Any]:
    """
    Get symbol trading rules from symbol_info table.
    Returns dict with base/quote increments, min sizes, etc.
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM symbol_info WHERE symbol = %s
        """, (symbol,))
        row = cur.fetchone()
        cur.close()
        if row:
            return dict(row)
        else:
            # Return defaults if not found
            return {
                'symbol': symbol,
                'base_increment': 0.00000001,
                'price_increment': 0.00000001, 
                'base_min_size': 0.00000001,
                'quote_min_size': 0.00001
            }
    except Exception as e:
        log.error(f"Error getting symbol info for {symbol}: {e}")
        return {}

def safe_filename(s: str) -> str:
    """Convert string to safe filename."""
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)

def sha256_hex(s: str) -> str:
    """SHA256 hash of string."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 4: PRICE CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════

def mid_price(bid: Decimal, ask: Decimal) -> Decimal:
    """Mid price = (bid + ask) / 2"""
    bid = Decimal(str(bid)); ask = Decimal(str(ask))
    return (bid + ask) / Decimal("2")

# Alias for compatibility
calculate_mid_price = mid_price

def microprice(bid: Decimal, ask: Decimal, bid_sz: Decimal, ask_sz: Decimal) -> Decimal:
    """Queue-depth weighted price leaning toward the thinner side."""
    bid = Decimal(str(bid)); ask = Decimal(str(ask))
    bid_sz = Decimal(str(bid_sz)); ask_sz = Decimal(str(ask_sz))
    denom = bid_sz + ask_sz
    return mid_price(bid, ask) if denom == 0 else (ask * bid_sz + bid * ask_sz) / denom

def calculate_spread(bid: float, ask: float) -> float:
    """
    Calculate bid-ask spread as percentage.
    Returns: (ask - bid) / mid * 100
    """
    if bid <= 0 or ask <= 0:
        return 0.0
    mid = (bid + ask) / 2
    return ((ask - bid) / mid) * 100 if mid > 0 else 0.0

def round_to_tick(price: float, tick_size: float) -> float:
    """Round price to valid tick increment."""
    if tick_size <= 0:
        return price
    return round(price / tick_size) * tick_size

def round_size(size: float, increment: float) -> float:
    """Round order size to valid increment."""
    if increment <= 0:
        return size
    return round(size / increment) * increment

def notional(qty: Decimal, price: Decimal) -> Decimal:
    """Notional = qty * price (quote-currency value)."""
    return Decimal(str(qty)) * Decimal(str(price))

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 5: FEE CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════

def apply_fee(amount: Decimal, rate: Decimal, coefficient: Decimal = Decimal("1")) -> Tuple[Decimal, Decimal]:
    """
    Apply fee with coefficient. Returns (fee, net_amount).
    """
    amount = Decimal(str(amount))
    eff = Decimal(str(rate)) * Decimal(str(coefficient))
    fee = (amount * eff).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    net = (amount - fee).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    return fee, net

def calculate_fees(symbol: str, size: float, price: float, side: str, 
                  maker_rate: float = 0.001, taker_rate: float = 0.001) -> float:
    """
    Calculate maker/taker fees for order.
    Returns fee amount in quote currency.
    """
    notional_value = size * price
    # Assume taker unless specified otherwise
    fee_rate = maker_rate if side == 'maker' else taker_rate
    return notional_value * fee_rate

def get_fee_rate(symbol: str, side: str, conn=None) -> float:
    """
    Get fee rate for symbol/side from database or default.
    """
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT maker_fee_rate, taker_fee_rate 
                FROM symbol_info 
                WHERE symbol = %s
            """, (symbol,))
            row = cur.fetchone()
            cur.close()
            if row:
                return float(row[0] if side == 'maker' else row[1])
        except:
            pass
    # Default rates
    return 0.001  # 0.1%

def breakeven(entry_price: Decimal, fee_rate: Decimal, coefficient: Decimal = Decimal("1")) -> Decimal:
    """Approx round-trip breakeven with fees on buy and sell."""
    entry = Decimal(str(entry_price))
    eff = Decimal(str(fee_rate)) * Decimal(str(coefficient))
    return entry * (Decimal("1") + (Decimal("2") * eff))

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 6: ORDER VALIDATION
# ══════════════════════════════════════════════════════════════════════════

def validate_min_order_size(symbol: str, size: float, conn=None) -> bool:
    """Check if order size meets minimum requirements."""
    if conn:
        info = get_symbol_info(conn, symbol)
        min_size = float(info.get('base_min_size', 0.00000001))
        return size >= min_size
    # Without connection, assume valid
    return size > 0

def validate_order_value(symbol: str, size: float, price: float, conn=None) -> bool:
    """Check if order meets minimum USDT value requirement."""
    notional_value = size * price
    if conn:
        info = get_symbol_info(conn, symbol)
        min_value = float(info.get('quote_min_size', 0.00001))
        return notional_value >= min_value
    # Default minimum
    return notional_value >= 0.00001

def count_open_orders(symbol: str, conn) -> int:
    """Count active orders for a symbol."""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM sim_orders 
            WHERE symbol = %s 
            AND status IN ('open', 'partial')
            AND COALESCE(deleted, FALSE) = FALSE
        """, (symbol,))
        count = cur.fetchone()[0]
        cur.close()
        return count
    except Exception as e:
        log.error(f"Error counting open orders for {symbol}: {e}")
        return 0

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 7: BALANCE OPERATIONS
# ══════════════════════════════════════════════════════════════════════════

def get_available_balance(currency: str, conn) -> float:
    """Get tradeable balance for currency."""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT available FROM sim_balances WHERE asset = %s
        """, (currency.upper(),))
        row = cur.fetchone()
        cur.close()
        return float(row[0]) if row else 0.0
    except Exception as e:
        log.error(f"Error getting balance for {currency}: {e}")
        return 0.0

def calculate_total_equity(conn, base_currency: str = 'USDT') -> float:
    """
    Sum portfolio value in base currency.
    Uses last prices from tickstick table.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT asset, available + hold as total FROM sim_balances
        """)
        balances = cur.fetchall()
        
        total_equity = 0.0
        for asset, balance in balances:
            if balance <= 0:
                continue
            if asset == base_currency:
                total_equity += balance
            else:
                # Get conversion rate
                symbol = f"{asset}-{base_currency}"
                cur.execute("""
                    SELECT last FROM tickstick 
                    WHERE symbol = %s 
                    ORDER BY timestamp DESC LIMIT 1
                """, (symbol,))
                price_row = cur.fetchone()
                if price_row:
                    total_equity += balance * float(price_row[0])
        
        cur.close()
        return total_equity
    except Exception as e:
        log.error(f"Error calculating total equity: {e}")
        return 0.0

def can_afford(currency: str, amount: float, conn) -> bool:
    """Check if sufficient balance for purchase."""
    available = get_available_balance(currency, conn)
    return available >= amount

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 8: DATABASE OPERATIONS
# ══════════════════════════════════════════════════════════════════════════

def get_db_connection() -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection to ariadne database.
    Uses connection pooling for efficiency.
    """
    global _db_pool
    if _db_pool is None:
        _db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,  # min 1, max 20 connections
            dbname="ariadne",
            user="postgres",
            host="localhost"
        )
    return _db_pool.getconn()

def release_db_connection(conn):
    """Return connection to pool."""
    global _db_pool
    if _db_pool:
        _db_pool.putconn(conn)

def execute_query(query: str, params: tuple = None, conn=None) -> list:
    """
    Safe query execution with error handling.
    Returns list of dict rows.
    """
    own_conn = False
    try:
        if conn is None:
            conn = get_db_connection()
            own_conn = True
        
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        
        # If SELECT query, fetch results
        if query.strip().upper().startswith('SELECT'):
            results = [dict(row) for row in cur.fetchall()]
        else:
            conn.commit()
            results = []
        
        cur.close()
        return results
    except Exception as e:
        log.error(f"Query execution failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if own_conn and conn:
            release_db_connection(conn)

def bulk_insert(table: str, data: list, columns: list = None, conn=None) -> bool:
    """
    Batch insert with rollback protection.
    data: list of tuples/lists to insert
    columns: column names (if not provided, assumes all columns)
    """
    own_conn = False
    try:
        if conn is None:
            conn = get_db_connection()
            own_conn = True
        
        cur = conn.cursor()
        
        if columns:
            placeholders = ','.join(['%s'] * len(columns))
            cols = ','.join(columns)
            query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        else:
            # Assumes data matches all columns in order
            placeholders = ','.join(['%s'] * len(data[0]))
            query = f"INSERT INTO {table} VALUES ({placeholders})"
        
        cur.executemany(query, data)
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        log.error(f"Bulk insert failed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if own_conn and conn:
            release_db_connection(conn)

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 9: RISK CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════

def calculate_position_size(capital: float, risk_pct: float, 
                           stop_loss_pct: float = None) -> float:
    """
    Kelly/fixed fractional position sizing.
    capital: total available capital
    risk_pct: percentage of capital to risk (e.g., 0.02 for 2%)
    stop_loss_pct: stop loss percentage (optional, for position sizing)
    """
    risk_amount = capital * risk_pct
    
    if stop_loss_pct and stop_loss_pct > 0:
        # Position size = risk_amount / stop_loss_pct
        return risk_amount / stop_loss_pct
    else:
        # Simple fixed fractional
        return risk_amount

def check_exposure_limits(symbol: str, size: float, conn, 
                         max_position_pct: float = 0.1) -> bool:
    """
    Validate against max exposure limits.
    max_position_pct: max percentage of portfolio in single position
    """
    total_equity = calculate_total_equity(conn)
    if total_equity <= 0:
        return False
    
    # Get current position if any
    base, quote = parse_symbol(symbol)
    current_balance = get_available_balance(base, conn)
    
    # Get price for position value calculation
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM tickstick 
            WHERE symbol = %s 
            ORDER BY timestamp DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        cur.close()
        
        if row:
            price = float(row[0])
            new_position_value = (current_balance + size) * price
            position_pct = new_position_value / total_equity
            return position_pct <= max_position_pct
    except:
        pass
    
    return True

def calculate_drawdown(peak: float, current: float) -> float:
    """
    Compute current drawdown percentage.
    Returns positive percentage (e.g., 10.5 for 10.5% drawdown)
    """
    if peak <= 0:
        return 0.0
    drawdown = ((peak - current) / peak) * 100
    return max(0.0, drawdown)

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 10: MARKET DATA & ANALYSIS
# ══════════════════════════════════════════════════════════════════════════

def get_latest_ticker(symbol: str, conn) -> dict:
    """
    Fetch most recent ticker data from tickstick table.
    Returns dict with bid, ask, last, volume, etc.
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM tickstick 
            WHERE symbol = %s 
            ORDER BY timestamp DESC 
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        cur.close()
        return dict(row) if row else {}
    except Exception as e:
        log.error(f"Error fetching ticker for {symbol}: {e}")
        return {}

def get_order_book_imbalance(symbol: str, conn) -> float:
    """
    Calculate bid/ask volume ratio.
    Returns: -1 to +1 (-1 = all ask pressure, +1 = all bid pressure)
    """
    ticker = get_latest_ticker(symbol, conn)
    if not ticker:
        return 0.0
    
    bid_size = float(ticker.get('best_bid_size', 0))
    ask_size = float(ticker.get('best_ask_size', 0))
    
    total_size = bid_size + ask_size
    if total_size == 0:
        return 0.0
    
    return (bid_size - ask_size) / total_size

def calculate_volatility(symbol: str, period: int, conn) -> float:
    """
    Historical volatility calculation over period minutes.
    Returns annualized volatility percentage.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(minutes=period)
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM tickstick 
            WHERE symbol = %s AND timestamp > %s 
            ORDER BY timestamp
        """, (symbol, cutoff))
        
        prices = [float(row[0]) for row in cur.fetchall()]
        cur.close()
        
        if len(prices) < 2:
            return 0.0
        
        # Calculate returns
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                ret = (prices[i] - prices[i-1]) / prices[i-1]
                returns.append(ret)
        
        if not returns:
            return 0.0
        
        # Standard deviation of returns
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        std_dev = variance ** 0.5
        
        # Annualize (crypto trades 24/7)
        periods_per_year = 365 * 24 * 60 / period
        annualized_vol = std_dev * (periods_per_year ** 0.5) * 100
        
        return annualized_vol
    except Exception as e:
        log.error(f"Error calculating volatility for {symbol}: {e}")
        return 0.0

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 11: STATE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════

def save_state(state: dict, filepath: str) -> bool:
    """
    Persist bot state to JSON file.
    Creates backup of existing file before overwriting.
    """
    try:
        path = Path(filepath)
        # Backup existing file
        if path.exists():
            backup_path = path.with_suffix('.json.bak')
            path.rename(backup_path)
        
        # Write new state
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2, default=str)
        
        log.info(f"State saved to {filepath}")
        return True
    except Exception as e:
        log.error(f"Failed to save state: {e}")
        # Restore backup if save failed
        backup_path = Path(filepath).with_suffix('.json.bak')
        if backup_path.exists():
            backup_path.rename(filepath)
        return False

def load_state(filepath: str) -> dict:
    """
    Restore bot state from JSON file.
    Returns empty dict if file doesn't exist or is corrupted.
    """
    try:
        with open(filepath, 'r') as f:
            state = json.load(f)
        log.info(f"State loaded from {filepath}")
        return state
    except FileNotFoundError:
        log.info(f"No state file found at {filepath}, starting fresh")
        return {}
    except json.JSONDecodeError as e:
        log.error(f"State file corrupted: {e}")
        # Try backup
        backup_path = Path(filepath).with_suffix('.json.bak')
        if backup_path.exists():
            try:
                with open(backup_path, 'r') as f:
                    state = json.load(f)
                log.info(f"State loaded from backup")
                return state
            except:
                pass
        return {}
    except Exception as e:
        log.error(f"Failed to load state: {e}")
        return {}

def update_heartbeat(process_name: str, conn=None) -> None:
    """
    Update process heartbeat in database.
    Creates heartbeat row if it doesn't exist.
    """
    own_conn = False
    try:
        if conn is None:
            conn = get_db_connection()
            own_conn = True
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO heartbeats (process_name, last_heartbeat, status, pid)
            VALUES (%s, NOW(), 'ok', %s)
            ON CONFLICT (process_name)
            DO UPDATE SET 
                last_heartbeat = NOW(),
                status = 'ok',
                pid = %s
        """, (process_name, os.getpid(), os.getpid()))
        
        conn.commit()
        cur.close()
    except Exception as e:
        log.error(f"Failed to update heartbeat for {process_name}: {e}")
        if conn:
            conn.rollback()
    finally:
        if own_conn and conn:
            release_db_connection(conn)

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 12: PROCESS MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════

def write_pid_file(filepath: str) -> None:
    """
    Create PID file for process monitoring.
    Used by Monit and other process managers.
    """
    try:
        # Ensure directory exists
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            f.write(str(os.getpid()))
        
        log.info(f"PID {os.getpid()} written to {filepath}")
    except Exception as e:
        log.error(f"Failed to write PID file: {e}")

def cleanup_pid_file(filepath: str) -> None:
    """Remove PID file on shutdown."""
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            log.info(f"PID file {filepath} removed")
    except Exception as e:
        log.error(f"Failed to remove PID file: {e}")

def check_process_alive(pid: int) -> bool:
    """
    Verify process is running via PID.
    Cross-platform compatible.
    """
    try:
        os.kill(pid, 0)  # Signal 0 = check if process exists
        return True
    except OSError:
        return False

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 13: RETRY & BACKOFF
# ══════════════════════════════════════════════════════════════════════════

def time_sleep(sec: float) -> None:
    import time as _t; _t.sleep(sec)

def retry(fn: Callable[[], Any],
          max_tries: int = 3,
          base_delay: float = 0.25,
          jitter: bool = True,
          exceptions: Tuple[type, ...] = (Exception,),
          logger: Optional[logging.Logger] = None) -> Any:
    """Simple exponential backoff with optional jitter."""
    logger = logger or log
    delay = base_delay
    for i in range(1, max_tries + 1):
        try:
            return fn()
        except exceptions as e:
            if i == max_tries:
                raise
            sleep_for = delay + (random.random() * delay if jitter else 0.0)
            logger.warning(f"retry {i}/{max_tries-1} after error: {e}; sleeping {sleep_for:.3f}s")
            time_sleep(sleep_for)
            delay *= 2.0

# ══════════════════════════════════════════════════════════════════════════
#  SECTION 14: TICKSTICK REPOSITORY (Alma's cache)
# ══════════════════════════════════════════════════════════════════════════

class TickstickRepo:
    """
    Read-only accessor to the latest ticker snapshot for a symbol from Alma's cache.
    Uses standard PostgreSQL connection to ariadne database.
    """
    def __init__(self):
        pass  # No initialization needed, connections created per query
    
    def _conn(self):
        """Standard PostgreSQL connection to ariadne."""
        return psycopg2.connect(dbname="ariadne", user="postgres", host="localhost")
    
    @staticmethod
    def _norm_row(d: Dict[str, Any]) -> Dict[str, Any]:
        # Collapse case/underscores for tolerant key access
        return {(k or "").lower().replace("_", ""): v for k, v in d.items()}
    
    def latest(self, symbol: str) -> Dict[str, Any]:
        """Get latest ticker data for symbol."""
        sym = symbol.upper()
        sql = """
            SELECT * FROM tickstick
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (sym,))
                row = cur.fetchone()
                if not row:
                    raise KeyError(f"No tickstick row for {sym}")
                return self._norm_row(dict(row))
        finally:
            conn.close()

