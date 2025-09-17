#>> A R I A N D E v6
#>> last update: 2025 | Sept. 5
#>>
#>> CANCELLATION MANAGER
#>> mm/core/alec.py
#>>
#>> Responsible for cancelling orders  
#>> Monitors for stale orders and stagnant pricing    
#>> Enforces compliance with configuration parameters
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]      
#>>────────────────────────────────────────────────────────────────

# Build|20250905.01

import logging
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import os
import importlib
import smtplib
import ssl
import uuid
from email.message import EmailMessage
from email.utils import formataddr
from zoneinfo import ZoneInfo

# third-party imports
from dotenv import load_dotenv

# local application imports
import mm.config.marcus as marcus

# load env for this process
load_dotenv("mm/data/secrets/.env")

# Import config parameters
from mm.config.marcus import (
    ORDER_REFRESH_SECONDS, STALE_ORDER_HOURS,
    MAX_SPREAD_DRIFT_PCT, PRICE_STAGNANT_MINUTES,
    QUOTE_CURRENCY
)

# ── Logger Setup ──────────────────────────────────────────────────────
logger = logging.getLogger('ariadne.termination')

class Alec:
    """
    Manages order cancellations based on various criteria
    """
    
    def __init__(self, trading_client):
        """
        Initialize with trading client reference
        
        Args:
            trading_client: Either KucoinClient or SimClient
        """
        self.client = trading_client
        self.logger = logger
        self.order_history: Dict[str, Dict] = {}  # order_id -> order details
        self.price_history: Dict[str, List[Tuple[float, float]]] = {}  # symbol -> [(timestamp, price)]
        self.cancellation_stats: Dict[str, int] = {
            'stale': 0,
            'stagnant': 0,
            'drift': 0,
            'risk': 0,
            'manual': 0
        }
        
    def send_email(subject: str, status: str, title: str, message: str) -> str:

        importlib.reload(marcus)
        if not bool(getattr(marcus, "ALERT_EMAIL_ENABLED", False)):
            return "disabled"
        if str(getattr(marcus, "ALERT_EMAIL_ENCRYPT", "SSL")).upper() != "SSL":
            return "Simple Mail Transfer Protocol not established. No conn."

        host = getattr(marcus, "ALERT_EMAIL_SMTP_SERVER", None)
        port = getattr(marcus, "ALERT_EMAIL_SMTP_PORT", None)
        recipient = getattr(marcus, "ALERT_EMAIL_RECIPIENT", None)

        USERCODE = "ALE"  # hardcode per file

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
        <tbody><tr style="font-family: Georgia, 'Times New Roman', Times, serif;font-size:20px;font-weight:600;background-color:#333;">
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
        <tr width="100%" height="25px"><td colspan="2"> </td></tr>
      </tbody></table>

      <!-- Footer -->
      <table role="presentation" width="400px" height="20px" cellpadding="4" cellspacing="0" border="0" style="font-family: Tahoma, Geneva, sans-serif;">
        <!-- DOCINT -->
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
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=10) as s:
            if user and pwd:
                s.login(user, pwd)
            s.send_message(msg)

        return msg_id
    
    def cancel_stale_orders(self) -> List[Tuple[str, str]]:
        """
        Cancel orders that are too old or need repricing
        
        Returns:
            List of (order_id, reason) tuples for cancelled orders
        """
        cancelled = []
        
        try:
            # Get all active orders
            all_orders = self.client.get_orders(status='active')
            
            for order in all_orders:
                order_id = order.get('id')
                symbol = order.get('symbol')
                
                # Check various cancellation criteria
                cancel_reason = self._should_cancel_order(order)
                
                if cancel_reason:
                    success = self._cancel_order(order_id, symbol)
                    if success:
                        cancelled.append((order_id, cancel_reason))
                        self.cancellation_stats[self._categorize_reason(cancel_reason)] += 1
                        
                # Update order tracking
                self.order_history[order_id] = {
                    'symbol': symbol,
                    'side': order.get('side'),
                    'price': float(order.get('price', 0)),
                    'size': float(order.get('size', 0)),
                    'created_at': order.get('createdAt', time.time()),
                    'last_checked': time.time()
                }
                
        except Exception as e:
            self.logger.error(f"Failed to check stale orders: {e}")
            
        if cancelled:
            self.logger.info(f"Cancelled {len(cancelled)} stale orders")
            
        return cancelled
    
    def cancel_all_orders(self) -> int:
        """
        Emergency cancel all open orders
        
        Returns:
            Number of orders cancelled
            
        This entire function is useless. It requires a manual trigger by me, which i dont have. Futher, there is no point in
        sending myself an alert email for something i have to manually do. Fuck Ai. I dont have time to fix this now, but seeing as
        I am the trigger, I can leave it in with no risk. Hence this note. I'll deal with it later.
        """
        cancelled_count = 0
        
        try:
            all_orders = self.client.get_orders(status='active')
            
            self.logger.warning(f"EMERGENCY: Cancelling all {len(all_orders)} open orders")
            
            for order in all_orders:
                order_id = order.get('id')
                symbol = order.get('symbol')
                
                if self._cancel_order(order_id, symbol):
                    cancelled_count += 1
                    
            try:
                send_email(
                    subject="[ STATCON3 ] Alma executed a corrective exit.",
                    status="STATCON3",
                    title="API Connection to KuCoin Failed",
                    message=f"<p><b>Alma was unable to fetch data from KuCoin via the API, the reported error was:</b><br><i>{e}</i></p><p>This exit was coded in to prevent stalling, infinite loops, and other outcomes that prevent Monit from knowing Alma is stuck. Monit <b><i>should</i></b> restart Alma.</p><p>Please ensure that this is the case by logging onto the server and using the command:<br><i>sudo monit status alma</i></p>",
                )
            except:
                pass
            
        except Exception as e:
            self.logger.error(f"Failed to cancel all orders: {e}")
            try:
                send_email(
                    subject="[ STATCON1 ] Alma executed a corrective exit.",
                    status="STATCON1",
                    title="API Connection to KuCoin Failed",
                    message=f"<p><b>Alma was unable to fetch data from KuCoin via the API, the reported error was:</b><br><i>{e}</i></p><p>This exit was coded in to prevent stalling, infinite loops, and other outcomes that prevent Monit from knowing Alma is stuck. Monit <b><i>should</i></b> restart Alma.</p><p>Please ensure that this is the case by logging onto the server and using the command:<br><i>sudo monit status alma</i></p>",
                )
            except:
                pass
            
        return cancelled_count
    
    def cancel_orders_for_pair(self, symbol: str, reason: str = "Manual request") -> int:
        """
        Cancel all orders for a specific trading pair
        
        Args:
            symbol: Trading pair symbol
            reason: Cancellation reason
            
        Returns:
            Number of orders cancelled
        """
        cancelled_count = 0
        
        try:
            orders = self.client.get_orders(symbol=symbol, status='active')
            
            for order in orders:
                order_id = order.get('id')
                if self._cancel_order(order_id, symbol):
                    cancelled_count += 1
                    
            if cancelled_count > 0:
                self.logger.info(f"Cancelled {cancelled_count} orders for {symbol}: {reason}")
                
        except Exception as e:
            self.logger.error(f"Failed to cancel orders for {symbol}: {e}")
            
        return cancelled_count
    
    def cleanup_old_positions(self, positions: Dict[str, float], 
                            hold_hours: int = 24) -> List[str]:
        """
        Identify positions held too long
        
        Args:
            positions: Current positions
            hold_hours: Maximum hold time
            
        Returns:
            List of symbols that should be liquidated
        """
        old_positions = []
        
        # This would need integration with trade history
        # For now, returning empty list
        # TODO: Track position entry times
        
        return old_positions
    
    def _should_cancel_order(self, order: Dict) -> Optional[str]:
        """
        Determine if an order should be cancelled
        
        Args:
            order: Order details
            
        Returns:
            Cancellation reason or None
        """
        order_id = order.get('id')
        symbol = order.get('symbol')
        created_at = order.get('createdAt', 0)
        price = float(order.get('price', 0))
        side = order.get('side')
        
        # Check if order is too old
        if created_at > 0:
            age_seconds = time.time() - (created_at / 1000)  # Convert ms to seconds

            # Refresh threshold (short-term)
            if age_seconds > ORDER_REFRESH_SECONDS:
                return f"stale - {age_seconds/3600:.1f} hours old"

            # Absolute cutoff (long-term)
            if age_seconds > (STALE_ORDER_HOURS * 3600):
                return f"stale - exceeded {STALE_ORDER_HOURS} hour limit"
        
        # Check for price drift
        current_market = self._get_current_market(symbol)
        if current_market:
            mid_price = (current_market['bid'] + current_market['ask']) / 2
            
            if mid_price > 0:
                if side == 'buy':
                    # Buy order should be below mid
                    expected_price = mid_price * 0.995  # Rough estimate
                    drift = abs(price - expected_price) / expected_price
                else:
                    # Sell order should be above mid
                    expected_price = mid_price * 1.005
                    drift = abs(price - expected_price) / expected_price
                
                if drift > MAX_SPREAD_DRIFT_PCT:
                    return f"drift - {drift:.1%} from optimal"
        
        # Check for stagnant prices
        if self._is_price_stagnant(symbol):
            return "stagnant - price not moving"
        
        return None
    
    def _cancel_order(self, order_id: str, symbol: str) -> bool:
        """
        Execute order cancellation
        
        Args:
            order_id: Order ID to cancel
            symbol: Trading pair symbol
            
        Returns:
            True if successful
        """
        try:
            self.client.cancel_order(order_id)
            self.logger.debug(f"Cancelled order {order_id} for {symbol}")
            
            # Remove from tracking
            if order_id in self.order_history:
                del self.order_history[order_id]
                
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    def _get_current_market(self, symbol: str) -> Optional[Dict]:
        """
        Get current market data for symbol
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Market data dict or None
        """
        try:
            tickers = self.client.get_ticker()
            for ticker in tickers:
                if ticker.get('symbol') == symbol:
                    return {
                        'bid': float(ticker.get('buy', 0)),
                        'ask': float(ticker.get('sell', 0)),
                        'last': float(ticker.get('last', 0))
                    }
        except Exception as e:
            self.logger.error(f"Failed to get market data: {e}")
            
        return None
    
    def _is_price_stagnant(self, symbol: str) -> bool:
        """
        Check if price has been stagnant
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            True if price is stagnant
        """
        if symbol not in self.price_history:
            return False
            
        history = self.price_history[symbol]
        if len(history) < 10:
            return False
            
        # Check price movement over last N minutes
        cutoff = time.time() - (PRICE_STAGNANT_MINUTES * 60)
        recent_prices = [price for ts, price in history if ts > cutoff]
        
        if len(recent_prices) < 2:
            return False
            
        # Calculate price range
        price_range = max(recent_prices) - min(recent_prices)
        avg_price = sum(recent_prices) / len(recent_prices)
        
        if avg_price > 0:
            volatility = price_range / avg_price
            # Consider stagnant if less than 0.1% movement
            return volatility < 0.001
            
        return False
    
    def update_price_tracking(self, symbol: str, price: float):
        """
        Update price history for stagnation detection
        
        Args:
            symbol: Trading pair symbol
            price: Current price
        """
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            
        self.price_history[symbol].append((time.time(), price))
        
        # Keep only last hour
        cutoff = time.time() - 3600
        self.price_history[symbol] = [
            (ts, p) for ts, p in self.price_history[symbol] if ts > cutoff
        ]
    
    def get_cancellation_report(self) -> Dict:
        """
        Get cancellation statistics
        
        Returns:
            Dict with cancellation metrics
        """
        total_cancellations = sum(self.cancellation_stats.values())
        
        return {
            'total_cancellations': total_cancellations,
            'by_reason': self.cancellation_stats.copy(),
            'tracked_orders': len(self.order_history),
            'monitored_symbols': len(self.price_history),
            'oldest_order_age': self._get_oldest_order_age()
        }
    
    def _get_oldest_order_age(self) -> float:
        """Get age of oldest tracked order in hours"""
        if not self.order_history:
            return 0
            
        oldest = min(o['created_at'] for o in self.order_history.values())
        return (time.time() - oldest) / 3600
    
    def _categorize_reason(self, reason: str) -> str:
        """Categorize cancellation reason for stats"""
        if 'stale' in reason:
            return 'stale'
        elif 'drift' in reason:
            return 'drift'
        elif 'stagnant' in reason:
            return 'stagnant'
        elif 'risk' in reason.lower():
            return 'risk'
        else:
            return 'manual'