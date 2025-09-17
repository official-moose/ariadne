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

# Import config parameters
from mm.config.marcus import (
    ORDER_REFRESH_SECONDS, STALE_ORDER_HOURS,
    MAX_SPREAD_DRIFT_PCT, PRICE_STAGNANT_MINUTES,
    ALERT_EMAIL_ENABLED, ALERT_EMAIL_ADDRESS,
    ALERT_EMAIL_RECIPIENT, QUOTE_CURRENCY
)

# Email imports (conditional)
if ALERT_EMAIL_ENABLED:
    import smtplib
    from email.mime.text import MIMEText
    from mm.utils.helpers.timezone import get_email_date

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
                    
            self._send_alert(f"Emergency cancelled {cancelled_count} orders")
            
        except Exception as e:
            self.logger.error(f"Failed to cancel all orders: {e}")
            self._send_alert(f"FAILED to cancel all orders: {e}")
            
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
    
    def _send_alert(self, message: str):
        """
        Send email alert
        
        Args:
            message: Alert message
        """
        if not ALERT_EMAIL_ENABLED:
            return
            
        try:
            msg = MIMEText(message)
            msg['Subject'] = f"❌ Termination Officer Alert - {get_email_date()}"
            msg['From'] = ALERT_EMAIL_ADDRESS
            msg['To'] = ALERT_EMAIL_RECIPIENT
            msg['Date'] = get_email_date()
            
            server = smtplib.SMTP_SSL(ALERT_EMAIL_SMTP_SERVER, ALERT_EMAIL_SMTP_PORT)
            server.login(ALERT_EMAIL_ADDRESS, ALERT_EMAIL_PASSWORD)
            server.sendmail(ALERT_EMAIL_ADDRESS, [ALERT_EMAIL_RECIPIENT], msg.as_string())
            server.quit()
            
            self.logger.info("Alert email sent")
        except Exception as e:
            self.logger.error(f"Failed to send alert email: {e}")
