#>> A R I A N D E v6
#>> last update: 2025 | Sept. 5
#>>
#>> Panic Manager
#>> mm/core/naomi.py
#>>
#>> Emergency response system for market crises
#>> Monitors for crashes, illiquidity, and extreme volatility.
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]    
#>>----------------------------------------------------------------

# Build|20250905.01

import logging
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

# Import config parameters
from mm.config.marcus import (
    PANIC_PRICE_MOVE_PCT, PANIC_VOLUME_SPIKE,
    PANIC_LOSS_THRESHOLD, PANIC_POSITION_LOSS_PCT,
    CONSECUTIVE_LOSS_LIMIT, QUOTE_CURRENCY,
    ALERT_EMAIL_ENABLED, ALERT_EMAIL_ADDRESS, 
    ALERT_EMAIL_RECIPIENT
)

# Email imports (conditional)
if ALERT_EMAIL_ENABLED:
    import smtplib
    from email.mime.text import MIMEText
    from mm.utils.helpers.timezone import get_email_date

# ── Logger Setup ──────────────────────────────────────────────────────
logger = logging.getLogger('ariadne.panic')

class Naomi:
    """
    Monitors for panic conditions and triggers emergency procedures
    """
    
    def __init__(self):
        """Initialize panic monitoring structures"""
        self.logger = logger
        self.price_history: Dict[str, List[Tuple[float, float]]] = {}  # symbol -> [(timestamp, price)]
        self.volume_history: Dict[str, List[Tuple[float, float]]] = {}  # symbol -> [(timestamp, volume)]
        self.consecutive_losses: int = 0
        self.last_check_time: float = 0
        self.panic_events: List[Dict] = []
        self.cooldown_until: Optional[float] = None
        
    def check_panic_conditions(self, positions: Dict[str, float], 
                             total_equity: float) -> Dict:
        """
        Check all panic conditions
        
        Args:
            positions: Current positions by symbol
            total_equity: Total account equity
            
        Returns:
            Dict with panic status and recommended actions
        """
        # Check cooldown
        if self.cooldown_until and time.time() < self.cooldown_until:
            return {
                'panic_mode': False,
                'in_cooldown': True,
                'cooldown_remaining': self.cooldown_until - time.time()
            }
        
        panic_triggers = []
        
        # Check for flash crash/spike in any position
        for symbol in positions:
            price_spike = self._check_price_spike(symbol)
            if price_spike:
                panic_triggers.append({
                    'type': 'price_spike',
                    'symbol': symbol,
                    'severity': 'critical',
                    'details': price_spike
                })
        
        # Check for abnormal volume
        volume_anomalies = self._check_volume_anomalies(positions.keys())
        if volume_anomalies:
            panic_triggers.extend(volume_anomalies)
        
        # Check position-specific losses
        position_panics = self._check_position_losses(positions, total_equity)
        if position_panics:
            panic_triggers.extend(position_panics)
        
        # Check consecutive losses
        if self.consecutive_losses >= CONSECUTIVE_LOSS_LIMIT:
            panic_triggers.append({
                'type': 'consecutive_losses',
                'severity': 'high',
                'count': self.consecutive_losses,
                'details': f'{self.consecutive_losses} consecutive losing trades'
            })
        
        # Determine action based on triggers
        if not panic_triggers:
            return {
                'panic_mode': False,
                'triggers': [],
                'reason': None
            }
        
        # Classify severity
        critical_count = sum(1 for t in panic_triggers if t['severity'] == 'critical')
        high_count = sum(1 for t in panic_triggers if t['severity'] == 'high')
        
        # Determine action
        if critical_count > 0:
            action = 'close_all'
            close_positions = True
        elif high_count >= 2:
            action = 'cancel_orders'
            close_positions = False
        else:
            action = 'monitor'
            close_positions = False
        
        # Log panic event
        panic_event = {
            'timestamp': time.time(),
            'triggers': panic_triggers,
            'action': action,
            'equity': total_equity
        }
        self.panic_events.append(panic_event)
        
        # Send alert
        if action in ['close_all', 'cancel_orders']:
            self._send_panic_alert(panic_triggers, action)
        
        # Set cooldown if taking action
        if action != 'monitor':
            self.cooldown_until = time.time() + 300  # 5 minute cooldown
        
        reason = self._format_panic_reason(panic_triggers)
        
        return {
            'panic_mode': action != 'monitor',
            'action': action,
            'close_positions': close_positions,
            'triggers': panic_triggers,
            'reason': reason
        }
    
    def _check_price_spike(self, symbol: str) -> Optional[Dict]:
        """
        Check for sudden price movements
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Spike details or None
        """
        if symbol not in self.price_history:
            return None
            
        history = self.price_history[symbol]
        if len(history) < 2:
            return None
        
        # Get current and 1-minute ago prices
        current_price = history[-1][1]
        
        # Find price from ~60 seconds ago
        current_time = time.time()
        for timestamp, price in reversed(history[:-1]):
            if current_time - timestamp >= 60:
                minute_ago_price = price
                break
        else:
            return None
        
        # Calculate percentage change
        if minute_ago_price > 0:
            change_pct = abs((current_price - minute_ago_price) / minute_ago_price)
            
            if change_pct >= PANIC_PRICE_MOVE_PCT:
                return {
                    'move_pct': change_pct,
                    'from_price': minute_ago_price,
                    'to_price': current_price,
                    'direction': 'spike' if current_price > minute_ago_price else 'crash'
                }
        
        return None
    
    def _check_volume_anomalies(self, symbols: List[str]) -> List[Dict]:
        """
        Check for abnormal volume spikes
        
        Args:
            symbols: List of symbols to check
            
        Returns:
            List of volume anomalies
        """
        anomalies = []
        
        for symbol in symbols:
            if symbol not in self.volume_history:
                continue
            
            history = self.volume_history[symbol]
            if len(history) < 10:  # Need history for average
                continue
            
            # Calculate average volume
            recent_volumes = [v for _, v in history[-10:]]
            avg_volume = sum(recent_volumes) / len(recent_volumes)
            current_volume = history[-1][1]
            
            if avg_volume > 0:
                volume_ratio = current_volume / avg_volume
                
                if volume_ratio >= PANIC_VOLUME_SPIKE:
                    anomalies.append({
                        'type': 'volume_spike',
                        'symbol': symbol,
                        'severity': 'high',
                        'details': {
                            'ratio': volume_ratio,
                            'current': current_volume,
                            'average': avg_volume
                        }
                    })
        
        return anomalies
    
    def _check_position_losses(self, positions: Dict[str, float], 
                              total_equity: float) -> List[Dict]:
        """
        Check for significant position losses
        
        Args:
            positions: Current positions
            total_equity: Total equity
            
        Returns:
            List of position-specific panic triggers
        """
        triggers = []
        
        # This would need access to entry prices to calculate actual losses
        # For now, we'll skip this check
        # TODO: Integrate with trade history for actual P&L
        
        return triggers
    
    def update_price_history(self, symbol: str, price: float):
        """
        Update price tracking
        
        Args:
            symbol: Trading pair symbol
            price: Current price
        """
        if symbol not in self.price_history:
            self.price_history[symbol] = []
        
        # Add new price point
        self.price_history[symbol].append((time.time(), price))
        
        # Keep only last 5 minutes of history
        cutoff = time.time() - 300
        self.price_history[symbol] = [
            (t, p) for t, p in self.price_history[symbol] if t > cutoff
        ]
    
    def update_volume_history(self, symbol: str, volume: float):
        """
        Update volume tracking
        
        Args:
            symbol: Trading pair symbol
            volume: Current volume
        """
        if symbol not in self.volume_history:
            self.volume_history[symbol] = []
        
        # Add new volume point
        self.volume_history[symbol].append((time.time(), volume))
        
        # Keep only last 30 minutes of history
        cutoff = time.time() - 1800
        self.volume_history[symbol] = [
            (t, v) for t, v in self.volume_history[symbol] if t > cutoff
        ]
    
    def register_trade_result(self, profit: bool):
        """
        Register trade outcome for consecutive loss tracking
        
        Args:
            profit: True if profitable, False if loss
        """
        if profit:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            
            if self.consecutive_losses >= CONSECUTIVE_LOSS_LIMIT:
                self.logger.warning(f"Consecutive losses: {self.consecutive_losses}")
    
    def reset_cooldown(self):
        """Manually reset panic cooldown"""
        self.cooldown_until = None
        self.logger.info("Panic cooldown reset")
    
    def get_panic_report(self) -> Dict:
        """
        Get current panic monitoring status
        
        Returns:
            Dict with panic metrics
        """
        return {
            'consecutive_losses': self.consecutive_losses,
            'monitored_symbols': len(self.price_history),
            'in_cooldown': bool(self.cooldown_until and time.time() < self.cooldown_until),
            'cooldown_remaining': max(0, self.cooldown_until - time.time()) if self.cooldown_until else 0,
            'recent_events': self.panic_events[-5:],  # Last 5 panic events
            'price_history_size': {s: len(h) for s, h in self.price_history.items()},
            'volume_history_size': {s: len(h) for s, h in self.volume_history.items()}
        }
    
    def _format_panic_reason(self, triggers: List[Dict]) -> str:
        """
        Format human-readable panic reason
        
        Args:
            triggers: List of panic triggers
            
        Returns:
            Formatted reason string
        """
        if not triggers:
            return "No panic conditions"
        
        reasons = []
        for trigger in triggers[:3]:  # Top 3 reasons
            if trigger['type'] == 'price_spike':
                direction = trigger['details']['direction']
                pct = trigger['details']['move_pct'] * 100
                reasons.append(f"{trigger['symbol']} {direction} {pct:.1f}%")
            elif trigger['type'] == 'volume_spike':
                ratio = trigger['details']['ratio']
                reasons.append(f"{trigger['symbol']} volume {ratio:.1f}x normal")
            elif trigger['type'] == 'consecutive_losses':
                reasons.append(f"{trigger['count']} consecutive losses")
        
        return "; ".join(reasons)
    
    def _send_panic_alert(self, triggers: List[Dict], action: str):
        """
        Send panic alert email
        
        Args:
            triggers: List of triggers
            action: Recommended action
        """
        if not ALERT_EMAIL_ENABLED:
            return
        
        # Format message
        message = f"PANIC CONDITIONS DETECTED\n\n"
        message += f"Action: {action.upper()}\n\n"
        message += "Triggers:\n"
        
        for trigger in triggers:
            message += f"- {trigger['type']}: "
            if 'symbol' in trigger:
                message += f"{trigger['symbol']} "
            if 'details' in trigger:
                message += f"{trigger['details']}\n"
            else:
                message += "\n"
        
        try:
            msg = MIMEText(message)
            msg['Subject'] = f"🚨 PANIC ALERT - {get_email_date()}"
            msg['From'] = ALERT_EMAIL_ADDRESS
            msg['To'] = ALERT_EMAIL_RECIPIENT
            msg['Date'] = get_email_date()
            msg['X-Priority'] = '1'  # Highest priority
            
            server = smtplib.SMTP_SSL(ALERT_EMAIL_SMTP_SERVER, ALERT_EMAIL_SMTP_PORT)
            server.login(ALERT_EMAIL_ADDRESS, ALERT_EMAIL_PASSWORD)
            server.sendmail(ALERT_EMAIL_ADDRESS, [ALERT_EMAIL_RECIPIENT], msg.as_string())
            server.quit()
            
            self.logger.info("Panic alert email sent")
        except Exception as e:
            self.logger.error(f"Failed to send panic alert: {e}")
