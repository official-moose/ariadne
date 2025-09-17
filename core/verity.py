#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250917.01
#===================================================================
# last update: 2025 | Sept. 17                  Production ready ✅
#===================================================================
# Verity - Applied IO (Intelligence Officer) (Metrics)
# mm/core/verity.py
#
# Performance and risk metrics tracking
# Statistical analysis of trading operations
# Provides insights for optimization
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✖ PERSISTANT RUNTIME  ✖ MONIT MANAGED
#===================================================================

# 🔸 Standard Library Imports ======================================

import logging
import time
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict, deque

# 🔸 Logger Setup ==================================================

logger = logging.getLogger('ariadne.metrics')

class IntelOps:
# 💬 Tracks and analyzes all performance and risk metrics

    def __init__(self, history_limit: int = 10000):
        """
        Initialize metrics tracking

        Args:
            history_limit: Maximum data points to keep per metric
        """
        self.logger = logger
        self.history_limit = history_limit

        # 🔹 Core metric storage =======================================

        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_limit))
        self.counters: Dict[str, int] = defaultdict(int)

        # 🔹 Summary statistics cache ==================================

        self.stats_cache: Dict[str, Dict] = {}
        self.cache_timestamp: float = 0
        self.cache_ttl: float = 60  # 1 minute cache

        # 🔹 Trading-specific metrics ==================================

        self.trades: List[Dict] = []
        self.daily_metrics: Dict[str, Dict] = defaultdict(dict)
        self.session_start: float = time.time()

    def record_metric(self, name: str, value: float, timestamp: Optional[float] = None):
        """
        Record a metric data point

        Args:
            name: Metric name
            value: Metric value
            timestamp: Optional timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = time.time()

        self.metrics[name].append({
            'value': value,
            'timestamp': timestamp
        })

        # 🔹 Invalidate cache ==========================================

        self.stats_cache.pop(name, None)

    def increment_counter(self, name: str, amount: int = 1):
        """
        Increment a counter metric

        Args:
            name: Counter name
            amount: Amount to increment
        """
        self.counters[name] += amount

    def get_stats(self, metric_name: str, window_seconds: Optional[int] = None) -> Dict:
        """
        Get statistics for a metric

        Args:
            metric_name: Name of metric
            window_seconds: Optional time window (uses all data if None)

        Returns:
            Dict with min, max, avg, count, last
        """
        # 🔹 Check cache first =========================================

        cache_key = f"{metric_name}_{window_seconds}"
        if cache_key in self.stats_cache and time.time() - self.cache_timestamp < self.cache_ttl:
            return self.stats_cache[cache_key]

        if metric_name not in self.metrics:
            return {
                'min': 0,
                'max': 0,
                'avg': 0,
                'count': 0,
                'last': 0,
                'std_dev': 0
            }

        data = list(self.metrics[metric_name])

        # 🔹 Filter by time window if specified ========================

        if window_seconds:
            cutoff = time.time() - window_seconds
            data = [d for d in data if d['timestamp'] > cutoff]

        if not data:
            return {
                'min': 0,
                'max': 0,
                'avg': 0,
                'count': 0,
                'last': 0,
                'std_dev': 0
            }

        values = [d['value'] for d in data]

        # 🔹 Calculate standard deviation ==============================

        avg = sum(values) / len(values)
        variance = sum((x - avg) ** 2 for x in values) / len(values)
        std_dev = variance ** 0.5

        stats = {
            'min': min(values),
            'max': max(values),
            'avg': avg,
            'count': len(values),
            'last': values[-1],
            'std_dev': std_dev
        }

        # 🔹 Cache result ==============================================

        self.stats_cache[cache_key] = stats
        self.cache_timestamp = time.time()

        return stats

    def record_trade(self, trade: Dict):
        """
        Record a completed trade

        Args:
            trade: Trade details dict
        """
        trade['timestamp'] = time.time()
        self.trades.append(trade)

        # 🔹 Update counters ===========================================

        self.increment_counter(f"trades_{trade['side']}")
        self.increment_counter("trades_total")

        # 🔹 Record P&L if available ===================================

        if 'pnl' in trade:
            self.record_metric('trade_pnl', trade['pnl'])
            if trade['pnl'] > 0:
                self.increment_counter('winning_trades')
            else:
                self.increment_counter('losing_trades')

    def get_performance_summary(self) -> Dict:
        """
        Get comprehensive performance summary

        Returns:
            Dict with performance metrics
        """
        # 🔹 Calculate uptime ==========================================

        uptime_seconds = time.time() - self.session_start
        uptime_hours = uptime_seconds / 3600

        # 🔹 Trade statistics ==========================================

        total_trades = self.counters['trades_total']
        winning_trades = self.counters['winning_trades']
        losing_trades = self.counters['losing_trades']

        win_rate = 0
        if total_trades > 0:
            win_rate = winning_trades / total_trades

        # 🔹 P&L statistics ============================================

        pnl_stats = self.get_stats('trade_pnl')
        loop_stats = self.get_stats('loop_time')
        equity_stats = self.get_stats('total_equity')

        return {
            'uptime_hours': round(uptime_hours, 2),
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': round(win_rate, 3),
            'buy_orders': self.counters['trades_buy'],
            'sell_orders': self.counters['trades_sell'],
            'pnl': {
                'total': sum(t.get('pnl', 0) for t in self.trades),
                'average': pnl_stats['avg'],
                'best': pnl_stats['max'],
                'worst': pnl_stats['min']
            },
            'loop_performance': {
                'average_ms': round(loop_stats['avg'] * 1000, 1),
                'min_ms': round(loop_stats['min'] * 1000, 1),
                'max_ms': round(loop_stats['max'] * 1000, 1)
            },
            'equity': {
                'current': equity_stats['last'],
                'high': equity_stats['max'],
                'low': equity_stats['min']
            }
        }

    def get_hourly_metrics(self, hours: int = 24) -> Dict[str, List]:
        """
        Get metrics aggregated by hour

        Args:
            hours: Number of hours to retrieve

        Returns:
            Dict of metric_name -> list of hourly values
        """
        hourly_data = defaultdict(list)
        cutoff = time.time() - (hours * 3600)

        for metric_name, data in self.metrics.items():

            # 🔹 Group by hour =========================================

            hourly_buckets = defaultdict(list)

            for point in data:
                if point['timestamp'] > cutoff:
                    hour = datetime.fromtimestamp(point['timestamp']).replace(
                        minute=0, second=0, microsecond=0
                    )
                    hourly_buckets[hour].append(point['value'])

            # 🔹 Calculate hourly averages =============================

            for hour in sorted(hourly_buckets.keys()):
                values = hourly_buckets[hour]
                hourly_data[metric_name].append({
                    'hour': hour.isoformat(),
                    'avg': sum(values) / len(values),
                    'count': len(values)
                })

        return dict(hourly_data)

    def log_performance_stats(self):
        """Log current performance statistics"""
        summary = self.get_performance_summary()

        self.logger.info(
            f"📊 Performance - "
            f"Uptime: {summary['uptime_hours']:.1f}h | "
            f"Trades: {summary['total_trades']} (Win: {summary['win_rate']:.1%}) | "
            f"P&L: ${summary['pnl']['total']:.2f} | "
            f"Loop: {summary['loop_performance']['average_ms']}ms"
        )

    def export_metrics(self, filepath: str):
        """
        Export all metrics to JSON file

        Args:
            filepath: Output file path
        """
        try:
            export_data = {
                'timestamp': time.time(),
                'session_start': self.session_start,
                'counters': dict(self.counters),
                'summary': self.get_performance_summary(),
                'metrics': {}
            }

        # 🔹 Convert deques to lists for JSON serialization ============

            for name, data in self.metrics.items():
                export_data['metrics'][name] = list(data)

            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2)

            self.logger.info(f"Exported metrics to {filepath}")

        except Exception as e:
            self.logger.error(f"Failed to export metrics: {e}")

    def reset_daily_metrics(self):
        """Reset daily tracking (call at UTC midnight)"""

        # 🔹 Store yesterday's data ====================================

        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.daily_metrics[yesterday] = {
            'trades': self.counters['trades_total'],
            'pnl': sum(t.get('pnl', 0) for t in self.trades),
            'win_rate': self.counters['winning_trades'] / max(1, self.counters['trades_total'])
        }

        # 🔹 Reset counters ============================================

        for key in ['trades_total', 'trades_buy', 'trades_sell', 'winning_trades', 'losing_trades']:
            self.counters[key] = 0

        # 🔹 Clear today's trades ======================================

            cutoff = datetime.utcnow().replace(hour=0, minute=0, second=0).timestamp()
            self.trades = [t for t in self.trades if t['timestamp'] < cutoff]

            self.logger.info("Daily metrics reset completed")
