#===================================================================
# 🍁 A R I A N D E           bot version 6.1 file build 20250924.01
#===================================================================
# last update: 2025 | Sept. 24                  PRODUCTION READY ✅
#===================================================================
# Thea - Seldon Engine
# mm/utils/seldon_engine/thea.py
#
# Captures full KuCoin orderbook + meta signals every 5 minutes
#
# [520] [741] [8]
#===================================================================
# 🜁 THE COMMANDER            ✔ PERSISTENT RUNTIME  ✔ MONIT MANAGED
#===================================================================

import os
import sys
import time
import json
import signal
import psycopg2
import numpy as np
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Import from Marcus
from mm.config.marcus import (
    PRC_THEA,
    HB_THEA,
    MIN_24H_VOLUME,
    MAX_24H_VOLUME,
    MIN_COIN_AGE,
    SIGNALS_CAPTURE_INT,
    BATCH_SIZE,
    CANDLE_15M_LIMIT,
    CANDLE_1H_LIMIT,
    BOOK_DEPTH,
    TRADES_LIMIT
)

# Import from Wintermute
from mm.utils.helpers.wintermute import (
    get_logger,
    get_db_connection,
    send_email,
    write_pid_file,
    cleanup_pid_file,
    update_heartbeat,
    init_logging
)

# Import client from Inara 
from mm.conn.conn_kucoin import KucoinClient

# ---- Logger -----------------------------------------------------------

logger = init_logging(
    LOG_SELF=True,
    LOG_MAIN=True,
    SCREEN_OUT=True,
    LOGGER="Thea"  
)

# ---- Signal Handler ------------------------------------------------------
shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"[SHUTDOWN] Received signal {signum}")
    shutdown_requested = True
    cleanup_pid_file("thea")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ---- Helpers -------------------------------------------------------------
def safe_float(value, default=0.0) -> float:
    """Safely convert to float with default."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def safe_decimal(value, default="0") -> Decimal:
    """Safely convert to Decimal."""
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value))
    except:
        return Decimal(default)

def calculate_spread_pct(bid: float, ask: float) -> float:
    """Calculate bid-ask spread percentage."""
    if bid <= 0:
        return 0.0
    return ((ask - bid) / bid) * 100

def aggregate_orderbook(bids: List, asks: List) -> Dict:
    """Aggregate orderbook data into summary metrics."""
    bid_total = sum(safe_float(b[1]) for b in bids[:ORDERBOOK_DEPTH])
    ask_total = sum(safe_float(a[1]) for a in asks[:ORDERBOOK_DEPTH])
    
    best_bid = safe_float(bids[0][0]) if bids else 0
    best_ask = safe_float(asks[0][0]) if asks else 0
    
    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
    spread_pct = calculate_spread_pct(best_bid, best_ask)
    
    return {
        'bid_size_total': bid_total,
        'ask_size_total': ask_total,
        'mid_price': mid_price,
        'spread_pct': spread_pct
    }

def aggregate_trades(trades: List) -> Dict:
    """Aggregate recent trades into flow metrics."""
    if not trades:
        return {
            'avg_trade_price': 0,
            'avg_trade_size': 0,
            'buy_volume_ratio': 0.5
        }
    
    prices = []
    sizes = []
    buy_volume = 0
    total_volume = 0
    
    for trade in trades:
        price = safe_float(trade.get('price'))
        size = safe_float(trade.get('size'))
        side = trade.get('side', '').lower()
        
        prices.append(price)
        sizes.append(size)
        
        volume = price * size
        total_volume += volume
        if side == 'buy':
            buy_volume += volume
    
    return {
        'avg_trade_price': np.mean(prices) if prices else 0,
        'avg_trade_size': np.mean(sizes) if sizes else 0,
        'buy_volume_ratio': buy_volume / total_volume if total_volume > 0 else 0.5
    }

def process_candles(candles_15m: List, candles_1h: List) -> Dict:
    """Process candle data into structured format."""
    result = {}
    
    # Process 15m candles (most recent)
    if candles_15m and len(candles_15m) > 0:
        latest_15m = candles_15m[-1]
        result.update({
            'open_15m': safe_float(latest_15m[1]),
            'high_15m': safe_float(latest_15m[2]),
            'low_15m': safe_float(latest_15m[3]),
            'close_15m': safe_float(latest_15m[4]),
            'vol_15m': safe_float(latest_15m[5]),
            'volval_15m': safe_float(latest_15m[6])
        })
    else:
        result.update({
            'open_15m': 0, 'high_15m': 0, 'low_15m': 0,
            'close_15m': 0, 'vol_15m': 0, 'volval_15m': 0
        })
    
    # Process 1h candles (most recent)
    if candles_1h and len(candles_1h) > 0:
        latest_1h = candles_1h[-1]
        result.update({
            'open_1h': safe_float(latest_1h[1]),
            'high_1h': safe_float(latest_1h[2]),
            'low_1h': safe_float(latest_1h[3]),
            'close_1h': safe_float(latest_1h[4]),
            'vol_1h': safe_float(latest_1h[5]),
            'volval_1h': safe_float(latest_1h[6])
        })
    else:
        result.update({
            'open_1h': 0, 'high_1h': 0, 'low_1h': 0,
            'close_1h': 0, 'vol_1h': 0, 'volval_1h': 0
        })
    
    return result

def check_symbol_maturity(client: KucoinClient, symbol: str) -> bool:
    """Check if symbol has been trading for at least MIN_DAYS_LISTED days."""
    try:
        # Get klines to check history
        end_time = int(time.time())
        start_time = end_time - (MIN_DAYS_LISTED * 24 * 3600)
        
        # Try to get daily candles for the period
        klines = client.get_klines(
            symbol=symbol,
            interval='1day',
            startAt=start_time,
            endAt=end_time
        )
        
        if not klines or not klines.get('data'):
            return False
            
        # Need at least MIN_DAYS_LISTED candles
        return len(klines['data']) >= MIN_DAYS_LISTED
        
    except Exception as e:
        logger.debug(f"[MATURITY] Could not check {symbol}: {e}")
        return False

# ---- Data Collection -----------------------------------------------------
class SignalsCollector:
    """Collect and store market signals for Seldon Engine."""
    
    def __init__(self):
        self.client = KucoinClient()
        self.cycle_count = 0
        self.last_heartbeat = time.time()
        
    def collect_symbol_data(self, symbol: str) -> Optional[Dict]:
        """Collect all data for a single symbol."""
        try:
            data = {'symbol': symbol}
            
            # Get ticker data first (for current metrics)
            ticker_resp = self.client.get_ticker(symbol)
            if not ticker_resp or ticker_resp.get('code') != '200000':
                logger.warning(f"[SKIP] {symbol}: No ticker data")
                return None
                
            ticker = ticker_resp.get('data', {})
            
            # Apply volume filter
            vol_value = safe_float(ticker.get('volValue'))
            if vol_value < MIN_VOLUME_USDT or vol_value > MAX_VOLUME_USDT:
                logger.debug(f"[FILTER] {symbol}: Volume {vol_value:,.0f} outside range")
                return None
            
            # Get candles
            try:
                candles_15m = self.client.get_klines(
                    symbol=symbol,
                    interval='15min',
                    limit=CANDLE_LIMITS['15min']
                ).get('data', [])
                
                candles_1h = self.client.get_klines(
                    symbol=symbol,
                    interval='1hour',
                    limit=CANDLE_LIMITS['1hour']
                ).get('data', [])
                
                candle_data = process_candles(candles_15m, candles_1h)
                data.update(candle_data)
                
            except Exception as e:
                logger.warning(f"[SKIP] {symbol}: Candle error: {e}")
                return None
            
            # Get orderbook
            try:
                ob_resp = self.client.get_order_book(symbol, depth=ORDERBOOK_DEPTH)
                if ob_resp and ob_resp.get('code') == '200000':
                    ob_data = ob_resp.get('data', {})
                    ob_metrics = aggregate_orderbook(
                        ob_data.get('bids', []),
                        ob_data.get('asks', [])
                    )
                    data.update(ob_metrics)
                else:
                    data.update({
                        'bid_size_total': 0,
                        'ask_size_total': 0,
                        'mid_price': 0,
                        'spread_pct': 0
                    })
            except Exception as e:
                logger.debug(f"[WARN] {symbol}: Orderbook error: {e}")
                data.update({
                    'bid_size_total': 0,
                    'ask_size_total': 0,
                    'mid_price': 0,
                    'spread_pct': 0
                })
            
            # Get recent trades
            try:
                trades_resp = self.client.get_recent_trades(symbol, limit=TRADES_LIMIT)
                if trades_resp and trades_resp.get('code') == '200000':
                    trades = trades_resp.get('data', [])
                    trade_metrics = aggregate_trades(trades)
                    data.update(trade_metrics)
                else:
                    data.update({
                        'avg_trade_price': 0,
                        'avg_trade_size': 0,
                        'buy_volume_ratio': 0.5
                    })
            except Exception as e:
                logger.debug(f"[WARN] {symbol}: Trades error: {e}")
                data.update({
                    'avg_trade_price': 0,
                    'avg_trade_size': 0,
                    'buy_volume_ratio': 0.5
                })
            
            # Add ticker snapshot data
            data.update({
                'last_price': safe_float(ticker.get('last')),
                'buy_price': safe_float(ticker.get('buy')),
                'sell_price': safe_float(ticker.get('sell')),
                'average_price': safe_float(ticker.get('averagePrice')),
                'vol': safe_float(ticker.get('vol')),
                'volval': safe_float(ticker.get('volValue')),
                'change_rate': safe_float(ticker.get('changeRate'))
            })
            
            return data
            
        except Exception as e:
            logger.warning(f"[SKIP] {symbol}: {str(e)}")
            return None
    
    def store_signals(self, signals: List[Dict]) -> int:
        """Store collected signals in database."""
        if not signals:
            return 0
            
        stored = 0
        conn = None
        
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            timestamp = int(time.time())
            
            for signal in signals:
                try:
                    # Generate signal ID
                    sigid = f"{signal['symbol']}_{timestamp}"
                    
                    cur.execute("""
                        INSERT INTO signals_intel (
                            sigid, timestamp, symbol, interval,
                            open_15m, high_15m, low_15m, close_15m, vol_15m, volval_15m,
                            open_1h, high_1h, low_1h, close_1h, vol_1h, volval_1h,
                            bid_size_total, ask_size_total, mid_price, spread_pct,
                            last_price, buy_price, sell_price, average_price,
                            vol, volval, change_rate,
                            avg_trade_price, avg_trade_size, buy_volume_ratio
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s
                        )
                        ON CONFLICT (sigid) DO NOTHING
                    """, (
                        sigid, timestamp, signal['symbol'], '5min',
                        signal['open_15m'], signal['high_15m'], signal['low_15m'],
                        signal['close_15m'], signal['vol_15m'], signal['volval_15m'],
                        signal['open_1h'], signal['high_1h'], signal['low_1h'],
                        signal['close_1h'], signal['vol_1h'], signal['volval_1h'],
                        signal['bid_size_total'], signal['ask_size_total'],
                        signal['mid_price'], signal['spread_pct'],
                        signal['last_price'], signal['buy_price'], signal['sell_price'],
                        signal['average_price'], signal['vol'], signal['volval'],
                        signal['change_rate'],
                        signal['avg_trade_price'], signal['avg_trade_size'],
                        signal['buy_volume_ratio']
                    ))
                    
                    stored += 1
                    
                except Exception as e:
                    logger.error(f"[ERROR] Failed to store signal for {signal['symbol']}: {e}")
                    continue
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"[ERROR] Database error: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                cur.close()
                conn.close()
        
        return stored
    
    def run_collection_cycle(self):
        """Run one collection cycle for all symbols."""
        try:
            # Get all tickers
            ticker_resp = self.client.get_all_tickers()
            if not ticker_resp or ticker_resp.get('code') != '200000':
                logger.error("[ERROR] Failed to fetch tickers")
                return
            
            tickers = ticker_resp.get('data', {}).get('ticker', [])
            
            # Apply prefilter: USDT pairs with volume in range
            filtered_symbols = []
            for ticker in tickers:
                symbol = ticker.get('symbol', '')
                
                # Must be USDT pair
                if not symbol.endswith('-USDT'):
                    continue
                
                # Check volume
                vol_value = safe_float(ticker.get('volValue'))
                if vol_value < MIN_VOLUME_USDT or vol_value > MAX_VOLUME_USDT:
                    continue
                
                filtered_symbols.append(symbol)
            
            # Check maturity for filtered symbols
            mature_symbols = []
            for symbol in filtered_symbols:
                if check_symbol_maturity(self.client, symbol):
                    mature_symbols.append(symbol)
                else:
                    logger.debug(f"[FILTER] {symbol}: Not mature enough")
            
            logger.info(f"[COLLECT] Processing {len(mature_symbols)} symbols "
                       f"(filtered from {len(tickers)} total)")
            
            # Collect data for each symbol
            signals = []
            for i, symbol in enumerate(mature_symbols):
                if shutdown_requested:
                    break
                
                # Collect data
                data = self.collect_symbol_data(symbol)
                if data:
                    signals.append(data)
                
                # Process in batches
                if len(signals) >= BATCH_SIZE:
                    stored = self.store_signals(signals)
                    notes.info(f"[BATCH] Stored {stored}/{len(signals)} signals")
                    signals = []
                
                # Brief pause between symbols
                if i % 10 == 0:
                    time.sleep(0.1)
            
            # Store remaining signals
            if signals:
                stored = self.store_signals(signals)
                notes.info(f"[FINAL] Stored {stored}/{len(signals)} signals")
            
            self.cycle_count += 1
            logger.info(f"[CYCLE] Completed cycle {self.cycle_count}")
            
        except Exception as e:
            logger.error(f"[ERROR] Collection cycle failed: {e}")
            # Send alert on repeated failures
            if self.cycle_count > 0 and self.cycle_count % 5 == 0:
                send_alert(
                    subject="[THEA] Collection Failures",
                    message=f"Thea has encountered errors in collection cycle {self.cycle_count}: {str(e)}",
                    recipient_override=None
                )
    
    def run(self):
        """Main run loop."""
        logger.info("=" * 70)
        logger.info("[INIT] THEA starting as persistent background process")
        logger.info(f"[CONFIG] Capture interval: {CAPTURE_INTERVAL}s")
        logger.info(f"[CONFIG] Volume range: {MIN_VOLUME_USDT:,} - {MAX_VOLUME_USDT:,} USDT")
        logger.info(f"[CONFIG] Minimum maturity: {MIN_DAYS_LISTED} days")
        
        write_pid_file("thea")
        
        # Initial heartbeat
        update_heartbeat("thea")
        
        while not shutdown_requested:
            try:
                start_time = time.time()
                
                # Run collection
                self.run_collection_cycle()
                
                # Update heartbeat
                if time.time() - self.last_heartbeat >= HEARTBEAT_INTERVAL:
                    update_heartbeat("thea")
                    self.last_heartbeat = time.time()
                
                # Sleep until next interval
                elapsed = time.time() - start_time
                sleep_time = max(CAPTURE_INTERVAL - elapsed, 1)
                
                if sleep_time > 1:
                    logger.info(f"[SLEEP] Next capture in {sleep_time:.0f}s")
                
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"[ERROR] Main loop error: {e}")
                time.sleep(30)  # Brief pause on error
        
        cleanup_pid_file("thea")
        logger.info("[SHUTDOWN] THEA stopped")

# ---- Main Entry ----------------------------------------------------------
if __name__ == "__main__":
    collector = SignalsCollector()
    collector.run()