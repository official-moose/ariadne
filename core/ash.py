#>> A R I A N D E v6
#>> last update: 2025 | Sept. 4
#>>
#>> ASH - Balance Cache Manager
#>> mm/core/ash.py
#>>
#>> Maintains real-time balance cache for Julius
#>> Updates every second from database and exchange
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250904.01

import os
import sys
import json
import signal
import time
from typing import Dict, Any
from pathlib import Path
from decimal import Decimal
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

# Load environment variables
load_dotenv()

# Add parent directory to path
sys.path.append('/root/Echelon/valentrix')

from mm.utils.helpers.wintermute import (
    get_logger,
    now_pack,
    write_pid_file,
    cleanup_pid_file,
    get_db_connection,
    release_db_connection
)

from mm.conn.conn_kucoin import KucoinClient

# Fetch the current operating mode from Inara
def get_mode_safe() -> str: 
    try: 
        from mm.utils.helpers import inara 
        return inara.get_mode() 
    except Exception: 
        return "halted"

# Configuration
BALANCE_CACHE_PATH = "/root/Echelon/valentrix/mm/data/source/balances.json"
PID_FILE = "/root/Echelon/valentrix/mm/core/ash.pid"
LOG_FILE = "/root/Echelon/valentrix/mm/core/ash.log"
UPDATE_INTERVAL = 1  # seconds

# Global shutdown flag
shutdown_requested = False

# Logger
log = get_logger("ash", LOG_FILE)

# Signal handler
def signal_handler(signum, frame):
    global shutdown_requested
    log.info(f"[SHUTDOWN] Received signal {signum}")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class BalanceCache:
    def __init__(self):
        self.kucoin = KucoinClient()
        self.cycle_count = 0
    
    def get_simulation_balances(self) -> Dict:
        """Get balances from sim_balances table."""
        conn = get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Get all balances
            cur.execute("""
                SELECT asset, available, hold, (available + hold) as total
                FROM sim_balances
                WHERE (available + hold) > 0
            """)
            balances = {}
            for row in cur.fetchall():
                balances[row['asset']] = {
                    'available': float(row['available']),
                    'hold': float(row['hold']),
                    'total': float(row['total'])
                }
            
            # Get current prices for assets
            asset_values = {}
            total_equity = 0.0
            
            for asset, bal in balances.items():
                if asset == 'USDT':
                    asset_values[asset] = {
                        'native': bal['total'],
                        'usdt_value': bal['total']
                    }
                    total_equity += bal['total']
                else:
                    # Get price from tickstick
                    symbol = f"{asset}-USDT"
                    cur.execute("""
                        SELECT last FROM tickstick 
                        WHERE symbol = %s 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """, (symbol,))
                    price_row = cur.fetchone()
                    
                    if price_row and price_row['last']:
                        price = float(price_row['last'])
                        usdt_value = bal['total'] * price
                        asset_values[asset] = {
                            'native': bal['total'],
                            'usdt_value': usdt_value
                        }
                        total_equity += usdt_value
                    else:
                        asset_values[asset] = {
                            'native': bal['total'],
                            'usdt_value': 0.0
                        }
            
            cur.close()
            
            return {
                'balances': balances,
                'asset_values': asset_values,
                'total_equity': total_equity
            }
            
        finally:
            release_db_connection(conn)
    
    def get_live_balances(self) -> Dict:
        """Get balances from KuCoin exchange."""
        try:
            # Get account balances
            raw_balances = self.kucoin.get_account_balances_detailed()
            
            balances = {}
            asset_values = {}
            total_equity = 0.0
            
            for asset, bal_info in raw_balances.items():
                if bal_info['available'] > 0 or bal_info['hold'] > 0:
                    balances[asset] = {
                        'available': bal_info['available'],
                        'hold': bal_info['hold'],
                        'total': bal_info['available'] + bal_info['hold']
                    }
                    
                    if asset == 'USDT':
                        asset_values[asset] = {
                            'native': balances[asset]['total'],
                            'usdt_value': balances[asset]['total']
                        }
                        total_equity += balances[asset]['total']
                    else:
                        # Get current price
                        symbol = f"{asset}-USDT"
                        price = self.kucoin.last_trade_price(symbol)
                        
                        if price and price > 0:
                            usdt_value = balances[asset]['total'] * price
                            asset_values[asset] = {
                                'native': balances[asset]['total'],
                                'usdt_value': usdt_value
                            }
                            total_equity += usdt_value
                        else:
                            asset_values[asset] = {
                                'native': balances[asset]['total'],
                                'usdt_value': 0.0
                            }
            
            return {
                'balances': balances,
                'asset_values': asset_values,
                'total_equity': total_equity
            }
            
        except Exception as e:
            log.error(f"Failed to get live balances: {e}")
            return {
                'balances': {},
                'asset_values': {},
                'total_equity': 0.0
            }
    
    def update_cache(self):
        """Update balance cache file."""
        self.cycle_count += 1
        
        try:
            mode = get_mode()
            tp = now_pack()
            
            # Get balances based on mode
            if mode == 'simulation':
                sim_data = self.get_simulation_balances()
                live_data = {'balances': {}, 'asset_values': {}, 'total_equity': 0.0}
            else:  # live
                sim_data = self.get_simulation_balances()  # Still track sim for comparison
                live_data = self.get_live_balances()
            
            # Build cache structure
            cache = {
                'version': '1.0',
                'updated_at': tp.iso,
                'updated_epoch_ms': tp.epoch_ms,
                'current_mode': mode,
                'simulation': {
                    'balances': sim_data['balances'],
                    'asset_values': sim_data['asset_values'],
                    'total_equity': sim_data['total_equity']
                },
                'live': {
                    'balances': live_data['balances'],
                    'asset_values': live_data['asset_values'],
                    'total_equity': live_data['total_equity']
                }
            }
            
            # Write atomically
            Path(BALANCE_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
            temp_path = f"{BALANCE_CACHE_PATH}.tmp"
            
            with open(temp_path, 'w') as f:
                json.dump(cache, f, indent=2, default=str)
            
            os.rename(temp_path, BALANCE_CACHE_PATH)
            
            if self.cycle_count % 60 == 0:  # Log every minute
                log.info(f"Cache updated - Mode: {mode}, Sim equity: ${sim_data['total_equity']:.2f}, Live equity: ${live_data['total_equity']:.2f}")
                
        except Exception as e:
            log.error(f"Failed to update cache: {e}")
    
    def update_heartbeat(self):
        """Update heartbeat in database."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO heartbeats (process_name, last_heartbeat, status, pid, cycle_count)
                VALUES ('ash', NOW(), 'caching', %s, %s)
                ON CONFLICT (process_name)
                DO UPDATE SET 
                    last_heartbeat = NOW(),
                    status = 'caching',
                    pid = %s,
                    cycle_count = %s
            """, (os.getpid(), self.cycle_count, os.getpid(), self.cycle_count))
            conn.commit()
            cur.close()
            release_db_connection(conn)
        except Exception as e:
            log.error(f"Failed to update heartbeat: {e}")
    
    def run(self):
        """Main process loop."""
        log.info(f"[INIT] ASH starting - updating every {UPDATE_INTERVAL}s")
        
        while not shutdown_requested:
            start = time.time()
            
            self.update_cache()
            
            # Update heartbeat every 30 cycles
            if self.cycle_count % 30 == 0:
                self.update_heartbeat()
            
            # Sleep for remainder of interval
            elapsed = time.time() - start
            sleep_time = max(UPDATE_INTERVAL - elapsed, 0.1)
            time.sleep(sleep_time)
        
        log.info("[SHUTDOWN] ASH shutting down")

if __name__ == "__main__":
    try:
        write_pid_file(PID_FILE)
        cache = BalanceCache()
        cache.run()
    finally:
        cleanup_pid_file(PID_FILE)