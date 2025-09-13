#>> A R I A N D E v6
#>> last update: 2025 | Sept. 4
#>>
#>> Ariadne Config Parameters
#>> mm/config/marcus.py
#>>
#>> Config parameters for the market making bot, Ariadne.
#>> Trading limits & tolerances.
#>> Operation mode toggle.
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250904.01

from typing import List

# ── Operational Mode Toggle ─────────────────────────────────────────────────────────────────────
MODE: str = "simulation"
# Options
#>> simulation  | full simulation of live operations.
#>> live        | production environment, real trades, real money.
#>> halted      | no new orders, existing bids/asks are allowed to complete.
#>> drain       | no new orders, existing bids cancelled, positions are liquidated profitable or neutral.
#>> maintenance | trading actions disabled, background processes continue.
#>> shadow      | runs the full decision loop in parallel with live but only logs intents (no orders).
    
# ── Dashboard Progress Bar  ─────────────────────────────────────────────────────────────────────   
SHOW_PROGRESS: bool = False

# ── Paths ───────────────────────────────────────────────────────────────────────────────────────    
SIMULATION_DB_PATH: str = "mm/data/sims/ariadne_sim.db"   
LIVE_DB_PATH: str = "mm/data/live/ariadne_live.db"        
LEDGER_DB_PATH: str = "mm/data/finance/ledger.db"
SIM_STATE_FILE: str = "mm/data/state/sim_state.json"   
LIVE_STATE_FILE: str = "mm/data/state/live_state.json" 

# ── Capital & Risk Management ───────────────────────────────────────────────────────────────────
INITIAL_CAPITAL: float = 2500.0             # USDT
MAX_EXPOSURE_PER_PAIR: float = 0.1          # 10% of total capital
QUOTE_CURRENCY: str = "USDT"
BASE_QUOTE: str = QUOTE_CURRENCY            #For backwards compatibility, can be removed later.
INVENTORY_DRAWDOWN_LIMIT: float = 0.1       # 10% loss on inventory triggers defense
    
# ── Market Making Parameters ────────────────────────────────────────────────────────────────────
TARGET_SPREAD_PCT: float = 0.01             # 1% spread (was 0.002)
MAX_SPREAD_PCT: float = 0.03                # 3% max spread (was 0.01)
MIN_SPREAD_PCT: float = 0.005               # 0.5% minimum spread
    
# ── Order Management ────────────────────────────────────────────────────────────────────────────
ORDER_REFRESH_SECONDS: int = 600            # Cancel and replace orders older than this
MAX_ORDERS_PER_PAIR: int = 2                # Max concurrent orders per side per pair
ORDER_DISTANCE_FROM_MID: float = 0.005      # Minimum 0.5% distance from mid price
    
# ── Position Management ─────────────────────────────────────────────────────────────────────────
MAX_POSITION_AGE_HOURS: int = 4             # Liquidate positions older than this
POSITION_REDUCTION_SPREAD: float = 0.005    # Tighter spread when reducing position
MAX_ASSET_PCT: float = 0.10                 # 10% per-asset cap (share of portfolio value, ex-cash)
CAP_MARGIN:    float = 0.01                 # +1% tolerance band (allows slight overage before hard-deny)
    
# ── Pair Selection ──────────────────────────────────────────────────────────────────────────────
MIN_PAIR_VOLUME_24H: float = 100000         # Minimum 24h volume in USDT
MAX_PAIRS_TO_TRADE: int = 5                 # Focus on top 5 pairs only
PAIR_ROTATION_INTERVAL: int = 300           # Re-evaluate pairs every 5 minutes

# ── Profitability Targets ───────────────────────────────────────────────────────────────────────
TARGET_DAILY_RETURN: float = 0.0445         # 4.45% daily target
MIN_PROFIT_PER_TRADE: float = 0.001         # 0.1% minimum profit after fees
FEE_BUFFER_MULTIPLIER: float = 2.5          # Spread must be 2.5x the round-trip fees

# ── Advanced Risk Parameters ────────────────────────────────────────────────────────────────────
MAX_DRAWDOWN_PCT: float = 0.1               # 10% maximum equity drawdown
DAILY_LOSS_LIMIT: float = 0.05              # 5% maximum daily loss
MAX_LEVERAGE: float = 1.0                   # No leverage (1.0 = spot only)
MIN_TRADE_SIZE: float = 10.0                # $10 minimum trade size
POSITION_TIMEOUT_HOURS: int = 24            # Hours before considering position stale

# ── Market Making Strategy ──────────────────────────────────────────────────────────────────────
# TARGET_SPREAD_PCT: float = 0.001          # 0.1% target spread in calm markets (was 10 bps)
# MAX_SPREAD_PCT: float = 0.005             # 0.5% max spread during volatility (was 50 bps)
DYNAMIC_SPREAD_MULTIPLIER: float = 1.5      # 1.5x market spread for opportunistic trades

# ── Volatility & Danger Detection ───────────────────────────────────────────────────────────────
VOLATILITY_THRESHOLD: float = 0.02          # 2% price move triggers wider spreads
PANIC_THRESHOLD: float = 0.05               # 5% price move triggers emergency shutdown
PANIC_LOOKBACK_WINDOW: int = 60             # Duration in seconds to measure the panic threshold

# ── Market Selection & Filters ──────────────────────────────────────────────────────────────────
QUOTE_CURRENCY: str = "USDT"                # Only select USDT pairs ᴾᴿᴱꝬᴵᴸᵀᴱᴿ
MIN_24H_VOLUME: float = 5000000             # 5M USDT minimum volume ᴾᴿᴱꝬᴵᴸᵀᴱᴿ
MAX_24H_VOLUME: float = 200000000           # 200M USDT maximum volume ᴾᴿᴱꝬᴵᴸᵀᴱᴿ
MIN_COIN_AGE: int = 7                       # Minimum days of trading required ᴾᴿᴱꝬᴵᴸᵀᴱᴿ
MIN_BOOK_DEPTH_USD: float = 1000.0          # Minimum order book depth in USD
MAX_TOP_WALL_SHARE: float = 0.3             # Maximum allowed top order dominance (30%)
MIN_LIQUIDITY_SCORE: float = 50.0           # Minimum score (0-100) to consider a pair
MAX_ACTIVE_PAIRS: int = 10                  # Maximum simultaneous positions

# ── Scoring Engine Weights ──────────────────────────────────────────────────────────────────────
LIQUIDITY: float = 0.40                     # 40% weight in overall scoring 
SPREAD_TIGHTNESS: float = 0.15              # 15% component in Liquidity scoring §ᵁᴮ
ORDER_BOOK_DEPTH: float = 0.15              # 15% component in Liquidity scoring §ᵁᴮ
SLIPPAGE_RESISTANCE: float = 0.10           # 10% component in Liquidity scoring §ᵁᴮ
MARKET: float = 0.30                        # 30% weight in overall scoring 
VOLATILITY_PROFILE: float = 0.15            # 15% component in Market scoring §ᵁᴮ
VOLUME_CONSISTENCY: float = 0.10            # 10% component in Market scoring §ᵁᴮ 
PRICE_STABILITY: float = 0.05               #  5% component in Market scoring §ᵁᴮ
TRADING: float = 0.30                       # 30% weight in overall scoring 
FEE_EFFICIENCY: float = 0.10                # 10% component in Trading scoring §ᵁᴮ 
EXECUTION_SPEED: float = 0.10               # 10% component in Trading scoring §ᵁᴮ 
MARKET_IMPACT: float = 0.10                 # 10% component in Trading scoring §ᵁᴮ
OPPORTUNITY_MOD: float = 0.05               #  5% bonus applied to the overall score

# ── Operational Settings ────────────────────────────────────────────────────────────────────────
LOOP_DELAY: int = 5                         # Seconds to wait between main loop iterations
LOG_LEVEL: str = "DEBUG"                    # DEBUG, INFO, WARNING, ERROR
CACHE_TTL: int = 30                         # Seconds to cache market data

# ── Emergency Settings ──────────────────────────────────────────────────────────────────────────
MAX_API_RETRIES: int = 3                    # Maximum API retry attempts
HEARTBEAT_TIMEOUT: int = 30                 # Seconds before considering system unresponsive
EMERGENCY_TIMEOUT: int = 300                # Seconds to wait after emergency stop

# ── Exchange & Connection Settings ──────────────────────────────────────────────────────────────
EXCHANGE: str = "kucoin"                    # Currently only kucoin is implemented
API_TIMEOUT: int = 10                       # Seconds before API requests time out
    
# ── Email Server Configuration Settings ─────────────────────────────────────────────────────────
ALERT_EMAIL_ENABLED: bool = True            # Set to False to disable email alerts
ALERT_EMAIL_SMTP_SERVER: str = "smtp.hostinger.com"  
ALERT_EMAIL_SMTP_PORT: int = 465
ALERT_EMAIL_ENCRYPT: str = "SSL"
ALERT_EMAIL_RECIPIENT: str = "james@hodlcorp.io"
# !! Sign-in credentials are located in the dotenv file.
    
# ── Data Manager Windows (in ms) ────────────────────────────────────────────────────────────────
DM_TICK_RETENTION_SECONDS: int = 3600
DM_CANDLE_RETENTION_SECONDS: int = 86400