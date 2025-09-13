#>> A R I A N D E v6
#>> last update: 2025 | Sept. 4
#>>
#>> Viki Config
#>> mm/utils/market_state/viki_config.py
#>>
#>> Thresholds and toggles for the Market State Oracle.
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250904.01

# Logging toggle and path
LOGGING: bool = True
LOG_PATH: str = "mm/utils/market_state/viki_log.json"

# Indicator lookbacks
RSI_LEN: int = 14
BOLL_LEN: int = 20
ATR_LEN: int = 14

# Returns lookback (index offsets on fetched arrays)
LOOKBACK_SHORT: int = 8    # ~ last 8 bars of given TF
LOOKBACK_LONG: int = 48    # context size for TF series

# Panic thresholds
PANIC_THRESH = {
    "ret_15m": -3.0,         # <= -3% in 15m
    "vol_z_min": 2.0,        # >= 2 std devs above mean
    "spread_pct_min": 0.25,  # >= 0.25% spread
}

# Capitulation thresholds (harsher than panic)
CAPITULATION_THRESH = {
    "ret_1h": -8.0,          # <= -8% in 1h
    "vol_z_min": 3.0,        # >= 3 std devs
    "rsi_max": 20.0,         # RSI <= 20
}

# Euphoria thresholds
EUPHORIA_THRESH = {
    "ret_1h": 6.0,           # >= +6% in 1h
    "vol_z_min": 2.0,        # >= 2 std devs
    "rsi_min": 70.0,         # RSI >= 70
}

# Stale thresholds
STALE_THRESH = {
    "ret_abs_max": 0.5,      # abs 15m return < 0.5%
    "bb_width_max": 1.0,     # band width < 1% of price
    "vol_ratio_max": 0.9,    # last vol lower than recent avg
    "spread_pct_max": 0.15,  # tight spread
}

# Wolf thresholds (rangebound but choppy)
WOLF_THRESH = {
    "bb_width_min": 2.0,      # band width >= 2% of price
    "flips_min": 3.0,         # >= 3 sign flips over short window
    "net_change_abs_max": 0.7 # <= 0.7 units net px change over short window
}

# Correction: % below recent high considered a correction
CORRECTION_DROP_PCT: float = 10.0
