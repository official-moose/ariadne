#>> A R I A N D E v6
#>> last update: 2025 | Sept. 4
#>>
#>> Congig (The Seldon Engine)
#>> mm/utils/seldon_engine/seldon_config.py
#>>
#>> Centralized weights and logging toggle
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]
#>>────────────────────────────────────────────────────────────────

# Build|20250904.01

# ── LOGGING ──────────────────────────────────────────────────────
LOGGING: bool = True
SCORING_PATH: str = "mm/utils/seldon_engine/scoring.json"

# ── SCORE WEIGHTING ──────────────────────────────────────────────
WEIGHTS = {
"trend": 1.0,
"volatility": 1.0,
"patterns": 1.2,
"momentum": 1.1,
"orderbook": 0.9,
"volume_spike": 1.0,
"candlestick": 1.1,
"onchain": 1.0,
}