#>> A R I A N D E v6.1
#>> last update: 2025 | Sept. 15
#>>
#>> clear_override.py
#>> mm/utils/tools/clear_override.py
#>>
#>> PURPOSE:
#>>   This is a utility script to clear any runtime override that has been set in Inara.
#>>   When an override is active, Inara returns the overridden mode instead of reading
#>>   the configuration in mm/config/marcus.py.
#>>
#>>   Running this script will reset Inara’s internal mode back to "none".
#>>   On the next call to get_mode(), Inara will re-read marcus.MODE
#>>   and use that as the system’s operational mode.
#>>
#>> USAGE:
#>>   python3 mm/utils/tools/clear_override.py
#>>
#>> NOTES:
#>>   • This does not change marcus.MODE itself.
#>>   • This only clears the override so marcus is respected again.
#>>   • No alert is sent; this script is intended for direct operator use.
#>>
#>> Auth'd -> Commander
#>>
#>>───────────────────────────────────────────────────

# Build|20250915.02

import logging

logger = logging.getLogger("ariadne.clear_override")

def main():
    from mm.utils.helpers import inara

    # Clear LRU cache and reset the override
    inara.get_mode.cache_clear()
    inara._mode = ""
    logger.info("Inara override cleared. Next get_mode() will read marcus.MODE.")
    print("Override cleared. Mode cache reset. marcus.MODE now active.")

if __name__ == "__main__":
    main()