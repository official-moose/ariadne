#>> A R I A N D E v6
#>> last update: 2025 | Sept. 04
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
#>>   • An alert email will be sent so ops knows the override was cleared.
#>>
#>> Auth'd -> Commander
#>>
#>>────────────────────────────────────────────────────────────────

# Build|20250904.01

import logging

logger = logging.getLogger("ariadne.clear_override")

def main():
    from mm.utils.helpers import inara
    # Reset the override by setting the private _mode to empty
    inara._mode = ""
    logger.info("Inara override cleared. Next get_mode() will read marcus.MODE.")

    try:
        from mm.utils.helpers.wintermute import send_alert
        send_alert(
            subject="[Ariadne] Inara override cleared",
            message="Inara runtime override has been cleared. "
                    "Future calls to get_mode() will follow marcus.MODE.",
            process_name="clear_override"
        )
        print("Override cleared. Alert sent.")
    except Exception as e:
        logger.error("Failed to send clear_override alert: %s", e)
        print("Override cleared, but alert email failed:", e)

if __name__ == "__main__":
    main()
