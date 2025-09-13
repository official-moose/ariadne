#>> A R I A N D E v6
#>> last update: 2025 | Sept. 5
#>>
#>> TQDM Logger
#>> mm/utils/tqdm/agnes.py
#>>
#>> A logging handler that safely outputs to a terminal.
#>> Ensures human-freindly readability.    
#>> Formats [DEBUG][INFO][WARNING][ERROR][CRITICAL] 
#>>
#>> Auth'd -> Commander
#>>
#>> [520] [741] [8]      
#>>────────────────────────────────────────────────────────────────

# Build|20250905.01

import logging
import tqdm
from typing import Optional

class TqdmLogHandler(logging.Handler):
    """
    A logging handler that safely outputs to a terminal being used by tqdm progress bars.
    Ensures log messages are printed on new lines without interfering with progress bars.
    """
    def __init__(self, level: int = logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

def setup_logger(name: str, level: int = logging.INFO, log_format: Optional[str] = None) -> logging.Logger:
    """
    A helper function to quickly create and configure a logger that uses the TqdmLogHandler.

    Args:
        name (str): The name of the logger (e.g., __name__).
        level (int): The logging level (e.g., logging.INFO).
        log_format (str, optional): A format string for the log messages.
                                    Uses a default with timestamp if none provided.

    Returns:
        logging.Logger: A configured logger instance.
    """
    if log_format is None:
        log_format = '%(asctime)s    %(message)s'

    formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')

    handler = TqdmLogHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    # Avoid adding multiple handlers if this function is called more than once for the same logger
    if not logger.handlers:
        logger.addHandler(handler)
    # Prevent the log messages from being propagated to the root logger and appearing twice
    logger.propagate = False

    return logger