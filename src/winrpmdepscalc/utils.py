import logging


class LogColors:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    BLUE = "\033[34m"
    BOLD = "\033[1m"


class ColorFormatter(logging.Formatter):
    COLOR_MAP = {
        logging.DEBUG: LogColors.CYAN,
        logging.INFO: LogColors.GREEN,
        logging.WARNING: LogColors.YELLOW,
        logging.ERROR: LogColors.RED,
        logging.CRITICAL: LogColors.RED + LogColors.BOLD,
    }

    def format(self, record):
        color = self.COLOR_MAP.get(record.levelno, LogColors.RESET)
        message = super().format(record)
        return f"{color}{message}{LogColors.RESET}"


_logger = logging.getLogger("winrpmdepscalc")
_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = ColorFormatter("%(message)s")
ch.setFormatter(formatter)
_logger.addHandler(ch)
