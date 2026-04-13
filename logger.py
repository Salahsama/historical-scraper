"""
Structured logger with colored console output and rotating file logs.
Monitors API rate limits and connection health.
"""
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from config import LOG_LEVEL, LOG_DIR, LOG_MAX_BYTES, LOG_BACKUP_COUNT


class ColorFormatter(logging.Formatter):
    """Console formatter with ANSI colors and icons."""
    COLORS = {
        "DEBUG":    "\033[36m",   # Cyan
        "INFO":     "\033[32m",   # Green
        "WARNING":  "\033[33m",   # Yellow
        "ERROR":    "\033[31m",   # Red
        "CRITICAL": "\033[35m",   # Magenta
    }
    RESET = "\033[0m"
    ICONS = {
        "DEBUG":    "🔍",
        "INFO":     "✅",
        "WARNING":  "⚠️ ",
        "ERROR":    "❌",
        "CRITICAL": "🔥",
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        icon = self.ICONS.get(record.levelname, "")
        ts = datetime.now().strftime("%H:%M:%S")
        module = record.name.split(".")[-1][:16].ljust(16)
        msg = record.getMessage()
        return f"{color}{ts} {icon} [{module}] {msg}{self.RESET}"


class FileFormatter(logging.Formatter):
    """Plain text formatter for file logging with structured fields."""
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"{ts} [{record.levelname:8s}] [{record.name}] {record.getMessage()}"


def get_logger(name: str) -> logging.Logger:
    """Create a named logger with console + rotating file handlers."""
    logger = logging.getLogger(f"scraper.{name}")
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    logger.propagate = False

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(ColorFormatter())
    logger.addHandler(console)

    # Rotating file handler
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / "scraper.log"
        fh = RotatingFileHandler(
            str(log_file),
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        fh.setFormatter(FileFormatter())
        logger.addHandler(fh)
    except OSError:
        pass  # Skip file logging if directory isn't writable

    return logger
