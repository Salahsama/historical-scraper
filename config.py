"""
Historical Memecoin Launch Scraper — Configuration
All tunable parameters in one place.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─── Paths ───────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "launches.db"
LOG_DIR = DATA_DIR / "logs"

# ─── Scheduling Intervals (seconds) ─────────────────────────────────
DISCOVERY_INTERVAL = 10          # Poll for new tokens every 10s
TRACKING_DURATION = 300          # Track each token for 5 minutes (300s)
TRACKING_TICK_INTERVAL = 5       # Price snapshot every 5 seconds
TRADE_POLL_INTERVAL = 3          # Poll PumpFun trades every 3 seconds
DEV_CHECK_DELAY = 60             # Check dev wallet after 60s of tracking
MAINTENANCE_INTERVAL = 600       # Maintenance loop every 10 minutes
STATS_LOG_INTERVAL = 3600        # Log scraper stats every hour

# ─── Concurrency ────────────────────────────────────────────────────
MAX_CONCURRENT_TRACKERS = 20     # Max tokens tracked simultaneously
TRACKER_QUEUE_SIZE = 200         # Max queued tokens waiting to be tracked

# ─── API Base URLs ──────────────────────────────────────────────────
DEXSCREENER_BASE = "https://api.dexscreener.com"
PUMPFUN_BASE = "https://frontend-api-v3.pump.fun"
GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"
SOLANA_RPC_URL = os.getenv(
    "SOLANA_RPC_URL",
    "https://api.mainnet-beta.solana.com"
)

# ─── Rate Limits (conservative to avoid bans) ───────────────────────
RATE_LIMITS = {
    "dexscreener": {"requests_per_second": 4.0, "burst": 8},
    "pumpfun":     {"requests_per_second": 1.5, "burst": 4},
    "gecko":       {"requests_per_second": 0.4, "burst": 2},
    "solana_rpc":  {"requests_per_second": 2.0, "burst": 5},
}

# ─── Circuit Breaker ────────────────────────────────────────────────
CIRCUIT_BREAKER_THRESHOLD = 5    # Consecutive failures before circuit opens
CIRCUIT_BREAKER_COOLDOWN = 60    # Seconds to wait before retrying after trip

# ─── Execution Constraints (for backtesting metadata) ───────────────
# These represent the limits of the final Rust execution bot.
# Stored as metadata so backtesting queries can factor them in.
EXECUTION_RPC_LIMIT = 50         # 50 requests/sec
EXECUTION_TX_LIMIT = 5           # 5 sendTransaction/sec

# ─── Logging ─────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("SCRAPER_LOG_LEVEL", "INFO")
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB per log file
LOG_BACKUP_COUNT = 5              # Keep 5 rotated log files

# ─── User Agent Rotation ────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]
