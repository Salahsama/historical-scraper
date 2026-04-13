"""
Async SQLite database layer for storing historical memecoin launch data.

Schema:
  - tokens:       One row per token (metadata + final stats)
  - price_ticks:  Price snapshots every 5s during first 5 minutes
  - trades:       Raw trades from PumpFun
  - dev_activity: Creator wallet sell/transfer events
  - scraper_stats: Operational metrics
"""
import time
import aiosqlite
from pathlib import Path
from logger import get_logger
from config import DB_PATH, DATA_DIR

log = get_logger("db")

# ─── Schema ─────────────────────────────────────────────────────────

SCHEMA = """
-- Core token metadata
CREATE TABLE IF NOT EXISTS tokens (
    mint            TEXT PRIMARY KEY,
    name            TEXT,
    symbol          TEXT,
    creator         TEXT,
    platform        TEXT,
    pair_address    TEXT,
    dex_id          TEXT,
    initial_liq_sol REAL,
    initial_liq_usd REAL,
    initial_mcap    REAL,
    bonding_pct     REAL,
    created_at      INTEGER,
    discovered_at   INTEGER,
    tracking_status TEXT DEFAULT 'pending',
    final_price_usd REAL,
    final_mcap      REAL,
    final_volume    REAL,
    total_buys_5m   INTEGER,
    total_sells_5m  INTEGER,
    dev_sold        INTEGER DEFAULT 0,
    dev_sold_pct    REAL,
    has_socials     INTEGER DEFAULT 0,
    source          TEXT
);

-- Price action ticks (~60 rows per token: 5s interval × 300s)
CREATE TABLE IF NOT EXISTS price_ticks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mint            TEXT NOT NULL,
    timestamp       INTEGER NOT NULL,
    price_usd       REAL,
    price_sol       REAL,
    volume_cum      REAL,
    mcap            REAL,
    buys_cum        INTEGER,
    sells_cum       INTEGER,
    liq_usd         REAL,
    tick_index      INTEGER,
    FOREIGN KEY (mint) REFERENCES tokens(mint)
);

-- Raw trades from PumpFun
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mint            TEXT NOT NULL,
    timestamp       INTEGER NOT NULL,
    tx_signature    TEXT,
    is_buy          INTEGER,
    sol_amount      REAL,
    token_amount    REAL,
    user_wallet     TEXT,
    FOREIGN KEY (mint) REFERENCES tokens(mint)
);

-- Dev wallet activity
CREATE TABLE IF NOT EXISTS dev_activity (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mint            TEXT NOT NULL,
    creator         TEXT NOT NULL,
    action          TEXT,
    timestamp       INTEGER,
    sol_amount      REAL,
    token_pct       REAL,
    tx_signature    TEXT,
    FOREIGN KEY (mint) REFERENCES tokens(mint)
);

-- Scraper operational stats
CREATE TABLE IF NOT EXISTS scraper_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL,
    tokens_discovered INTEGER,
    tokens_tracked  INTEGER,
    tokens_complete INTEGER,
    api_calls_total INTEGER,
    api_errors      INTEGER,
    rate_limit_hits INTEGER,
    uptime_hours    REAL
);

-- Indices for fast queries
CREATE INDEX IF NOT EXISTS idx_ticks_mint ON price_ticks(mint);
CREATE INDEX IF NOT EXISTS idx_ticks_ts ON price_ticks(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_mint ON trades(mint);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_dev_mint ON dev_activity(mint);
CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(tracking_status);
CREATE INDEX IF NOT EXISTS idx_tokens_created ON tokens(created_at);
CREATE INDEX IF NOT EXISTS idx_tokens_platform ON tokens(platform);
"""


class Database:
    """Async SQLite database manager for historical launch data."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self):
        """Initialize the database connection and schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))

        # Enable WAL mode for better concurrent read/write
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA cache_size=-64000")  # 64MB cache
        await self._db.execute("PRAGMA temp_store=MEMORY")

        # Create tables
        await self._db.executescript(SCHEMA)
        await self._db.commit()

        # Count existing records
        async with self._db.execute("SELECT COUNT(*) FROM tokens") as cur:
            count = (await cur.fetchone())[0]
        log.info(f"Database connected — {count} existing tokens in {self.db_path.name}")

    async def close(self):
        """Close the database connection."""
        if self._db:
            await self._db.commit()
            await self._db.close()
            self._db = None
            log.info("Database connection closed")

    # ─── Token Operations ───────────────────────────────────────────

    async def token_exists(self, mint: str) -> bool:
        """Check if a token is already in the database."""
        async with self._db.execute(
            "SELECT 1 FROM tokens WHERE mint = ?", (mint,)
        ) as cur:
            return (await cur.fetchone()) is not None

    async def insert_token(self, data: dict) -> bool:
        """Insert a new token. Returns True if inserted, False if already exists."""
        try:
            await self._db.execute(
                """INSERT OR IGNORE INTO tokens
                   (mint, name, symbol, creator, platform, pair_address, dex_id,
                    initial_liq_sol, initial_liq_usd, initial_mcap, bonding_pct,
                    created_at, discovered_at, tracking_status, has_socials, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
                (
                    data.get("mint", ""),
                    data.get("name", ""),
                    data.get("symbol", ""),
                    data.get("creator", ""),
                    data.get("platform", ""),
                    data.get("pair_address", ""),
                    data.get("dex_id", ""),
                    data.get("initial_liq_sol", 0.0),
                    data.get("initial_liq_usd", 0.0),
                    data.get("initial_mcap", 0.0),
                    data.get("bonding_pct", 0.0),
                    data.get("created_at", 0),
                    int(time.time()),
                    int(data.get("has_socials", False)),
                    data.get("source", ""),
                ),
            )
            await self._db.commit()
            return self._db.total_changes > 0
        except Exception as e:
            log.error(f"Failed to insert token {data.get('mint', '?')[:8]}: {e}")
            return False

    async def update_tracking_status(self, mint: str, status: str):
        """Update token tracking status: pending → tracking → complete/failed."""
        await self._db.execute(
            "UPDATE tokens SET tracking_status = ? WHERE mint = ?",
            (status, mint),
        )
        await self._db.commit()

    async def update_token_final(self, mint: str, data: dict):
        """Update token with final 5-minute snapshot data."""
        await self._db.execute(
            """UPDATE tokens SET
               final_price_usd = ?, final_mcap = ?, final_volume = ?,
               total_buys_5m = ?, total_sells_5m = ?,
               dev_sold = ?, dev_sold_pct = ?,
               tracking_status = 'complete'
               WHERE mint = ?""",
            (
                data.get("final_price_usd", 0.0),
                data.get("final_mcap", 0.0),
                data.get("final_volume", 0.0),
                data.get("total_buys_5m", 0),
                data.get("total_sells_5m", 0),
                int(data.get("dev_sold", False)),
                data.get("dev_sold_pct", 0.0),
                mint,
            ),
        )
        await self._db.commit()

    async def get_pending_tokens(self) -> list[dict]:
        """Get tokens stuck in 'pending' or 'tracking' state for cleanup."""
        async with self._db.execute(
            """SELECT mint, discovered_at FROM tokens
               WHERE tracking_status IN ('pending', 'tracking')
               ORDER BY discovered_at ASC"""
        ) as cur:
            rows = await cur.fetchall()
            return [{"mint": r[0], "discovered_at": r[1]} for r in rows]

    # ─── Price Tick Operations ──────────────────────────────────────

    async def insert_price_tick(self, mint: str, data: dict, tick_index: int):
        """Insert a single price tick observation."""
        try:
            await self._db.execute(
                """INSERT INTO price_ticks
                   (mint, timestamp, price_usd, price_sol, volume_cum,
                    mcap, buys_cum, sells_cum, liq_usd, tick_index)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    mint,
                    int(time.time()),
                    data.get("price_usd", 0.0),
                    data.get("price_sol", 0.0),
                    data.get("volume_cum", 0.0),
                    data.get("mcap", 0.0),
                    data.get("buys_cum", 0),
                    data.get("sells_cum", 0),
                    data.get("liq_usd", 0.0),
                    tick_index,
                ),
            )
            # Commit in batches — every 5 ticks
            if tick_index % 5 == 0:
                await self._db.commit()
        except Exception as e:
            log.debug(f"Failed to insert price tick for {mint[:8]}: {e}")

    async def insert_price_ticks_batch(self, ticks: list[tuple]):
        """Batch insert multiple price ticks for efficiency."""
        if not ticks:
            return
        try:
            await self._db.executemany(
                """INSERT INTO price_ticks
                   (mint, timestamp, price_usd, price_sol, volume_cum,
                    mcap, buys_cum, sells_cum, liq_usd, tick_index)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ticks,
            )
            await self._db.commit()
        except Exception as e:
            log.error(f"Batch tick insert failed: {e}")

    # ─── Trade Operations ───────────────────────────────────────────

    async def insert_trades(self, mint: str, trades: list[dict]):
        """Batch insert raw trades from PumpFun."""
        if not trades:
            return
        rows = []
        for t in trades:
            rows.append((
                mint,
                t.get("timestamp", int(time.time())),
                t.get("signature", ""),
                int(t.get("is_buy", True)),
                t.get("sol_amount", 0.0),
                t.get("token_amount", 0.0),
                t.get("user", ""),
            ))
        try:
            await self._db.executemany(
                """INSERT OR IGNORE INTO trades
                   (mint, timestamp, tx_signature, is_buy, sol_amount,
                    token_amount, user_wallet)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            await self._db.commit()
        except Exception as e:
            log.debug(f"Trade insert failed for {mint[:8]}: {e}")

    async def get_trade_signatures(self, mint: str) -> set[str]:
        """Get existing trade signatures to avoid duplicates."""
        async with self._db.execute(
            "SELECT tx_signature FROM trades WHERE mint = ? AND tx_signature != ''",
            (mint,),
        ) as cur:
            rows = await cur.fetchall()
            return {r[0] for r in rows}

    # ─── Dev Activity Operations ────────────────────────────────────

    async def insert_dev_activity(self, mint: str, data: dict):
        """Record dev wallet activity (sell, transfer, etc.)."""
        try:
            await self._db.execute(
                """INSERT INTO dev_activity
                   (mint, creator, action, timestamp, sol_amount,
                    token_pct, tx_signature)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    mint,
                    data.get("creator", ""),
                    data.get("action", ""),
                    data.get("timestamp", int(time.time())),
                    data.get("sol_amount", 0.0),
                    data.get("token_pct", 0.0),
                    data.get("tx_signature", ""),
                ),
            )
            await self._db.commit()
        except Exception as e:
            log.debug(f"Dev activity insert failed for {mint[:8]}: {e}")

    # ─── Stats Operations ──────────────────────────────────────────

    async def insert_stats(self, stats: dict):
        """Record scraper operational stats."""
        try:
            await self._db.execute(
                """INSERT INTO scraper_stats
                   (timestamp, tokens_discovered, tokens_tracked,
                    tokens_complete, api_calls_total, api_errors,
                    rate_limit_hits, uptime_hours)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    int(time.time()),
                    stats.get("tokens_discovered", 0),
                    stats.get("tokens_tracked", 0),
                    stats.get("tokens_complete", 0),
                    stats.get("api_calls_total", 0),
                    stats.get("api_errors", 0),
                    stats.get("rate_limit_hits", 0),
                    stats.get("uptime_hours", 0.0),
                ),
            )
            await self._db.commit()
        except Exception as e:
            log.debug(f"Stats insert failed: {e}")

    async def get_summary(self) -> dict:
        """Get database summary for monitoring."""
        summary = {}
        for table in ["tokens", "price_ticks", "trades", "dev_activity"]:
            async with self._db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                summary[table] = (await cur.fetchone())[0]

        # Status breakdown
        async with self._db.execute(
            "SELECT tracking_status, COUNT(*) FROM tokens GROUP BY tracking_status"
        ) as cur:
            summary["status_breakdown"] = dict(await cur.fetchall())

        return summary

    async def vacuum(self):
        """Optimize database (run during maintenance)."""
        try:
            await self._db.execute("PRAGMA optimize")
            log.debug("Database optimized")
        except Exception as e:
            log.debug(f"Vacuum failed: {e}")

    async def flush(self):
        """Force commit any pending writes."""
        if self._db:
            await self._db.commit()


# Global database instance
db = Database()
