"""
Historical Memecoin Launch Scraper — Main Orchestrator

A resilient 24/7 bot that collects historical data for Solana memecoin launches
(PumpFun + Raydium) using ONLY free APIs. Data is stored in SQLite for
backtesting a sniping strategy.

Architecture:
  1. Discovery Loop (10s)   — Polls PumpFun/GeckoTerminal/DexScreener for new tokens
  2. Tracker Manager         — Spawns per-token 5-minute tracking coroutines (max 20)
  3. Maintenance Loop (10m)  — DB cleanup, stats logging, health monitoring

Run:
  python main.py

Constraints this data is optimized for:
  - Execution RPC: 50 req/s
  - Execution TX:  5 sendTransaction/s
"""
import asyncio
import signal
import sys
import time
from datetime import datetime, timezone

import aiohttp

from config import (
    DISCOVERY_INTERVAL,
    MAINTENANCE_INTERVAL,
    STATS_LOG_INTERVAL,
    MAX_CONCURRENT_TRACKERS,
    TRACKER_QUEUE_SIZE,
    DATA_DIR,
)
from db import db
from scraper import TokenDiscovery, TokenTracker
from rate_limiter import rate_limiter
from logger import get_logger

log = get_logger("main")

# ─── Banner ──────────────────────────────────────────────────────────
BANNER = """
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║   ██╗  ██╗██╗███████╗████████╗ ██████╗ ██████╗ ██╗ ██████╗ █████╗   ║
║   ██║  ██║██║██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗██║██╔════╝██╔══██╗  ║
║   ███████║██║███████╗   ██║   ██║   ██║██████╔╝██║██║     ███████║   ║
║   ██╔══██║██║╚════██║   ██║   ██║   ██║██╔══██╗██║██║     ██╔══██║   ║
║   ██║  ██║██║███████║   ██║   ╚██████╔╝██║  ██║██║╚██████╗██║  ██║   ║
║   ╚═╝  ╚═╝╚═╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═╝ ╚═════╝╚═╝  ╚═╝  ║
║                                                                      ║
║   Historical Memecoin Launch Scraper                                 ║
║   ─────────────────────────────────                                  ║
║   Free APIs • 24/7 VPS • SQLite Storage                              ║
║   PumpFun + Raydium → Backtesting Data                               ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""


class HistoricalScraper:
    """Main orchestrator for the historical data scraper."""

    def __init__(self):
        self.running = True
        self.discovery = TokenDiscovery()
        self.tracker = TokenTracker()
        self._start_time = time.time()

        # Token queue: new tokens waiting to be tracked
        self._token_queue: asyncio.Queue = asyncio.Queue(maxsize=TRACKER_QUEUE_SIZE)

        # Semaphore to limit concurrent trackers
        self._tracker_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TRACKERS)

        # Stats
        self._stats = {
            "tokens_discovered": 0,
            "tokens_queued": 0,
            "tracker_spawns": 0,
            "queue_drops": 0,
            "discovery_cycles": 0,
        }

    async def run(self):
        """Main entry point — initialize and start all loops."""
        print(BANNER)
        log.info("Starting Historical Memecoin Launch Scraper")
        log.info(f"Data directory: {DATA_DIR}")
        log.info(f"Discovery interval: {DISCOVERY_INTERVAL}s")
        log.info(f"Max concurrent trackers: {MAX_CONCURRENT_TRACKERS}")

        # Initialize database
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        await db.connect()

        # Load known mints to avoid re-tracking
        await self.discovery.load_known_mints()

        # Set up graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown)

        # Create shared HTTP session
        connector = aiohttp.TCPConnector(
            limit=30,               # Max concurrent connections
            ttl_dns_cache=300,       # DNS cache: 5 minutes
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=20)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            log.info("🚀 All systems go — starting scraper loops")

            # Run all loops concurrently
            await asyncio.gather(
                self._discovery_loop(session),
                self._tracker_manager(session),
                self._maintenance_loop(),
                self._stats_loop(),
                return_exceptions=True,
            )

        # Cleanup
        await db.close()
        log.info("Scraper shut down cleanly")

    def _shutdown(self):
        """Graceful shutdown handler."""
        log.info("🛑 Shutdown signal received — finishing active trackers...")
        self.running = False

    # ─── Discovery Loop ─────────────────────────────────────────────

    async def _discovery_loop(self, session: aiohttp.ClientSession):
        """Poll all sources for new tokens every DISCOVERY_INTERVAL seconds."""
        log.info("🔍 Discovery loop started")

        while self.running:
            try:
                new_tokens = await self.discovery.discover(session)

                for token_data in new_tokens:
                    self._stats["tokens_discovered"] += 1
                    try:
                        self._token_queue.put_nowait(token_data)
                        self._stats["tokens_queued"] += 1
                    except asyncio.QueueFull:
                        self._stats["queue_drops"] += 1
                        log.warning(
                            f"Queue full — dropping {token_data.get('symbol', '?')} "
                            f"({token_data['mint'][:8]}...)"
                        )

                if new_tokens:
                    log.info(
                        f"📡 Discovered {len(new_tokens)} new tokens | "
                        f"Queue: {self._token_queue.qsize()}/{TRACKER_QUEUE_SIZE}"
                    )

                self._stats["discovery_cycles"] += 1

            except Exception as e:
                log.error(f"Discovery loop error: {e}")

            await asyncio.sleep(DISCOVERY_INTERVAL)

        log.info("Discovery loop stopped")

    # ─── Tracker Manager ────────────────────────────────────────────

    async def _tracker_manager(self, session: aiohttp.ClientSession):
        """
        Continuously pulls tokens from the queue and spawns tracking coroutines.
        Limited to MAX_CONCURRENT_TRACKERS concurrent trackers.
        """
        log.info(f"📈 Tracker manager started (max {MAX_CONCURRENT_TRACKERS} concurrent)")

        active_tasks: set[asyncio.Task] = set()

        while self.running or not self._token_queue.empty():
            # Clean up finished tasks
            done = {t for t in active_tasks if t.done()}
            for t in done:
                active_tasks.discard(t)
                # Re-raise exceptions from tasks (for logging)
                try:
                    t.result()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log.debug(f"Tracker task exception: {e}")

            # Spawn new trackers if capacity available
            try:
                token_data = await asyncio.wait_for(
                    self._token_queue.get(), timeout=2.0
                )
            except asyncio.TimeoutError:
                continue

            self._stats["tracker_spawns"] += 1

            # Create tracker coroutine with semaphore
            async def _track_with_semaphore(td):
                async with self._tracker_semaphore:
                    await self.tracker.track(td["mint"], td, session)

            task = asyncio.create_task(
                _track_with_semaphore(token_data),
                name=f"track_{token_data.get('symbol', '?')}_{token_data['mint'][:8]}",
            )
            active_tasks.add(task)

        # Wait for active trackers to finish on shutdown
        if active_tasks:
            log.info(f"Waiting for {len(active_tasks)} active trackers to finish...")
            await asyncio.gather(*active_tasks, return_exceptions=True)

        log.info("Tracker manager stopped")

    # ─── Maintenance Loop ───────────────────────────────────────────

    async def _maintenance_loop(self):
        """Periodic maintenance: DB optimization, stale tracker cleanup."""
        log.info("🔧 Maintenance loop started")
        await asyncio.sleep(MAINTENANCE_INTERVAL)  # Wait before first run

        while self.running:
            try:
                # Database optimization
                await db.vacuum()

                # Database summary
                summary = await db.get_summary()
                log.info(
                    f"🗄️  DB: tokens={summary.get('tokens', 0)} | "
                    f"ticks={summary.get('price_ticks', 0)} | "
                    f"trades={summary.get('trades', 0)} | "
                    f"dev_acts={summary.get('dev_activity', 0)}"
                )

                status = summary.get("status_breakdown", {})
                if status:
                    log.info(f"   Status: {status}")

                # Mark very old 'tracking' tokens as failed (>10 minutes)
                stale = await db.get_pending_tokens()
                now = int(time.time())
                for t in stale:
                    if now - t.get("discovered_at", now) > 600:
                        await db.update_tracking_status(t["mint"], "failed")

                # Record stats
                uptime_hours = (time.time() - self._start_time) / 3600
                rl_stats = rate_limiter.get_stats()
                total_api = sum(s.get("total_requests", 0) for s in rl_stats.values())
                total_errors = sum(s.get("failed", 0) for s in rl_stats.values())
                total_rl = sum(s.get("rate_limited", 0) for s in rl_stats.values())

                await db.insert_stats({
                    "tokens_discovered": self._stats["tokens_discovered"],
                    "tokens_tracked": self.tracker.stats["completed"],
                    "tokens_complete": self.tracker.stats["completed"],
                    "api_calls_total": total_api,
                    "api_errors": total_errors,
                    "rate_limit_hits": total_rl,
                    "uptime_hours": uptime_hours,
                })

            except Exception as e:
                log.error(f"Maintenance error: {e}")

            await asyncio.sleep(MAINTENANCE_INTERVAL)

        log.info("Maintenance loop stopped")

    # ─── Stats Logging Loop ─────────────────────────────────────────

    async def _stats_loop(self):
        """Log comprehensive stats every hour."""
        await asyncio.sleep(60)  # Wait a minute before first log

        while self.running:
            try:
                uptime = time.time() - self._start_time
                hours = uptime / 3600
                mins = uptime / 60

                # Scraper stats
                tracker_stats = self.tracker.stats
                discovery_stats = self.discovery.stats

                # Rate limiter health
                rl_health = rate_limiter.get_health()
                rl_stats = rate_limiter.get_stats()

                log.info("=" * 60)
                log.info(f"📊 SCRAPER STATS — Uptime: {hours:.1f}h ({mins:.0f}min)")
                log.info(
                    f"   Discovery: {self._stats['tokens_discovered']} found | "
                    f"{self._stats['discovery_cycles']} cycles | "
                    f"{self._stats['queue_drops']} dropped"
                )
                log.info(
                    f"   Tracking:  {tracker_stats['active']} active | "
                    f"{tracker_stats['completed']} complete | "
                    f"{tracker_stats['failed']} failed"
                )

                # Per-source API health
                for source, health in rl_health.items():
                    stats = rl_stats.get(source, {})
                    circuit = "🔴 OPEN" if health["circuit_open"] else "🟢 OK"
                    log.info(
                        f"   [{source:12s}] {circuit} | "
                        f"reqs={stats.get('total_requests', 0)} | "
                        f"ok={stats.get('successful', 0)} | "
                        f"429s={stats.get('rate_limited', 0)} | "
                        f"err={stats.get('failed', 0)}"
                    )

                # Queue status
                log.info(
                    f"   Queue: {self._token_queue.qsize()}/{TRACKER_QUEUE_SIZE}"
                )
                log.info("=" * 60)

            except Exception as e:
                log.error(f"Stats loop error: {e}")

            await asyncio.sleep(STATS_LOG_INTERVAL)


# ─── Entry Point ─────────────────────────────────────────────────────

def main():
    scraper = HistoricalScraper()
    try:
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.critical(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
