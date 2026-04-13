"""
Smart rate limiter with token-bucket algorithm, exponential backoff,
circuit breaker, and per-source monitoring.

Designed for 24/7 operation against free APIs without getting banned.
"""
import asyncio
import time
import random
from collections import defaultdict
from logger import get_logger
from config import RATE_LIMITS, CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_COOLDOWN

log = get_logger("rate_limit")


class CircuitBreaker:
    """Circuit breaker to prevent hammering a failing API."""

    def __init__(self, threshold: int, cooldown: float):
        self.threshold = threshold
        self.cooldown = cooldown
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.is_open = False

    def record_success(self):
        """Reset failure count on success."""
        self.failure_count = 0
        if self.is_open:
            self.is_open = False
            log.info("Circuit breaker closed — API recovered")

    def record_failure(self):
        """Record a failure. Opens circuit if threshold exceeded."""
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        if self.failure_count >= self.threshold and not self.is_open:
            self.is_open = True
            log.warning(
                f"Circuit breaker OPEN — {self.failure_count} consecutive failures. "
                f"Cooling down {self.cooldown}s"
            )

    def can_proceed(self) -> bool:
        """Check if requests are allowed."""
        if not self.is_open:
            return True
        # Check if cooldown has elapsed
        elapsed = time.monotonic() - self.last_failure_time
        if elapsed >= self.cooldown:
            self.is_open = False
            self.failure_count = 0
            log.info("Circuit breaker cooldown elapsed — retrying")
            return True
        return False

    @property
    def remaining_cooldown(self) -> float:
        if not self.is_open:
            return 0.0
        elapsed = time.monotonic() - self.last_failure_time
        return max(0.0, self.cooldown - elapsed)


class RateLimiter:
    """Token-bucket rate limiter with circuit breaker per API source."""

    def __init__(self):
        self._buckets: dict[str, dict] = {}
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._breakers: dict[str, CircuitBreaker] = {}
        self._stats: dict[str, dict] = {}

        for source, cfg in RATE_LIMITS.items():
            self._buckets[source] = {
                "tokens": float(cfg["burst"]),
                "max_tokens": float(cfg["burst"]),
                "refill_rate": cfg["requests_per_second"],
                "last_refill": time.monotonic(),
            }
            self._breakers[source] = CircuitBreaker(
                CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_COOLDOWN
            )
            self._stats[source] = {
                "total_requests": 0,
                "successful": 0,
                "failed": 0,
                "rate_limited": 0,
                "circuit_trips": 0,
            }

    def _refill(self, source: str) -> None:
        """Refill tokens based on elapsed time."""
        bucket = self._buckets[source]
        now = time.monotonic()
        elapsed = now - bucket["last_refill"]
        added = elapsed * bucket["refill_rate"]
        bucket["tokens"] = min(bucket["max_tokens"], bucket["tokens"] + added)
        bucket["last_refill"] = now

    async def acquire(self, source: str) -> bool:
        """Wait until a request token is available. Returns False if circuit is open."""
        if source not in self._buckets:
            return True  # Unknown source — no limiting

        breaker = self._breakers[source]
        if not breaker.can_proceed():
            remaining = breaker.remaining_cooldown
            log.debug(f"[{source}] Circuit open — {remaining:.0f}s remaining")
            return False

        async with self._locks[source]:
            while True:
                self._refill(source)
                bucket = self._buckets[source]
                if bucket["tokens"] >= 1.0:
                    bucket["tokens"] -= 1.0
                    return True
                # Wait for one token to refill + jitter
                wait_time = (1.0 - bucket["tokens"]) / bucket["refill_rate"]
                wait_time += random.uniform(0.01, 0.15)
                await asyncio.sleep(wait_time)

    async def safe_request(self, source: str, coro_fn, max_retries: int = 4):
        """
        Execute an async callable with rate limiting, exponential backoff,
        and circuit breaker protection.

        Args:
            source: API source name (must match RATE_LIMITS key)
            coro_fn: Async callable (no args) that performs the request
            max_retries: Maximum retry attempts

        Returns:
            The result of coro_fn(), or None on failure
        """
        breaker = self._breakers.get(source)
        stats = self._stats.get(source, {})

        for attempt in range(max_retries):
            # Check circuit breaker
            if breaker and not breaker.can_proceed():
                stats["circuit_trips"] = stats.get("circuit_trips", 0) + 1
                return None

            # Acquire rate limit token
            can = await self.acquire(source)
            if not can:
                return None

            stats["total_requests"] = stats.get("total_requests", 0) + 1

            try:
                result = await coro_fn()
                if breaker:
                    breaker.record_success()
                stats["successful"] = stats.get("successful", 0) + 1
                return result

            except RateLimitError:
                stats["rate_limited"] = stats.get("rate_limited", 0) + 1
                if breaker:
                    breaker.record_failure()
                wait = (2 ** attempt) * 2 + random.uniform(1.0, 3.0)
                log.warning(f"[{source}] Rate limited — backoff {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                await asyncio.sleep(wait)

            except Exception as e:
                stats["failed"] = stats.get("failed", 0) + 1
                if breaker:
                    breaker.record_failure()
                if attempt == max_retries - 1:
                    log.error(f"[{source}] Request failed after {max_retries} retries: {e}")
                    return None
                wait = (2 ** attempt) + random.uniform(0.1, 0.5)
                log.debug(f"[{source}] Retry {attempt+1}/{max_retries} after {wait:.1f}s: {e}")
                await asyncio.sleep(wait)

        return None

    def get_stats(self) -> dict:
        """Return per-source request statistics."""
        return dict(self._stats)

    def get_health(self) -> dict:
        """Return health status per source."""
        health = {}
        for source in self._buckets:
            breaker = self._breakers[source]
            health[source] = {
                "circuit_open": breaker.is_open,
                "failure_count": breaker.failure_count,
                "cooldown_remaining": breaker.remaining_cooldown,
                "tokens_available": self._buckets[source]["tokens"],
            }
        return health


class RateLimitError(Exception):
    """Raised when an API returns 429 Too Many Requests."""
    pass


# Global singleton
rate_limiter = RateLimiter()
