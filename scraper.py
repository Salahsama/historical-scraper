"""
Historical Memecoin Launch Scraper — Core Scraping Engine

Three components:
  1. TokenDiscovery  — Polls PumpFun, GeckoTerminal, DexScreener for new launches
  2. TokenTracker    — Tracks each token for 5 minutes (price ticks + trades)
  3. DevWalletChecker — Checks if the creator sold immediately

All requests go through rate_limiter for safety. No paid APIs.
"""
import asyncio
import time
import random
import json
from typing import Optional

import aiohttp

from config import (
    DEXSCREENER_BASE, PUMPFUN_BASE, GECKOTERMINAL_BASE, SOLANA_RPC_URL,
    TRACKING_DURATION, TRACKING_TICK_INTERVAL, TRADE_POLL_INTERVAL,
    DEV_CHECK_DELAY, USER_AGENTS, MAX_CONCURRENT_TRACKERS,
)
from rate_limiter import rate_limiter, RateLimitError
from db import db
from logger import get_logger

log = get_logger("scraper")


# ─── Helpers ─────────────────────────────────────────────────────────

def _headers(accept: str = "application/json") -> dict:
    """Generate request headers with randomized User-Agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": accept,
    }


def _gecko_headers() -> dict:
    """GeckoTerminal wants a specific Accept header for versioning."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json;version=20230302",
    }


# ═══════════════════════════════════════════════════════════════════════
# 1. TOKEN DISCOVERY
# ═══════════════════════════════════════════════════════════════════════

class TokenDiscovery:
    """Polls free APIs for new token launches and enqueues them for tracking."""

    def __init__(self):
        self._seen_mints: set[str] = set()
        self._stats = {"discovered": 0, "duplicates": 0, "errors": 0}

    async def load_known_mints(self):
        """Load already-known mints from DB to avoid re-tracking."""
        try:
            pending = await db.get_pending_tokens()
            for t in pending:
                self._seen_mints.add(t["mint"])
            # Also load completed tokens
            # (we query raw SQL via the db connection)
            async with db._db.execute(
                "SELECT mint FROM tokens"
            ) as cur:
                rows = await cur.fetchall()
                for r in rows:
                    self._seen_mints.add(r[0])
            log.info(f"Loaded {len(self._seen_mints)} known mints from database")
        except Exception as e:
            log.warning(f"Failed to load known mints: {e}")

    async def discover(
        self, session: aiohttp.ClientSession
    ) -> list[dict]:
        """
        Poll all sources for new tokens.
        Returns list of new token dicts ready for tracking.
        """
        new_tokens = []

        # Source 1: PumpFun latest launches
        pf_tokens = await self._discover_pumpfun(session)
        new_tokens.extend(pf_tokens)

        # Source 2: GeckoTerminal new pools
        gecko_tokens = await self._discover_geckoterminal(session)
        new_tokens.extend(gecko_tokens)

        # Source 3: DexScreener boosted tokens
        dex_tokens = await self._discover_dexscreener(session)
        new_tokens.extend(dex_tokens)

        return new_tokens

    # ─── PumpFun ────────────────────────────────────────────────────

    async def _discover_pumpfun(
        self, session: aiohttp.ClientSession
    ) -> list[dict]:
        """Fetch latest launches from PumpFun."""
        url = f"{PUMPFUN_BASE}/coins"
        params = {
            "limit": 50,
            "offset": 0,
            "sort": "created_timestamp",
            "order": "DESC",
            "includeNsfw": "false",
        }

        async def _fetch():
            async with session.get(
                url, params=params, headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                elif resp.status == 429:
                    raise RateLimitError("PumpFun rate limited")
                return []

        try:
            data = await rate_limiter.safe_request("pumpfun", _fetch)
            if not isinstance(data, list):
                return []

            new_tokens = []
            for item in data:
                mint = item.get("mint", "")
                if not mint or mint in self._seen_mints:
                    continue

                self._seen_mints.add(mint)
                self._stats["discovered"] += 1

                # Parse PumpFun data
                virtual_sol = item.get("virtual_sol_reserves", 0)
                real_sol = item.get("real_sol_reserves", 0)
                bonding_pct = min(100.0, (real_sol / 85_000_000_000) * 100) if real_sol else 0.0

                token_data = {
                    "mint": mint,
                    "name": item.get("name", ""),
                    "symbol": item.get("symbol", ""),
                    "creator": item.get("creator", ""),
                    "platform": "pumpfun",
                    "pair_address": "",
                    "dex_id": "pumpfun",
                    "initial_liq_sol": virtual_sol / 1e9 if virtual_sol else 0.0,
                    "initial_liq_usd": float(item.get("usd_market_cap", 0) or 0),
                    "initial_mcap": float(item.get("usd_market_cap", 0) or 0),
                    "bonding_pct": bonding_pct,
                    "created_at": item.get("created_timestamp", 0),
                    "has_socials": bool(
                        item.get("website") or item.get("twitter") or item.get("telegram")
                    ),
                    "source": "pumpfun",
                }
                new_tokens.append(token_data)

            return new_tokens

        except Exception as e:
            self._stats["errors"] += 1
            log.debug(f"PumpFun discovery error: {e}")
            return []

    # ─── GeckoTerminal ──────────────────────────────────────────────

    async def _discover_geckoterminal(
        self, session: aiohttp.ClientSession
    ) -> list[dict]:
        """Fetch new Solana pools from GeckoTerminal."""
        url = f"{GECKOTERMINAL_BASE}/networks/solana/new_pools"

        async def _fetch():
            async with session.get(
                url, headers=_gecko_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                elif resp.status == 429:
                    raise RateLimitError("GeckoTerminal rate limited")
                return None

        try:
            data = await rate_limiter.safe_request("gecko", _fetch)
            if not data or "data" not in data:
                return []

            new_tokens = []
            for pool in data.get("data", []):
                attrs = pool.get("attributes", {})
                rels = pool.get("relationships", {})

                # Extract base token address
                base_token = rels.get("base_token", {}).get("data", {})
                token_id = base_token.get("id", "")  # format: "solana_<address>"
                mint = token_id.replace("solana_", "") if token_id.startswith("solana_") else ""

                if not mint or mint in self._seen_mints:
                    continue

                self._seen_mints.add(mint)
                self._stats["discovered"] += 1

                # Parse pool data
                pool_addr = attrs.get("address", "")
                reserve_usd = float(attrs.get("reserve_in_usd", 0) or 0)

                # Get the DEX name from relationships
                dex_data = rels.get("dex", {}).get("data", {})
                dex_id = dex_data.get("id", "").replace("solana_", "")

                token_data = {
                    "mint": mint,
                    "name": attrs.get("name", "").split(" / ")[0] if attrs.get("name") else "",
                    "symbol": "",  # GeckoTerminal pool names are "TOKEN / SOL"
                    "creator": "",
                    "platform": "raydium" if "raydium" in dex_id.lower() else dex_id,
                    "pair_address": pool_addr,
                    "dex_id": dex_id,
                    "initial_liq_sol": 0.0,  # Will be updated from DexScreener
                    "initial_liq_usd": reserve_usd,
                    "initial_mcap": float(attrs.get("market_cap_usd") or 0),
                    "bonding_pct": 0.0,
                    "created_at": 0,  # Parse from pool_created_at if available
                    "has_socials": False,
                    "source": "geckoterminal",
                }

                # Parse creation timestamp
                created_str = attrs.get("pool_created_at", "")
                if created_str:
                    try:
                        from datetime import datetime, timezone
                        dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                        token_data["created_at"] = int(dt.timestamp())
                    except (ValueError, TypeError):
                        token_data["created_at"] = int(time.time())

                new_tokens.append(token_data)

            return new_tokens

        except Exception as e:
            self._stats["errors"] += 1
            log.debug(f"GeckoTerminal discovery error: {e}")
            return []

    # ─── DexScreener ────────────────────────────────────────────────

    async def _discover_dexscreener(
        self, session: aiohttp.ClientSession
    ) -> list[dict]:
        """Fetch boosted/trending tokens from DexScreener as a discovery source."""
        url = f"{DEXSCREENER_BASE}/token-boosts/top/v1"

        async def _fetch():
            async with session.get(
                url, headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                elif resp.status == 429:
                    raise RateLimitError("DexScreener rate limited")
                return []

        try:
            data = await rate_limiter.safe_request("dexscreener", _fetch)
            if not isinstance(data, list):
                return []

            # Filter Solana tokens only
            solana_tokens = [t for t in data if t.get("chainId") == "solana"]

            new_tokens = []
            for item in solana_tokens:
                mint = item.get("tokenAddress", "")
                if not mint or mint in self._seen_mints:
                    continue

                self._seen_mints.add(mint)
                self._stats["discovered"] += 1

                token_data = {
                    "mint": mint,
                    "name": item.get("name", ""),
                    "symbol": item.get("symbol", ""),
                    "creator": "",
                    "platform": "unknown",  # Will be determined during tracking
                    "pair_address": "",
                    "dex_id": "",
                    "initial_liq_sol": 0.0,
                    "initial_liq_usd": 0.0,
                    "initial_mcap": 0.0,
                    "bonding_pct": 0.0,
                    "created_at": int(time.time()),
                    "has_socials": bool(item.get("links")),
                    "source": "dexscreener",
                }
                new_tokens.append(token_data)

            return new_tokens

        except Exception as e:
            self._stats["errors"] += 1
            log.debug(f"DexScreener discovery error: {e}")
            return []

    @property
    def stats(self) -> dict:
        return dict(self._stats)


# ═══════════════════════════════════════════════════════════════════════
# 2. TOKEN TRACKER
# ═══════════════════════════════════════════════════════════════════════

class TokenTracker:
    """Tracks a single token for TRACKING_DURATION seconds, collecting price ticks and trades."""

    def __init__(self):
        self._active_count = 0
        self._completed_count = 0
        self._failed_count = 0

    async def track(self, mint: str, token_data: dict, session: aiohttp.ClientSession):
        """
        Track a single token for 5 minutes.

        1. Insert token into DB
        2. Mark as 'tracking'
        3. Poll price every 5s, trades every 3s
        4. After 60s, check dev wallet
        5. After 300s, write final snapshot, mark complete
        """
        self._active_count += 1
        platform = token_data.get("platform", "")
        symbol = token_data.get("symbol", "") or token_data.get("name", "")[:8]

        try:
            # Insert token into DB
            inserted = await db.insert_token(token_data)
            if not inserted:
                log.debug(f"Token {mint[:8]} already in DB — skipping")
                self._active_count -= 1
                return

            await db.update_tracking_status(mint, "tracking")
            log.info(f"📈 Tracking [{symbol}] {mint[:12]}... ({platform}) for {TRACKING_DURATION}s")

            start_time = time.time()
            tick_index = 0
            dev_checked = False
            creator = token_data.get("creator", "")

            # Track seen trade signatures to avoid duplicates
            seen_sigs: set[str] = set()

            # Resolve pool address for trade fetching (GeckoTerminal needs it)
            pool_address = token_data.get("pair_address", "")

            # Cumulative counters
            cum_buys = 0
            cum_sells = 0
            cum_volume = 0.0
            last_price_usd = 0.0
            last_mcap = 0.0
            last_liq = 0.0

            # Two interleaved loops: price ticks (5s) and trades (3s)
            # We use a single loop with the smaller interval and alternate
            iteration = 0

            while time.time() - start_time < TRACKING_DURATION:
                elapsed = time.time() - start_time
                iteration += 1

                # ── Price tick (every 5s) ────────────────────────────
                if iteration % 2 == 0 or iteration == 1:  # ~every 6s with the 3s sleep
                    price_data = await self._fetch_price(mint, session)
                    if price_data:
                        last_price_usd = price_data.get("price_usd", last_price_usd)
                        last_mcap = price_data.get("mcap", last_mcap)
                        last_liq = price_data.get("liq_usd", last_liq)

                        buys = price_data.get("buys_5m", 0)
                        sells = price_data.get("sells_5m", 0)
                        vol = price_data.get("volume_5m", 0.0)

                        # Use 5m counters from DexScreener as cumulative within our window
                        cum_buys = buys
                        cum_sells = sells
                        cum_volume = vol

                        tick_data = {
                            "price_usd": last_price_usd,
                            "price_sol": price_data.get("price_sol", 0.0),
                            "volume_cum": cum_volume,
                            "mcap": last_mcap,
                            "buys_cum": cum_buys,
                            "sells_cum": cum_sells,
                            "liq_usd": last_liq,
                        }
                        await db.insert_price_tick(mint, tick_data, tick_index)
                        tick_index += 1

                # ── Trades (strategic fetches at 60s and 240s) ──────
                # GeckoTerminal returns 300 trades/call, so 2 fetches
                # gives full coverage without hitting rate limits.
                # Resolve pool address once after first price fetch
                if not pool_address and price_data:
                    pool_address = await self._resolve_pool_address(mint, session)

                # Fetch trades at ~60s and ~240s marks
                trade_fetch_times = {60, 240}
                elapsed_rounded = int(elapsed)
                if pool_address and elapsed_rounded in trade_fetch_times:
                    new_trades = await self._fetch_trades_gecko(
                        mint, pool_address, seen_sigs, session
                    )
                    if new_trades:
                        await db.insert_trades(mint, new_trades)
                        for t in new_trades:
                            sig = t.get("signature", "")
                            if sig:
                                seen_sigs.add(sig)

                # ── Dev wallet check (after 60s) ────────────────────
                if not dev_checked and elapsed >= DEV_CHECK_DELAY and creator:
                    dev_checked = True
                    asyncio.create_task(
                        self._check_dev_wallet(mint, creator, session)
                    )

                # Sleep until next iteration
                await asyncio.sleep(TRADE_POLL_INTERVAL)

            # ── Final snapshot ──────────────────────────────────────
            final_data = {
                "final_price_usd": last_price_usd,
                "final_mcap": last_mcap,
                "final_volume": cum_volume,
                "total_buys_5m": cum_buys,
                "total_sells_5m": cum_sells,
                "dev_sold": 0,
                "dev_sold_pct": 0.0,
            }

            # Check if dev sold (from dev_activity table)
            try:
                async with db._db.execute(
                    "SELECT COUNT(*), SUM(token_pct) FROM dev_activity WHERE mint = ? AND action = 'sell'",
                    (mint,),
                ) as cur:
                    row = await cur.fetchone()
                    if row and row[0] > 0:
                        final_data["dev_sold"] = 1
                        final_data["dev_sold_pct"] = row[1] or 0.0
            except Exception:
                pass

            await db.update_token_final(mint, final_data)
            await db.flush()
            self._completed_count += 1
            self._active_count -= 1

            log.info(
                f"✅ Complete [{symbol}] {mint[:12]}... — "
                f"${last_price_usd:.6f} | {tick_index} ticks | "
                f"{len(seen_sigs)} trades | dev_sold={final_data['dev_sold']}"
            )

        except asyncio.CancelledError:
            await db.update_tracking_status(mint, "failed")
            self._active_count -= 1
            raise

        except Exception as e:
            log.error(f"Tracker error for {mint[:12]}: {e}")
            await db.update_tracking_status(mint, "failed")
            self._failed_count += 1
            self._active_count -= 1

    # ─── Price Fetching (DexScreener) ───────────────────────────────

    async def _fetch_price(
        self, mint: str, session: aiohttp.ClientSession
    ) -> Optional[dict]:
        """Fetch current price/volume data from DexScreener."""
        url = f"{DEXSCREENER_BASE}/tokens/v1/solana/{mint}"

        async def _fetch():
            async with session.get(
                url, headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data
                elif resp.status == 429:
                    raise RateLimitError("DexScreener rate limited")
                return None

        try:
            data = await rate_limiter.safe_request("dexscreener", _fetch, max_retries=2)
            if not data:
                return None

            # DexScreener returns a list of pairs for this token
            pairs = data if isinstance(data, list) else [data]

            if not pairs:
                return None

            # Pick the pair with highest liquidity
            best_pair = max(
                pairs,
                key=lambda p: float(
                    (p.get("liquidity") or {}).get("usd", 0) or 0
                ),
                default=None,
            )
            if not best_pair:
                return None

            volume = best_pair.get("volume", {})
            txns = best_pair.get("txns", {})
            txns_5m = txns.get("m5", {})
            liq = best_pair.get("liquidity", {})

            return {
                "price_usd": float(best_pair.get("priceUsd", 0) or 0),
                "price_sol": float(best_pair.get("priceNative", 0) or 0),
                "volume_5m": float(volume.get("m5", 0) or 0),
                "mcap": float(best_pair.get("marketCap", 0) or 0),
                "buys_5m": int(txns_5m.get("buys", 0) or 0),
                "sells_5m": int(txns_5m.get("sells", 0) or 0),
                "liq_usd": float(liq.get("usd", 0) or 0),
            }

        except Exception as e:
            log.debug(f"Price fetch error for {mint[:8]}: {e}")
            return None

    # ─── Pool Address Resolution ─────────────────────────────────────

    async def _resolve_pool_address(
        self, mint: str, session: aiohttp.ClientSession
    ) -> str:
        """Try to find the pool address for a token via DexScreener."""
        url = f"{DEXSCREENER_BASE}/tokens/v1/solana/{mint}"

        async def _fetch():
            async with session.get(
                url, headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                return None

        try:
            data = await rate_limiter.safe_request("dexscreener", _fetch, max_retries=1)
            if not data:
                return ""
            pairs = data if isinstance(data, list) else [data]
            if pairs:
                addr = pairs[0].get("pairAddress", "")
                if addr:
                    # Also update the DB with the pair address
                    try:
                        await db._db.execute(
                            "UPDATE tokens SET pair_address = ? WHERE mint = ? AND pair_address = ''",
                            (addr, mint),
                        )
                        await db._db.commit()
                    except Exception:
                        pass
                return addr
            return ""
        except Exception:
            return ""

    # ─── Trade Fetching (GeckoTerminal) ─────────────────────────────

    async def _fetch_trades_gecko(
        self,
        mint: str,
        pool_address: str,
        seen_sigs: set[str],
        session: aiohttp.ClientSession,
    ) -> list[dict]:
        """
        Fetch raw trades from GeckoTerminal for any Solana pool.
        Returns up to 300 trades with tx_hash, wallet, amounts, and buy/sell side.
        """
        url = f"{GECKOTERMINAL_BASE}/networks/solana/pools/{pool_address}/trades"

        async def _fetch():
            async with session.get(
                url, headers=_gecko_headers(),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                elif resp.status == 429:
                    raise RateLimitError("GeckoTerminal rate limited")
                return None

        try:
            data = await rate_limiter.safe_request("gecko", _fetch, max_retries=2)
            if not data or "data" not in data:
                return []

            new_trades = []
            for trade in data.get("data", []):
                attrs = trade.get("attributes", {})
                tx_hash = attrs.get("tx_hash", "")

                if tx_hash and tx_hash in seen_sigs:
                    continue

                kind = attrs.get("kind", "")  # "buy" or "sell"
                is_buy = kind == "buy"

                # Parse amounts
                from_amount = float(attrs.get("from_token_amount", 0) or 0)
                to_amount = float(attrs.get("to_token_amount", 0) or 0)
                volume_usd = float(attrs.get("volume_in_usd", 0) or 0)

                # Determine SOL amount based on buy/sell direction
                # For buys: from=SOL, to=token. For sells: from=token, to=SOL
                sol_amount = from_amount if is_buy else to_amount
                token_amount = to_amount if is_buy else from_amount

                # Parse timestamp
                ts_str = attrs.get("block_timestamp", "")
                try:
                    from datetime import datetime as dt_cls
                    ts_dt = dt_cls.fromisoformat(ts_str.replace("Z", "+00:00"))
                    timestamp = int(ts_dt.timestamp())
                except (ValueError, TypeError, AttributeError):
                    timestamp = int(time.time())

                new_trades.append({
                    "signature": tx_hash,
                    "timestamp": timestamp,
                    "is_buy": is_buy,
                    "sol_amount": sol_amount,
                    "token_amount": token_amount,
                    "user": attrs.get("tx_from_address", ""),
                })

            return new_trades

        except Exception as e:
            log.debug(f"GeckoTerminal trade fetch error for {mint[:8]}: {e}")
            return []

    # ─── Dev Wallet Checking (Solana RPC) ───────────────────────────

    async def _check_dev_wallet(
        self, mint: str, creator: str, session: aiohttp.ClientSession
    ):
        """
        Check if the creator wallet sold tokens in the first minutes.
        Uses Solana public RPC getSignaturesForAddress.
        """
        if not creator:
            return

        async def _fetch_sigs():
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    creator,
                    {"limit": 20}
                ],
            }
            async with session.post(
                SOLANA_RPC_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data.get("result", [])
                elif resp.status == 429:
                    raise RateLimitError("Solana RPC rate limited")
                return []

        try:
            signatures = await rate_limiter.safe_request(
                "solana_rpc", _fetch_sigs, max_retries=2
            )
            if not signatures:
                return

            # Check each recent transaction for token transfers
            for sig_info in signatures[:10]:
                sig = sig_info.get("signature", "")
                if not sig:
                    continue

                # Fetch transaction details
                tx_data = await self._fetch_transaction(sig, session)
                if not tx_data:
                    continue

                # Look for token transfer instructions from this creator
                meta = tx_data.get("meta", {})
                if not meta:
                    continue

                # Check post-token balances for signs of selling
                pre_balances = meta.get("preTokenBalances", [])
                post_balances = meta.get("postTokenBalances", [])

                for pre in pre_balances:
                    if pre.get("owner") == creator and pre.get("mint") == mint:
                        pre_amount = float(
                            pre.get("uiTokenAmount", {}).get("uiAmount", 0) or 0
                        )
                        # Find matching post balance
                        for post in post_balances:
                            if post.get("owner") == creator and post.get("mint") == mint:
                                post_amount = float(
                                    post.get("uiTokenAmount", {}).get("uiAmount", 0) or 0
                                )
                                if post_amount < pre_amount:
                                    sold_pct = (
                                        (pre_amount - post_amount) / pre_amount * 100
                                        if pre_amount > 0
                                        else 0
                                    )
                                    log.warning(
                                        f"🚨 Dev sell detected for {mint[:8]}: "
                                        f"{sold_pct:.1f}% sold"
                                    )
                                    await db.insert_dev_activity(mint, {
                                        "creator": creator,
                                        "action": "sell",
                                        "timestamp": sig_info.get("blockTime", int(time.time())),
                                        "sol_amount": 0.0,
                                        "token_pct": sold_pct,
                                        "tx_signature": sig,
                                    })
                                    return  # Found a sell, done

        except Exception as e:
            log.debug(f"Dev wallet check error for {mint[:8]}: {e}")

    async def _fetch_transaction(
        self, signature: str, session: aiohttp.ClientSession
    ) -> Optional[dict]:
        """Fetch a single transaction by signature from Solana RPC."""

        async def _fetch():
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0,
                    }
                ],
            }
            async with session.post(
                SOLANA_RPC_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data.get("result")
                elif resp.status == 429:
                    raise RateLimitError("Solana RPC rate limited")
                return None

        try:
            return await rate_limiter.safe_request(
                "solana_rpc", _fetch, max_retries=2
            )
        except Exception:
            return None

    @property
    def stats(self) -> dict:
        return {
            "active": self._active_count,
            "completed": self._completed_count,
            "failed": self._failed_count,
        }
