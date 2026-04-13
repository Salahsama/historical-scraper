# Historical Memecoin Launch Scraper

A resilient, 24/7 asynchronous Python bot that collects high-fidelity historical data on Solana memecoin launches (**PumpFun** and **Raydium**) using only **free APIs**. All data is stored in a local SQLite database optimised for backtesting a sniping strategy.

---

## Features

- **Multi-source discovery** — polls PumpFun, GeckoTerminal, and DexScreener every 10 seconds for newly launched tokens
- **Per-token 5-minute tracking** — captures price ticks every 5 seconds, raw trades, and dev wallet activity for the critical launch window
- **Up to 20 concurrent trackers** — semaphore-controlled, with a 200-token queue buffer for burst periods
- **Conservative rate limiting** — per-API token-bucket + circuit-breaker to avoid bans:
  | API           | Req/s | Burst |
  |---------------|-------|-------|
  | DexScreener   | 4.0   | 8     |
  | PumpFun       | 1.5   | 4     |
  | GeckoTerminal | 0.4   | 2     |
  | Solana RPC    | 2.0   | 5     |
- **Graceful shutdown** — catches `SIGINT`/`SIGTERM`; finishes active trackers before exit
- **Automatic DB maintenance** — WAL mode, 64 MB cache, periodic `PRAGMA optimize`, stale-tracker cleanup every 10 minutes
- **Hourly stats logging** — uptime, discovery counts, API health, queue depth

---

## Database Schema

All data lives in `data/launches.db` (SQLite).

| Table           | Description                                             |
|-----------------|---------------------------------------------------------|
| `tokens`        | One row per token — metadata, initial liquidity, final 5-min stats |
| `price_ticks`   | ~60 rows per token — price/mcap/volume snapshots every 5 s |
| `trades`        | Raw buy/sell trades from PumpFun                        |
| `dev_activity`  | Creator wallet sell / transfer events                   |
| `scraper_stats` | Operational metrics logged every maintenance cycle      |

---

## Project Structure

```
historical-scraper/
├── main.py          # Orchestrator — discovery, tracker manager, maintenance loops
├── scraper.py       # TokenDiscovery & TokenTracker logic
├── db.py            # Async SQLite layer (aiosqlite)
├── rate_limiter.py  # Token-bucket rate limiter with circuit breaker
├── config.py        # All tunable parameters
├── logger.py        # Rotating file + console logger
├── requirements.txt
└── data/
    ├── launches.db  # SQLite database (auto-created)
    └── logs/        # Rotating log files
```

---

## Prerequisites

- Python **3.11+**
- (Optional) A private Solana RPC URL for higher throughput

---

## Installation

```bash
# Clone / enter the directory
cd historical-scraper

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Configuration

Copy or create a `.env` file in the `historical-scraper/` directory:

```env
# Optional: replace with a private RPC for better rate limits
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com

# Logging level: DEBUG | INFO | WARNING | ERROR
SCRAPER_LOG_LEVEL=INFO
```

All other parameters (intervals, concurrency caps, rate limits) are tunable directly in `config.py`.

---

## Running

```bash
python main.py
```

The bot will:
1. Create `data/` and `data/launches.db` automatically
2. Load previously seen mints to skip duplicates
3. Start the discovery, tracker, maintenance, and stats loops concurrently

Stop cleanly with **Ctrl+C** or `kill -SIGTERM <pid>`.

---

## Running 24/7 on a VPS (systemd)

```ini
# /etc/systemd/system/memecoin-scraper.service
[Unit]
Description=Historical Memecoin Launch Scraper
After=network.target

[Service]
WorkingDirectory=/path/to/historical-scraper
ExecStart=/path/to/.venv/bin/python main.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now memecoin-scraper
sudo journalctl -fu memecoin-scraper
```

---

## Backtesting Integration

The scraper is designed as the **data collection layer** of a broader quantitative sniping pipeline. The SQLite database can be queried directly for feature engineering and strategy backtesting:

```sql
-- Example: tokens with >$5k initial liquidity that 3x'd in the first 5 minutes
SELECT t.mint, t.symbol, t.initial_liq_usd,
       (t.final_mcap / t.initial_mcap) AS mcap_multiplier
FROM tokens t
WHERE t.initial_liq_usd > 5000
  AND t.final_mcap / t.initial_mcap >= 3.0
  AND t.tracking_status = 'complete'
ORDER BY mcap_multiplier DESC;
```

The data is structured to support backtesting within the execution constraints of the final Rust sniper bot:
- `EXECUTION_RPC_LIMIT` = 50 req/s
- `EXECUTION_TX_LIMIT` = 5 `sendTransaction`/s

---

## Dependencies

| Package          | Purpose                     |
|------------------|-----------------------------|
| `aiohttp`        | Async HTTP client           |
| `aiosqlite`      | Async SQLite driver         |
| `python-dotenv`  | `.env` file loading         |

---

## License

Private / internal use.
