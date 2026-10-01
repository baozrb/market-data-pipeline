# Market Data Pipeline: Yahoo Finance → MySQL + Parquet

A daily pipeline that downloads OHLCV bars for stocks and crypto, validates them, upserts them into **MySQL**, and exports a **Parquet** data lake. It also writes a Markdown report. Run it once a day; it only fetches what is new.

```
Yahoo Finance ──► validate ──► MySQL (daily_bars, ingest_runs) ──► Parquet (symbol=XXX/bars.parquet)
                     │                                       └──► reports/YYYY-MM-DD.md
                     └─ rejected / suspicious rows go to the report
```

## What it handles

| Problem | How it is handled |
|---|---|
| Re-downloading everything every day | **Incremental loads.** Only bars after the last stored date are fetched, plus a 7-day look-back window to pick up vendor revisions. |
| Running twice creates duplicates | **Idempotent upserts** on `(symbol, date)`, using `ON DUPLICATE KEY UPDATE` on MySQL and `ON CONFLICT` on SQLite. |
| Splits and dividends silently change past prices | When previously stored bars come back with different `close` / `adj_close`, the vendor has re-based history, so that symbol is **reloaded in full**. A naive incremental loader misses this and leaves a mix of old and new adjustment factors. |
| Half-finished bars from today's session | `end` is exclusive, so only completed bars are stored. |
| Broken vendor rows | Rows with missing values, non-positive prices, or high/low that contradict open/close are **rejected** and listed in the report. One-day moves over 30% are **flagged** but kept. |
| One symbol fails | Each symbol runs in its own transaction. Failures are logged to `ingest_runs` and the rest still load. The exit code is 1, so schedulers can alert on it. |

## Quick start

```bash
docker compose up -d                 # MySQL 8.4 on localhost:3307 (dev credentials)
python -m venv .venv
.venv\Scripts\activate               # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python pipeline.py                   # first run backfills from 2018; later runs are incremental
```

To use your own database, set `DATABASE_URL`, e.g. `mysql+pymysql://user:pass@host:3306/market`. Symbols and the start date are set in [`config.py`](config.py).

Reading the data back:

```python
import pandas as pd
lake = pd.read_parquet("parquet")                     # all symbols, `symbol` comes from the folder name
spy = pd.read_parquet("parquet", filters=[("symbol", "=", "SPY")])
```

```sql
SELECT symbol, MAX(date) AS last_bar, COUNT(*) AS bars FROM daily_bars GROUP BY symbol;
SELECT * FROM ingest_runs ORDER BY id DESC LIMIT 20;   -- load history and errors
```

## Scheduling

Windows Task Scheduler, every weekday at 07:00:

```bat
schtasks /create /tn "market-data" /sc weekly /d MON,TUE,WED,THU,FRI /st 07:00 ^
  /tr "C:\path\to\project\.venv\Scripts\python.exe C:\path\to\project\pipeline.py"
```

Linux cron: `0 7 * * 1-5 cd /path/to/project && .venv/bin/python pipeline.py`

## Tests

```bash
pytest -q                                                       # SQLite, offline
TEST_DATABASE_URL=mysql+pymysql://pipeline:pipeline_dev@127.0.0.1:3307/market_test pytest -q
```

The tests use a fake vendor and cover: first load and idempotent re-runs, incremental fetch windows, rejection of bad rows, full reload after a dividend re-bases history, Parquet matching MySQL, and one failing symbol not blocking the others.

## Project layout

```
pipeline.py        orchestration + CLI
source.py          Yahoo Finance download, normalised columns
clean.py           validation rules
db.py              schema + upsert (SQLAlchemy Core: MySQL / SQLite)
report.py          daily Markdown report
config.py          symbols, start date, DATABASE_URL
docker-compose.yml local MySQL
tests/             pytest suite
```
