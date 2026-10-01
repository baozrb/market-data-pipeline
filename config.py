"""Pipeline settings. Override the database with the DATABASE_URL environment variable."""
import os
from pathlib import Path

ROOT = Path(__file__).parent

SYMBOLS = ["SPY", "QQQ", "AAPL", "NVDA", "BTC-USD", "ETH-USD"]
HISTORY_START = "2018-01-01"
LOOKBACK_DAYS = 7  # re-fetch recent bars to pick up revisions and detect re-adjusted history

# Default points at the dev MySQL from docker-compose.yml (dev-only credentials).
DATABASE_URL = os.environ.get("DATABASE_URL", "mysql+pymysql://pipeline:pipeline_dev@127.0.0.1:3307/market")

PARQUET_DIR = ROOT / "parquet"
REPORT_DIR = ROOT / "reports"
