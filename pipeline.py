"""Incremental daily-bar pipeline: fetch -> validate -> upsert into MySQL -> export Parquet -> report.

Usage:
    python pipeline.py                     # update every symbol in config.SYMBOLS
    python pipeline.py --symbols SPY AAPL  # update a subset
"""
import argparse
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

import config
import db
from clean import validate
from report import write_report

log = logging.getLogger("pipeline")
Fetch = Callable[[str, date | str, date | str], pd.DataFrame]


@dataclass
class SymbolResult:
    symbol: str
    status: str = "ok"
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    full_reload: bool = False
    message: str = ""
    rejected: pd.DataFrame = field(default_factory=pd.DataFrame)
    warnings: pd.DataFrame = field(default_factory=pd.DataFrame)


def history_rebased(stored: pd.DataFrame, fresh: pd.DataFrame) -> bool:
    """True if bars we already stored now come back with different prices. That happens after a
    split (close changes) or a dividend (adj_close changes), and means all history must be reloaded."""
    if stored.empty:
        return False
    both = stored.merge(fresh, on="date", suffixes=("_db", "_new"))
    both = both[both["date"] < stored["date"].max()]  # the newest bar may simply have been revised
    return not all(np.allclose(both[f"{c}_db"], both[f"{c}_new"], rtol=1e-4) for c in ("close", "adj_close"))


def update_symbol(engine: Engine, fetch: Fetch, symbol: str, today: date) -> SymbolResult:
    res = SymbolResult(symbol)
    with engine.begin() as conn:  # one transaction per symbol
        last = db.last_date(conn, symbol)
        start = config.HISTORY_START if last is None else last - timedelta(days=config.LOOKBACK_DAYS)
        # end=today is exclusive, so a bar that is still trading today is never stored
        clean, res.rejected, res.warnings = validate(fetch(symbol, start, today))

        if last is not None and history_rebased(db.read_bars(conn, symbol, start=start), clean):
            log.info("%s: stored history no longer matches the vendor, reloading in full", symbol)
            clean, res.rejected, res.warnings = validate(fetch(symbol, config.HISTORY_START, today))
            db.delete_symbol(conn, symbol)
            res.full_reload = True

        known = set(db.read_bars(conn, symbol, start=clean["date"].min() if len(clean) else None)["date"])
        res.fetched = len(clean) + len(res.rejected)
        res.inserted = int((~clean["date"].isin(known)).sum())
        res.updated = len(clean) - res.inserted
        db.upsert_bars(conn, symbol, clean)
    return res


def export_parquet(engine: Engine, symbol: str, out_dir: Path) -> Path:
    """Hive-style layout (symbol=XXX/bars.parquet), so pd.read_parquet(out_dir) loads every symbol."""
    with engine.connect() as conn:
        df = db.read_bars(conn, symbol).drop(columns=["symbol", "updated_at"])
    path = out_dir / f"symbol={symbol}" / "bars.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def run_pipeline(engine: Engine, fetch: Fetch, symbols: list[str], today: date,
                 parquet_dir: Path = config.PARQUET_DIR) -> list[SymbolResult]:
    results = []
    for symbol in symbols:
        try:
            res = update_symbol(engine, fetch, symbol, today)
            if res.inserted or res.updated:
                export_parquet(engine, symbol, parquet_dir)
            log.info("%s: +%d new, %d refreshed, %d rejected%s", symbol, res.inserted, res.updated,
                     len(res.rejected), " (full reload)" if res.full_reload else "")
        except Exception as exc:  # one bad symbol must not stop the others
            log.exception("%s: failed", symbol)
            res = SymbolResult(symbol, status="error", message=f"{type(exc).__name__}: {exc}")
        with engine.begin() as conn:
            db.log_run(conn, symbol=symbol, status=res.status, rows_fetched=res.fetched,
                       rows_inserted=res.inserted, rows_updated=res.updated,
                       rows_rejected=len(res.rejected), full_reload=res.full_reload, message=res.message)
        results.append(res)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbols", nargs="+", default=config.SYMBOLS)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from source import fetch_daily
    engine = db.get_engine(config.DATABASE_URL)
    today = date.today()
    results = run_pipeline(engine, fetch_daily, args.symbols, today)
    path = write_report(engine, results, today, config.REPORT_DIR)
    log.info("report written to %s", path)
    return 1 if any(r.status == "error" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
