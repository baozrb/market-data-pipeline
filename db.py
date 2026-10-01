"""Schema and database helpers. SQLAlchemy Core, so the same code runs on MySQL and SQLite."""
from datetime import date, datetime

import pandas as pd
from sqlalchemy import (BigInteger, Boolean, Column, Connection, Date, DateTime, Double, Integer,
                        MetaData, String, Table, Text, create_engine, delete, func, insert, select)
from sqlalchemy.engine import Engine

from clean import PRICE_COLS

metadata = MetaData()

daily_bars = Table(
    "daily_bars", metadata,
    Column("symbol", String(20), primary_key=True),
    Column("date", Date, primary_key=True),
    *[Column(c, Double, nullable=False) for c in PRICE_COLS],
    Column("volume", BigInteger, nullable=False),
    Column("updated_at", DateTime, nullable=False),
)

ingest_runs = Table(
    "ingest_runs", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_at", DateTime, nullable=False),
    Column("symbol", String(20), nullable=False),
    Column("status", String(10), nullable=False),  # ok / error
    Column("rows_fetched", Integer, nullable=False, default=0),
    Column("rows_inserted", Integer, nullable=False, default=0),
    Column("rows_updated", Integer, nullable=False, default=0),
    Column("rows_rejected", Integer, nullable=False, default=0),
    Column("full_reload", Boolean, nullable=False, default=False),
    Column("message", Text),
)

VALUE_COLS = PRICE_COLS + ["volume", "updated_at"]


def get_engine(url: str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True)
    metadata.create_all(engine)
    return engine


def last_date(conn: Connection, symbol: str) -> date | None:
    return conn.execute(select(func.max(daily_bars.c.date)).where(daily_bars.c.symbol == symbol)).scalar()


def read_bars(conn: Connection, symbol: str, start: date | None = None) -> pd.DataFrame:
    q = select(daily_bars).where(daily_bars.c.symbol == symbol).order_by(daily_bars.c.date)
    if start is not None:
        q = q.where(daily_bars.c.date >= start)
    df = pd.DataFrame(conn.execute(q).mappings().all(), columns=[c.name for c in daily_bars.columns])
    df["date"] = pd.to_datetime(df["date"])
    return df


def delete_symbol(conn: Connection, symbol: str) -> None:
    conn.execute(delete(daily_bars).where(daily_bars.c.symbol == symbol))


def upsert_bars(conn: Connection, symbol: str, df: pd.DataFrame) -> None:
    """Insert new bars and overwrite existing ones (same symbol + date)."""
    if df.empty:
        return
    now = datetime.now().replace(microsecond=0)
    rows = [
        {"symbol": symbol, "date": r.date.date(), **{c: float(getattr(r, c)) for c in PRICE_COLS},
         "volume": int(r.volume), "updated_at": now}
        for r in df.itertuples(index=False)
    ]
    dialect = conn.dialect.name
    if dialect == "mysql":
        from sqlalchemy.dialects.mysql import insert as dialect_insert
        stmt = dialect_insert(daily_bars)
        stmt = stmt.on_duplicate_key_update({c: stmt.inserted[c] for c in VALUE_COLS})
    elif dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as dialect_insert
        stmt = dialect_insert(daily_bars)
        stmt = stmt.on_conflict_do_update(index_elements=["symbol", "date"],
                                          set_={c: stmt.excluded[c] for c in VALUE_COLS})
    else:
        raise NotImplementedError(f"upsert not implemented for {dialect}")
    for i in range(0, len(rows), 1000):
        conn.execute(stmt, rows[i:i + 1000])


def log_run(conn: Connection, **fields) -> None:
    conn.execute(insert(ingest_runs).values(run_at=datetime.now().replace(microsecond=0), **fields))
