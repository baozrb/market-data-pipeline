"""Pipeline tests against a fake data vendor.

Runs on SQLite by default. To run against MySQL as well:
    TEST_DATABASE_URL=mysql+pymysql://pipeline:pipeline_dev@127.0.0.1:3307/market_test pytest
"""
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
import db
from clean import validate
from pipeline import run_pipeline
from report import write_report


def make_bars(start="2024-01-01", periods=300, seed=0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, periods)))
    open_ = close * (1 + rng.normal(0, 0.002, periods))
    return pd.DataFrame({
        "date": pd.bdate_range(start, periods=periods),
        "open": open_, "high": np.maximum(open_, close) * 1.005, "low": np.minimum(open_, close) * 0.995,
        "close": close, "adj_close": close * 0.97, "volume": rng.integers(1e6, 5e6, periods),
    })


class FakeVendor:
    """Serves bars from in-memory frames, like the Yahoo source would."""

    def __init__(self, data: dict[str, pd.DataFrame]):
        self.data, self.calls = data, []

    def __call__(self, symbol, start, end):
        self.calls.append((symbol, str(start), str(end)))
        if symbol not in self.data:
            raise ConnectionError("vendor unavailable")
        df = self.data[symbol]
        return df[(df["date"] >= pd.Timestamp(start)) & (df["date"] < pd.Timestamp(end))].copy()


@pytest.fixture
def engine(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "HISTORY_START", "2024-01-01")
    url = os.environ.get("TEST_DATABASE_URL", f"sqlite:///{tmp_path / 'test.db'}")
    eng = db.get_engine(url)
    db.metadata.drop_all(eng)
    db.metadata.create_all(eng)
    yield eng
    eng.dispose()


def count(engine, symbol):
    with engine.connect() as conn:
        return len(db.read_bars(conn, symbol))


def run(engine, vendor, today, tmp_path, symbols=("AAA",)):
    return run_pipeline(engine, vendor, list(symbols), today, parquet_dir=tmp_path / "pq")


def test_first_run_loads_history_and_rerun_is_idempotent(engine, tmp_path):
    bars = make_bars()
    vendor = FakeVendor({"AAA": bars})
    today = date(2024, 6, 3)
    expected = int((bars["date"] < pd.Timestamp(today)).sum())

    first = run(engine, vendor, today, tmp_path)[0]
    assert (first.inserted, first.updated) == (expected, 0)

    second = run(engine, vendor, today, tmp_path)[0]
    assert second.inserted == 0 and second.updated > 0  # only the look-back window is refreshed
    assert count(engine, "AAA") == expected


def test_incremental_run_only_fetches_recent_window(engine, tmp_path):
    vendor = FakeVendor({"AAA": make_bars()})
    run(engine, vendor, date(2024, 6, 3), tmp_path)
    res = run(engine, vendor, date(2024, 6, 17), tmp_path)[0]
    assert res.inserted == 10  # business days 3 June .. 14 June
    _, start, _ = vendor.calls[-1]
    assert start == "2024-05-24"  # last stored bar (31 May) minus 7 days


def test_bad_rows_are_rejected_and_reported(engine, tmp_path):
    bars = make_bars()
    bars.loc[5, "close"] = np.nan
    bars.loc[6, "high"] = bars.loc[6, "low"] * 0.5
    bars.loc[7, "open"] = -1
    res = run(engine, FakeVendor({"AAA": bars}), date(2024, 6, 3), tmp_path)[0]

    assert sorted(res.rejected["reason"]) == ["high below open/close", "missing values", "non-positive price"]
    with engine.connect() as conn:
        stored = set(db.read_bars(conn, "AAA")["date"])
    assert not stored & set(bars.loc[5:7, "date"])

    report = write_report(engine, [res], date(2024, 6, 3), tmp_path / "reports").read_text()
    assert "missing values" in report and "| AAA" in report


def test_dividend_rebases_history_and_triggers_full_reload(engine, tmp_path):
    bars = make_bars()
    vendor = FakeVendor({"AAA": bars})
    run(engine, vendor, date(2024, 6, 3), tmp_path)

    rebased = bars.copy()
    rebased["adj_close"] *= 0.99  # vendor re-adjusts all history after a new dividend
    vendor.data["AAA"] = rebased
    res = run(engine, vendor, date(2024, 6, 10), tmp_path)[0]

    assert res.full_reload
    with engine.connect() as conn:
        stored = db.read_bars(conn, "AAA")
    expected = rebased[rebased["date"] < pd.Timestamp(2024, 6, 10)]
    np.testing.assert_allclose(stored["adj_close"], expected["adj_close"])


def test_parquet_export_matches_database(engine, tmp_path):
    run(engine, FakeVendor({"AAA": make_bars(), "BBB": make_bars(seed=1)}), date(2024, 6, 3), tmp_path,
        symbols=("AAA", "BBB"))
    lake = pd.read_parquet(tmp_path / "pq")  # both symbols, read through the partition folders
    assert set(lake["symbol"].astype(str)) == {"AAA", "BBB"}
    with engine.connect() as conn:
        stored = db.read_bars(conn, "BBB")
    from_lake = lake[lake["symbol"].astype(str) == "BBB"].sort_values("date").reset_index(drop=True)
    np.testing.assert_allclose(from_lake["close"], stored["close"])


def test_failing_symbol_does_not_stop_others(engine, tmp_path):
    results = run(engine, FakeVendor({"AAA": make_bars()}), date(2024, 6, 3), tmp_path, symbols=("BAD", "AAA"))
    assert [r.status for r in results] == ["error", "ok"]
    assert "vendor unavailable" in results[0].message
    assert count(engine, "AAA") > 0
    with engine.connect() as conn:
        logged = conn.execute(db.ingest_runs.select()).mappings().all()
    assert [r["status"] for r in logged] == ["error", "ok"]


def test_validate_flags_big_moves_without_rejecting():
    bars = make_bars(periods=10)
    bars.loc[5, ["open", "high", "low", "close", "adj_close"]] *= 2
    clean, rejected, warnings = validate(bars)
    assert len(clean) == 10 and rejected.empty
    assert len(warnings) == 2  # the jump up and the drop back
