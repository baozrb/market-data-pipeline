"""Daily Markdown report: what was loaded, latest prices, and data quality issues."""
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

import db


def _ret(closes: pd.Series, n: int) -> str:
    return f"{closes.iloc[-1] / closes.iloc[-1 - n] - 1:+.2%}" if len(closes) > n else "-"


def write_report(engine: Engine, results: list, today: date, out_dir: Path) -> Path:
    rows, issues = [], []
    with engine.connect() as conn:
        for r in results:
            bars = db.read_bars(conn, r.symbol)
            closes = bars["close"]
            rows.append({
                "Symbol": r.symbol,
                "Status": r.status + (" (full reload)" if r.full_reload else ""),
                "New": r.inserted, "Refreshed": r.updated, "Rejected": len(r.rejected),
                "Rows in DB": len(bars),
                "Last bar": f"{bars['date'].iloc[-1]:%Y-%m-%d}" if len(bars) else "-",
                "Close": f"{closes.iloc[-1]:,.2f}" if len(bars) else "-",
                "1D": _ret(closes, 1), "5D": _ret(closes, 5), "20D": _ret(closes, 20),
            })
            if r.message:
                issues.append({"Symbol": r.symbol, "Date": "-", "Type": "error", "Detail": r.message})
            for kind, frame in (("rejected", r.rejected), ("warning", r.warnings)):
                for x in frame.itertuples():
                    issues.append({"Symbol": r.symbol, "Date": f"{x.date:%Y-%m-%d}", "Type": kind, "Detail": x.reason})

    table = pd.DataFrame(rows).to_markdown(index=False, disable_numparse=True)  # keep our number formatting
    text = f"# Market data report - {today:%Y-%m-%d}\n\n{table}\n\n## Data quality\n\n"
    text += pd.DataFrame(issues).to_markdown(index=False, disable_numparse=True) if issues else "No issues found."
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{today:%Y-%m-%d}.md"
    path.write_text(text + "\n", encoding="utf-8")
    return path
