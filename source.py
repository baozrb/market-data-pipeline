"""Daily bars from Yahoo Finance, normalised to the pipeline's column layout."""
from datetime import date

import pandas as pd
import yfinance as yf

COLUMNS = ["date", "open", "high", "low", "close", "adj_close", "volume"]


def fetch_daily(symbol: str, start: date | str, end: date | str) -> pd.DataFrame:
    """Bars with start <= date < end. `close` is split-adjusted, `adj_close` also includes dividends."""
    raw = yf.download(symbol, start=str(start), end=str(end), auto_adjust=False, actions=False, progress=False)
    if raw.empty:
        return pd.DataFrame(columns=COLUMNS)
    if isinstance(raw.columns, pd.MultiIndex):
        raw = raw.xs(symbol, axis=1, level="Ticker")
    df = raw.rename(columns={"Adj Close": "adj_close"}).rename(columns=str.lower)
    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    return df.rename_axis("date").reset_index()[COLUMNS]
