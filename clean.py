"""Data quality rules: reject broken rows, flag suspicious ones."""
import pandas as pd

PRICE_COLS = ["open", "high", "low", "close", "adj_close"]
TOL = 1e-6  # vendors round OHLC independently, so allow tiny inconsistencies
BIG_MOVE = 0.30  # flag one-day close moves larger than this


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Returns (clean rows sorted by date, rejected rows, warning rows). The last two carry a `reason`."""
    df = df.sort_values("date").reset_index(drop=True)
    reason = pd.Series("", index=df.index)

    def reject(mask, why):
        reason[mask & (reason == "")] = why

    body_hi = df[["open", "close"]].max(axis=1)
    body_lo = df[["open", "close"]].min(axis=1)
    reject(df[PRICE_COLS + ["volume"]].isna().any(axis=1), "missing values")
    reject((df[PRICE_COLS] <= 0).any(axis=1), "non-positive price")
    reject(df["high"] < body_hi * (1 - TOL), "high below open/close")
    reject(df["low"] > body_lo * (1 + TOL), "low above open/close")
    reject(df["volume"] < 0, "negative volume")
    reject(df["date"].duplicated(keep="last"), "duplicate date")

    bad = reason != ""
    clean = df[~bad].reset_index(drop=True)
    rejected = df[bad].assign(reason=reason[bad])

    move = clean["close"].pct_change().abs()
    big = move > BIG_MOVE
    warnings = clean[big].assign(reason=[f"close moved {m:.0%} in one bar" for m in move[big]])
    return clean, rejected, warnings
