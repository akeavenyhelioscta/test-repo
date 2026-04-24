"""Pull the PDA D1-IUS ticker feed for today's trade date from Postgres.

Logs a meta section (trade date, contract, strip, date range) and one line
per trade via ``utils.logging_utils``. No parquet output.

Usage:
    python modelling/data/pjm_next_day_ticker_feed.py
"""
from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd
import psycopg2

# Ensure modelling/ is importable regardless of CWD.
_MODELLING_ROOT = Path(__file__).resolve().parent.parent
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import credentials  # noqa: E402  modelling/credentials.py
from utils import logging_utils  # noqa: E402

OUTPUT_NAME = "pjm_next_day_ticker_feed"

QUERY = """
SELECT
    exec_time_local,
    trade_date,
    symbol,
    description,
    product_type,
    contract_type,
    strip,
    start_date,
    end_date,
    price,
    quantity,
    trade_direction
FROM pjm_da_modelling_cleaned.ice_python_ticker_data
WHERE
    trade_date = current_date
    AND symbol = 'PDA D1-IUS'
ORDER BY exec_time_local DESC
"""

META_COLUMNS = (
    "trade_date",
    "symbol",
    "description",
    "product_type",
    "contract_type",
    "strip",
    "start_date",
    "end_date",
)

DIRECTION_COLORS = {
    "Hit": logging_utils.Colors.BRIGHT_RED,
    "Lift": logging_utils.Colors.BRIGHT_GREEN,
    "Spread": logging_utils.Colors.BRIGHT_YELLOW,
}


def _color_direction(direction: str, width: int = 6) -> str:
    """Return ``direction`` left-padded and wrapped in its ANSI color.

    Padding runs on the raw string so alignment stays correct once the
    invisible escape codes are added around it.
    """
    padded = f"{direction:<{width}}"
    if not logging_utils.supports_color():
        return padded
    color = DIRECTION_COLORS.get(direction)
    if color is None:
        return padded
    return f"{color}{padded}{logging_utils.Colors.RESET}"


def _connect():
    return psycopg2.connect(
        user=credentials.AZURE_POSTGRESQL_DB_USER,
        password=credentials.AZURE_POSTGRESQL_DB_PASSWORD,
        host=credentials.AZURE_POSTGRESQL_DB_HOST,
        port=credentials.AZURE_POSTGRESQL_DB_PORT,
        dbname=credentials.AZURE_POSTGRESQL_DB_NAME,
    )


def fetch() -> pd.DataFrame:
    """Run the ticker query and return the result."""
    with warnings.catch_warnings():
        # pandas warns when read_sql is given a DB-API connection instead of SQLAlchemy.
        warnings.simplefilter("ignore", UserWarning)
        with _connect() as conn:
            return pd.read_sql(QUERY, conn)


def _log_meta(log, df: pd.DataFrame) -> None:
    log.section("Meta")
    for col in META_COLUMNS:
        if col not in df.columns:
            continue
        unique = df[col].dropna().unique()
        if len(unique) == 1:
            log.info(f"  {col:<15} {unique[0]}")
        else:
            log.info(f"  {col:<15} {len(unique)} distinct values")
    log.info(f"  {'rows':<15} {len(df)}")


def _log_summary(log, df: pd.DataFrame) -> None:
    # df is ordered by exec_time_local DESC, so the first row is the latest trade.
    log.section("Summary")
    log.info(f"  {'Last':<15} {df.iloc[0]['price']:.2f}")
    log.info(f"  {'High':<15} {df['price'].max():.2f}")
    log.info(f"  {'Low':<15} {df['price'].min():.2f}")
    log.info(f"  {'Volume':<15} {df['quantity'].sum():,.0f}")


def _log_trades(log, df: pd.DataFrame) -> None:
    log.section("Trades (most recent first)")
    for _, row in df.iterrows():
        direction = _color_direction(str(row["trade_direction"]))
        log.info(
            f"  {row['exec_time_local']}  "
            f"{direction} "
            f"{row['price']:>6.2f}  "
            f"qty {row['quantity']}"
        )


def main() -> None:
    log = logging_utils.init_logging(name=OUTPUT_NAME, log_dir=_MODELLING_ROOT / "logs")
    df = fetch()
    if df.empty:
        log.warning("No ticker rows for trade_date = current_date, symbol = 'PDA D1-IUS'")
        return
    _log_meta(log, df)
    _log_summary(log, df)
    _log_trades(log, df)


if __name__ == "__main__":
    main()
