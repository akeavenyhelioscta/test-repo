"""
Utility helpers for ICE intraday quotes (get_quotes / get_timesales).

Extends the shared ``backend.scrapes.ice_python.utils`` module with functions
specific to snapshot quotes and time-and-sales retrieval.  Rate-limit
enforcement and retry logic live here so individual scripts stay thin.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytz

from backend.scrapes.ice_python import utils
from backend.utils import azure_postgresql_utils as azure_postgresql

_logger = logging.getLogger(__name__)

DEFAULT_DATABASE = utils.DEFAULT_DATABASE
DEFAULT_SCHEMA = utils.DEFAULT_SCHEMA

# ICE API limits (from ICE XL Python Guide, Section 6.1)
QUOTES_MAX_SYMBOLS_PER_REQUEST = 500
QUOTES_MAX_REQUESTS_PER_SECOND = 10
TIMESALES_MAX_SYMBOLS_PER_REQUEST = 10
TIMESALES_MIN_INTERVAL_SECONDS = 1.0

# Fields matching the ICE_PYTHON_TICK_LEVEL_DATA screenshot.
# Override at call site after confirming with ice.get_quotes_fields().
DEFAULT_QUOTE_FIELDS: list[str] = [
    "Open",
    "High",
    "Low",
    "Bid",
    "Ask",
    "Last",
    "Volume",
    "VWAP",
    "Settle",
    "Recent Settlement",
]

# Column name mapping: ICE field name -> PostgreSQL column name
FIELD_TO_COLUMN: dict[str, str] = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Bid": "bid",
    "Ask": "ask",
    "Last": "last",
    "Volume": "volume",
    "VWAP": "vwap",
    "Settle": "settle",
    "Recent Settlement": "recent_settlement",
}

NUMERIC_COLUMNS: list[str] = [
    "open",
    "high",
    "low",
    "bid",
    "ask",
    "last",
    "volume",
    "vwap",
    "settle",
    "recent_settlement",
]

COLUMN_TO_FIELD: dict[str, str] = {
    column_name: field_name for field_name, column_name in FIELD_TO_COLUMN.items()
}

# Mountain time — uses America/Edmonton to match the rest of the codebase,
# automatically handling MST/MDT transitions.
MT = pytz.timezone("America/Edmonton")

INTRADAY_QUOTES_TABLE_NAME = "intraday_quotes"
INTRADAY_QUOTES_COLUMNS: list[str] = [
    "trade_date",
    "snapshot_at",
    "data_type",
    "symbol",
    "value",
]
INTRADAY_QUOTES_DATA_TYPES: list[str] = [
    "DATE",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "FLOAT",
]
INTRADAY_QUOTES_PRIMARY_KEY: list[str] = [
    "trade_date",
    "snapshot_at",
    "data_type",
    "symbol",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def chunk_symbols(
    symbols: list[str],
    chunk_size: int,
) -> list[list[str]]:
    """Split a symbol list into API-safe batches."""
    return [
        symbols[i : i + chunk_size]
        for i in range(0, len(symbols), chunk_size)
    ]


def empty_quotes_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the expected quotes schema."""
    return pd.DataFrame(
        columns=["snapshot_at", "symbol"] + NUMERIC_COLUMNS
    )


def empty_intraday_quotes_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the intraday quotes schema."""
    return pd.DataFrame(columns=INTRADAY_QUOTES_COLUMNS)


def current_snapshot_at_mst(now: datetime | None = None) -> datetime:
    """Return the current snapshot timestamp rounded to the minute in MST."""
    base_time = now or datetime.now(timezone.utc)
    if base_time.tzinfo is None:
        base_time = base_time.replace(tzinfo=timezone.utc)
    return base_time.astimezone(MT).replace(second=0, microsecond=0)


def normalize_snapshot_at_mst(snapshot_at: datetime | None = None) -> datetime:
    """Coerce any timestamp to fixed-offset MST rounded to the minute."""
    if snapshot_at is None:
        return current_snapshot_at_mst()
    if snapshot_at.tzinfo is None:
        snapshot_at = MT.localize(snapshot_at)
    else:
        snapshot_at = snapshot_at.astimezone(MT)
    return snapshot_at.replace(second=0, microsecond=0)


def snapshot_at_to_db_value(snapshot_at: datetime | None = None) -> datetime:
    """Convert a snapshot timestamp to a naive MST wall-clock value for Postgres."""
    return normalize_snapshot_at_mst(snapshot_at).replace(tzinfo=None)


def _ensure_publisher_awake(ice) -> None:
    """Disable hibernation so the ICE XL publisher stays responsive."""
    try:
        if ice.get_hibernation():
            ice.set_hibernation(False)
            _logger.info("ICE XL publisher hibernation disabled")
    except Exception as exc:
        _logger.warning(f"Could not check/set publisher hibernation: {exc}")


# ---------------------------------------------------------------------------
# get_quotes wrappers
# ---------------------------------------------------------------------------

def get_quotes_snapshot(
    symbols: list[str],
    fields: list[str] | None = None,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    """Fetch a single snapshot via ``ice.get_quotes()``.

    Handles chunking (500 symbols max per request), retries with
    exponential backoff, and Publisher health checks.

    Returns the raw response from ``ice.get_quotes()`` (list of lists)
    or an empty list on failure.
    """
    ice = utils.get_icepython_module()
    _ensure_publisher_awake(ice)
    fields = fields or DEFAULT_QUOTE_FIELDS

    all_results: list = []
    chunks = chunk_symbols(symbols, QUOTES_MAX_SYMBOLS_PER_REQUEST)

    for chunk in chunks:
        result = _get_quotes_with_retry(
            ice=ice,
            symbols=chunk,
            fields=fields,
            max_retries=max_retries,
            backoff_base=backoff_base,
        )
        if result:
            all_results.extend(result if not all_results else result[1:])

    return all_results


def _get_quotes_with_retry(
    ice,
    symbols: list[str],
    fields: list[str],
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    """Call ``ice.get_quotes()`` with bounded exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            data = ice.get_quotes(symbols, fields)
            if data:
                return data
            _logger.warning(
                f"get_quotes returned empty (attempt {attempt}/{max_retries})"
            )
        except Exception as exc:
            _logger.warning(
                f"get_quotes attempt {attempt}/{max_retries} failed: {exc}"
            )
        if attempt < max_retries:
            wait = backoff_base ** attempt
            _logger.info(f"Retrying in {wait:.1f}s...")
            time.sleep(wait)

    _logger.error(f"All {max_retries} get_quotes attempts failed")
    return []


# ---------------------------------------------------------------------------
# get_timesales wrappers (Phase 2 — structure ready, not yet used)
# ---------------------------------------------------------------------------

def get_timesales_batch(
    symbols: list[str],
    fields: list[str],
    start_date: datetime,
    end_date: datetime,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    """Fetch time-and-sales data respecting the 10 symbol / 1 req-per-sec limits.

    Returns the raw combined response or an empty list on failure.
    """
    ice = utils.get_icepython_module()
    _ensure_publisher_awake(ice)

    all_results: list = []
    chunks = chunk_symbols(symbols, TIMESALES_MAX_SYMBOLS_PER_REQUEST)

    for i, chunk in enumerate(chunks):
        if i > 0:
            time.sleep(TIMESALES_MIN_INTERVAL_SECONDS)

        result = _get_timesales_with_retry(
            ice=ice,
            symbols=chunk,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            max_retries=max_retries,
            backoff_base=backoff_base,
        )
        if result:
            all_results.extend(result if not all_results else result[1:])

    return all_results


def _get_timesales_with_retry(
    ice,
    symbols: list[str],
    fields: list[str],
    start_date: datetime,
    end_date: datetime,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    """Call ``ice.get_timesales()`` with bounded exponential backoff."""
    date_fmt = "%Y-%m-%d %H:%M:%S"
    for attempt in range(1, max_retries + 1):
        try:
            data = ice.get_timesales(
                symbols,
                fields,
                start_date=start_date.strftime(date_fmt),
                end_date=end_date.strftime(date_fmt),
            )
            if data:
                return data
            _logger.warning(
                f"get_timesales returned empty (attempt {attempt}/{max_retries})"
            )
        except Exception as exc:
            _logger.warning(
                f"get_timesales attempt {attempt}/{max_retries} failed: {exc}"
            )
        if attempt < max_retries:
            wait = backoff_base ** attempt
            time.sleep(wait)

    _logger.error(f"All {max_retries} get_timesales attempts failed")
    return []


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_quotes(
    raw_data: list,
    snapshot_at: datetime | None = None,
) -> pd.DataFrame:
    """Parse raw ``get_quotes()`` response into a tidy DataFrame.

    ``get_quotes()`` returns a list-of-lists where the first row is a
    header and subsequent rows are data.  The first column of each data
    row is the symbol.  Remaining columns correspond to the requested
    fields.

    Returns a DataFrame with columns:
        snapshot_at, symbol, open, high, low, bid, ask, last,
        volume, vwap, settle, recent_settlement
    """
    if not raw_data or len(raw_data) <= 1:
        return empty_quotes_frame()

    snapshot_at = snapshot_at_to_db_value(snapshot_at)

    header = raw_data[0]
    rows = raw_data[1:]

    df = pd.DataFrame(rows, columns=header)

    # The first column is typically the symbol identifier.
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "symbol"})

    # Rename ICE field names to snake_case DB columns
    df = df.rename(columns=FIELD_TO_COLUMN)

    # Coerce numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["snapshot_at"] = snapshot_at

    # Drop rows where the symbol is missing or all price cols are null
    df = df.dropna(subset=["symbol"])
    price_cols = [c for c in NUMERIC_COLUMNS if c in df.columns]
    df = df.dropna(subset=price_cols, how="all")

    # Ensure consistent column order
    output_cols = ["snapshot_at", "symbol"] + [
        c for c in NUMERIC_COLUMNS if c in df.columns
    ]
    return df[output_cols].reset_index(drop=True)


def format_intraday_quotes(
    raw_data: list,
    snapshot_at: datetime | None = None,
) -> pd.DataFrame:
    """Transform a single ICE quote snapshot into long-format intraday rows."""
    wide_df = format_quotes(raw_data=raw_data, snapshot_at=snapshot_at)
    if wide_df.empty:
        return empty_intraday_quotes_frame()

    value_columns = [column for column in NUMERIC_COLUMNS if column in wide_df.columns]
    if not value_columns:
        return empty_intraday_quotes_frame()

    intraday_df = wide_df.melt(
        id_vars=["snapshot_at", "symbol"],
        value_vars=value_columns,
        var_name="data_type",
        value_name="value",
    )
    intraday_df["value"] = pd.to_numeric(intraday_df["value"], errors="coerce")
    intraday_df = intraday_df.dropna(subset=["value"])
    if intraday_df.empty:
        return empty_intraday_quotes_frame()

    intraday_df["trade_date"] = pd.to_datetime(
        intraday_df["snapshot_at"],
        errors="coerce",
    ).dt.date
    intraday_df["data_type"] = intraday_df["data_type"].map(COLUMN_TO_FIELD)
    intraday_df = intraday_df.dropna(subset=INTRADAY_QUOTES_COLUMNS)

    return (
        intraday_df[INTRADAY_QUOTES_COLUMNS]
        .sort_values(INTRADAY_QUOTES_PRIMARY_KEY)
        .reset_index(drop=True)
    )


def ensure_intraday_quotes_table(
    table_name: str = INTRADAY_QUOTES_TABLE_NAME,
    database: str = DEFAULT_DATABASE,
    schema: str = DEFAULT_SCHEMA,
) -> None:
    """Create and validate the single intraday quotes table used by the pipeline."""
    connection = azure_postgresql._connect_to_azure_postgressql(database=database)
    cursor = connection.cursor()

    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
            trade_date DATE NOT NULL,
            snapshot_at TIMESTAMP NOT NULL,
            data_type VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            value FLOAT,
            PRIMARY KEY (trade_date, snapshot_at, data_type, symbol)
        );
    """
    validate_columns_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    validate_constraint_query = f"""
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.table_schema = '{schema}'
          AND tc.table_name = '{table_name}'
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        GROUP BY tc.constraint_name
        HAVING string_agg(
            kcu.column_name,
            ', ' ORDER BY kcu.ordinal_position
        ) = 'trade_date, snapshot_at, data_type, symbol';
    """
    add_constraint_query = f"""
        ALTER TABLE {schema}.{table_name}
        ADD CONSTRAINT {table_name}_upsert_key
        UNIQUE (trade_date, snapshot_at, data_type, symbol);
    """

    try:
        cursor.execute(create_schema_query)
        cursor.execute(create_table_query)
        connection.commit()

        cursor.execute(validate_columns_query)
        existing_columns = [row[0] for row in cursor.fetchall()]
        expected = INTRADAY_QUOTES_COLUMNS + ["created_at", "updated_at"]
        if existing_columns != expected:
            raise ValueError(
                f"Expected {schema}.{table_name} columns "
                f"{expected}, found {existing_columns}"
            )

        cursor.execute(validate_constraint_query)
        if not cursor.fetchall():
            cursor.execute(add_constraint_query)
            connection.commit()
    finally:
        cursor.close()
        connection.close()


def upsert_intraday_quotes(
    df: pd.DataFrame,
    table_name: str = INTRADAY_QUOTES_TABLE_NAME,
    database: str = DEFAULT_DATABASE,
    schema: str = DEFAULT_SCHEMA,
    primary_key: list[str] | None = None,
) -> None:
    """Upsert long-format intraday quotes to Azure PostgreSQL."""
    if df.empty:
        return

    ensure_intraday_quotes_table(
        table_name=table_name,
        database=database,
        schema=schema,
    )

    primary_key = primary_key or INTRADAY_QUOTES_PRIMARY_KEY
    upsert_df = (
        df[INTRADAY_QUOTES_COLUMNS]
        .drop_duplicates(subset=primary_key, keep="last")
        .reset_index(drop=True)
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=upsert_df,
        columns=INTRADAY_QUOTES_COLUMNS,
        data_types=INTRADAY_QUOTES_DATA_TYPES,
        primary_key=primary_key,
    )
