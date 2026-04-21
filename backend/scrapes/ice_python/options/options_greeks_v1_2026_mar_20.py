"""
ICE options Greeks snapshot pipeline.

Discovers ATM option symbols via ``ice.get_autolist()``, then fetches a
point-in-time snapshot of Greeks and market data via ``ice.get_quotes()``.

Output table: ``ice_python.options_greeks``
    Wide format — one row per option contract per snapshot.

Columns:
    trade_date, snapshot_at, symbol, underlying, underlying_price, strike,
    expiration, option_type, settle, bid, ask, last, volume, open_interest,
    delta, gamma, theta, vega, rho, pct_in_out_of_money

API reference:
    - Options autolist: ICE XL Python Guide, Section 4.1 (pp. 8-9)
    - get_quotes() fields: Section 3.1 (p. 5)
    - Greeks fields confirmed via ice.get_quotes_fields(): Delta, Gamma,
      Theta, Vega, Rho, Strike, Expiration, Underlying, Underlying Price
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger
from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.intraday_quotes.ice_intraday_quotes_utils import (
    current_snapshot_at_mst,
    snapshot_at_to_db_value,
    get_quotes_snapshot,
)
from backend.scrapes.ice_python.symbols.options_symbols import (
    get_options_products,
    get_options_product_codes,
    STRIP_MAPPING,
)

API_SCRAPE_NAME = "options_greeks_v1_2026_mar_20"
TABLE_NAME = "options_greeks"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# ---------------------------------------------------------------------------
# ICE quote fields for options
# ---------------------------------------------------------------------------

OPTIONS_QUOTE_FIELDS: list[str] = [
    "Settle",
    "Bid",
    "Ask",
    "Last",
    "Volume",
    "Open Interest All",
    "Strike",
    "Expiration",
    "Underlying",
    "Underlying Price",
    "Delta",
    "Gamma",
    "Theta",
    "Vega",
    "Rho",
    "% In/Out of Money",
]

# ICE field name -> DB column name
FIELD_TO_COLUMN: dict[str, str] = {
    "Settle": "settle",
    "Bid": "bid",
    "Ask": "ask",
    "Last": "last",
    "Volume": "volume",
    "Open Interest All": "open_interest",
    "Strike": "strike",
    "Expiration": "expiration",
    "Underlying": "underlying",
    "Underlying Price": "underlying_price",
    "Delta": "delta",
    "Gamma": "gamma",
    "Theta": "theta",
    "Vega": "vega",
    "Rho": "rho",
    "% In/Out of Money": "pct_in_out_of_money",
}

NUMERIC_COLUMNS: list[str] = [
    "settle", "bid", "ask", "last", "volume", "open_interest",
    "strike", "underlying_price",
    "delta", "gamma", "theta", "vega", "rho", "pct_in_out_of_money",
]

TABLE_COLUMNS: list[str] = [
    "trade_date",
    "snapshot_at",
    "symbol",
    "underlying",
    "underlying_price",
    "strike",
    "expiration",
    "option_type",
    "settle",
    "bid",
    "ask",
    "last",
    "volume",
    "open_interest",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "pct_in_out_of_money",
]

TABLE_DATA_TYPES: list[str] = [
    "DATE",        # trade_date
    "TIMESTAMP",   # snapshot_at
    "VARCHAR",     # symbol
    "VARCHAR",     # underlying
    "FLOAT",       # underlying_price
    "FLOAT",       # strike
    "DATE",        # expiration
    "VARCHAR",     # option_type
    "FLOAT",       # settle
    "FLOAT",       # bid
    "FLOAT",       # ask
    "FLOAT",       # last
    "FLOAT",       # volume
    "FLOAT",       # open_interest
    "FLOAT",       # delta
    "FLOAT",       # gamma
    "FLOAT",       # theta
    "FLOAT",       # vega
    "FLOAT",       # rho
    "FLOAT",       # pct_in_out_of_money
]

TABLE_PRIMARY_KEY: list[str] = ["trade_date", "snapshot_at", "symbol"]

# Option type regex: symbol contains C or P before the strike
_OPTION_TYPE_RE = re.compile(r"[A-Z]\d{2}(C|P)[\d.]+-")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def _get_relevant_expirations(
    product: str,
    exchange_suffix: str = "-IUS",
    months_forward: int = 6,
) -> list[str]:
    """Get the next N monthly expiration codes for a product.

    Uses ``ice.get_autolist('***{product}{suffix}')`` to fetch all available
    expirations, then filters to the front ``months_forward`` entries.
    """
    ice = utils.get_icepython_module()
    autolist_root = f"***{product}{exchange_suffix}"

    try:
        expirations = ice.get_autolist(autolist_root)
    except Exception as exc:
        logger.error(f"get_autolist('{autolist_root}') failed: {exc}")
        return []

    if not expirations:
        logger.warning(f"No expirations found for {autolist_root}")
        return []

    # Filter to monthly strips only (single letter + 2-digit year, e.g. "J26")
    monthly = [e for e in expirations if re.match(r"^[A-Z]\d{2}$", str(e))]

    selected = monthly[:months_forward]
    logger.info(
        f"{product}: {len(expirations)} total expirations, "
        f"{len(monthly)} monthly, selected {len(selected)}"
    )
    return selected


def _discover_option_symbols(
    product: str,
    expiration: str,
    exchange_suffix: str = "-IUS",
    atm_range: int = 10,
) -> list[str]:
    """Discover ATM option symbols for one product/expiration.

    Uses ``ice.get_autolist('***{product}{suffix} {expiry} ATM:{n}')``
    which returns ``atm_range`` puts + ``atm_range`` calls near the money.
    """
    ice = utils.get_icepython_module()
    autolist_query = f"***{product}{exchange_suffix} {expiration} ATM:{atm_range}"

    try:
        symbols = ice.get_autolist(autolist_query)
    except Exception as exc:
        logger.error(f"get_autolist('{autolist_query}') failed: {exc}")
        return []

    if not symbols:
        logger.warning(f"No option symbols found for {autolist_query}")
        return []

    logger.info(f"{product} {expiration}: discovered {len(symbols)} option symbols")
    return list(symbols)


def _discover_all_symbols(
    products: list[dict] | None = None,
    months_forward: int = 6,
) -> list[str]:
    """Discover option symbols across all configured products and expirations."""
    products = products or get_options_products()
    all_symbols: list[str] = []

    for entry in products:
        product = entry["product"]
        suffix = entry.get("exchange_suffix", "-IUS")
        atm_range = entry.get("atm_range", 10)

        expirations = _get_relevant_expirations(
            product=product,
            exchange_suffix=suffix,
            months_forward=months_forward,
        )

        for expiry in expirations:
            symbols = _discover_option_symbols(
                product=product,
                expiration=expiry,
                exchange_suffix=suffix,
                atm_range=atm_range,
            )
            all_symbols.extend(symbols)

    logger.info(f"Total option symbols discovered: {len(all_symbols)}")
    return all_symbols


# ---------------------------------------------------------------------------
# Pull
# ---------------------------------------------------------------------------

def _pull(symbols: list[str]) -> list:
    """Fetch a single snapshot of Greeks + market data for all option symbols."""
    if not symbols:
        logger.warning("No option symbols to pull")
        return []

    logger.info(f"Requesting quotes for {len(symbols)} option symbols")
    return get_quotes_snapshot(
        symbols=symbols,
        fields=OPTIONS_QUOTE_FIELDS,
    )


# ---------------------------------------------------------------------------
# Format
# ---------------------------------------------------------------------------

def _parse_option_type(symbol: str) -> str:
    """Extract 'call' or 'put' from an ICE option symbol."""
    match = _OPTION_TYPE_RE.search(symbol)
    if match:
        return "call" if match.group(1) == "C" else "put"
    return "unknown"


def _format(
    raw_data: list,
    snapshot_at: datetime | None = None,
) -> pd.DataFrame:
    """Parse raw get_quotes response into a wide-format options DataFrame."""
    if not raw_data or len(raw_data) <= 1:
        return pd.DataFrame(columns=TABLE_COLUMNS)

    snapshot_at_val = snapshot_at_to_db_value(snapshot_at)

    header = raw_data[0]
    rows = raw_data[1:]

    df = pd.DataFrame(rows, columns=header)

    # First column is the symbol
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "symbol"})

    # Rename ICE fields to DB columns
    df = df.rename(columns=FIELD_TO_COLUMN)

    # Coerce numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Parse expiration as date
    if "expiration" in df.columns:
        df["expiration"] = pd.to_datetime(
            df["expiration"], errors="coerce"
        ).dt.date

    # Add computed columns
    df["snapshot_at"] = snapshot_at_val
    df["trade_date"] = snapshot_at_val.date() if hasattr(snapshot_at_val, "date") else snapshot_at_val
    df["option_type"] = df["symbol"].apply(_parse_option_type)

    # Drop rows with no symbol or no Greeks at all
    df = df.dropna(subset=["symbol"])
    greeks = ["delta", "gamma", "theta", "vega", "rho"]
    available_greeks = [g for g in greeks if g in df.columns]
    if available_greeks:
        df = df.dropna(subset=available_greeks, how="all")

    # Ensure column order and filter to expected columns
    output_cols = [c for c in TABLE_COLUMNS if c in df.columns]
    result = df[output_cols].reset_index(drop=True)

    if result.empty:
        logger.warning("All rows dropped during formatting")
    else:
        logger.info(f"Formatted {len(result)} option rows")

    return result


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def _ensure_table(
    table_name: str = TABLE_NAME,
    database: str = utils.DEFAULT_DATABASE,
    schema: str = utils.DEFAULT_SCHEMA,
) -> None:
    """Create the options_greeks table if it doesn't exist."""
    from backend.utils import azure_postgresql_utils as azure_postgresql

    connection = azure_postgresql._connect_to_azure_postgressql(database=database)
    cursor = connection.cursor()

    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
                trade_date DATE NOT NULL,
                snapshot_at TIMESTAMP NOT NULL,
                symbol VARCHAR NOT NULL,
                underlying VARCHAR,
                underlying_price FLOAT,
                strike FLOAT,
                expiration DATE,
                option_type VARCHAR,
                settle FLOAT,
                bid FLOAT,
                ask FLOAT,
                last FLOAT,
                volume FLOAT,
                open_interest FLOAT,
                delta FLOAT,
                gamma FLOAT,
                theta FLOAT,
                vega FLOAT,
                rho FLOAT,
                pct_in_out_of_money FLOAT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                PRIMARY KEY (trade_date, snapshot_at, symbol)
            );
        """)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def _upsert(
    df: pd.DataFrame,
    table_name: str = TABLE_NAME,
    database: str = utils.DEFAULT_DATABASE,
    schema: str = utils.DEFAULT_SCHEMA,
) -> None:
    """Upsert options Greeks to Azure PostgreSQL."""
    if df.empty:
        return

    from backend.utils import azure_postgresql_utils as azure_postgresql

    _ensure_table(table_name=table_name, database=database, schema=schema)

    upsert_df = (
        df[TABLE_COLUMNS]
        .drop_duplicates(subset=TABLE_PRIMARY_KEY, keep="last")
        .reset_index(drop=True)
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=upsert_df,
        columns=TABLE_COLUMNS,
        data_types=TABLE_DATA_TYPES,
        primary_key=TABLE_PRIMARY_KEY,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def main(
    products: list[dict] | None = None,
    months_forward: int = 6,
) -> pd.DataFrame:
    """Capture a single options Greeks snapshot.

    Parameters
    ----------
    products : list[dict] | None
        Override the product registry. If omitted, uses all products from
        ``backend/scrapes/ice_python/symbols/options_symbols.py``.
    months_forward : int
        Number of monthly expirations to pull (default 6).
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="ice_python",
        target_table=f"{utils.DEFAULT_SCHEMA}.{TABLE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        # Discover
        symbols = _discover_all_symbols(
            products=products,
            months_forward=months_forward,
        )

        if not symbols:
            logger.warning("No option symbols discovered — exiting")
            run.success(rows_processed=0, metadata={"symbols_discovered": 0})
            return pd.DataFrame(columns=TABLE_COLUMNS)

        # Pull
        raw_data = _pull(symbols=symbols)

        if not raw_data or len(raw_data) <= 1:
            logger.warning("No quote data returned from ICE")
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_discovered": len(symbols),
                    "symbols_returned": 0,
                },
            )
            return pd.DataFrame(columns=TABLE_COLUMNS)

        # Format
        snapshot_at = current_snapshot_at_mst()
        df = _format(raw_data=raw_data, snapshot_at=snapshot_at)

        if df.empty:
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_discovered": len(symbols),
                    "symbols_returned": 0,
                },
            )
            return pd.DataFrame(columns=TABLE_COLUMNS)

        # Audit
        returned_symbols = set(df["symbol"].unique())
        requested_symbols = set(symbols)
        missing_symbols = requested_symbols - returned_symbols
        products_in_data = (
            df["underlying"].dropna().unique().tolist() if "underlying" in df.columns else []
        )

        if missing_symbols:
            logger.warning(
                f"Missing {len(missing_symbols)}/{len(requested_symbols)} "
                f"symbols (sample: {sorted(missing_symbols)[:5]})"
            )

        # Upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)
        logger.success("Options Greeks snapshot upserted successfully")

        run.success(
            rows_processed=len(df),
            metadata={
                "symbols_discovered": len(symbols),
                "symbols_returned": len(returned_symbols),
                "symbols_missing": len(missing_symbols),
                "products_in_data": products_in_data,
                "trade_date": snapshot_at.date().isoformat(),
                "snapshot_at": snapshot_at.isoformat(),
                "option_types": df["option_type"].value_counts().to_dict()
                if "option_type" in df.columns
                else {},
            },
        )
        return df

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        run.failure(error=exc)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
