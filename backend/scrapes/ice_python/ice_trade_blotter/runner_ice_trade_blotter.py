"""
ICE intraday quote snapshots for trade blotter PnL marks.

Pulls Last, Settle, Recent Settlement, Strip, Startdt, Enddt for
PDP/PDA short-term power symbols and upserts wide-format rows to
``ice_python.ice_blotter_settles_v1_2026_apr_02``.

Usage:
    python -m backend.scrapes.ice_python.ice_trade_blotter.runner_ice_trade_blotter
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.intraday_quotes import ice_intraday_quotes_utils

API_SCRAPE_NAME = "runner_ice_trade_blotter"
TARGET_TABLE_NAME = "ice_blotter_settles_v1_2026_apr_02"

MT = pytz.timezone("America/Edmonton")

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# ── Symbols ─────────────────────────────────────────────────────────────

PNL_SYMBOLS: list[dict] = [
    {"symbol": "PDP D0-IUS", "description": "PJM RT Balance of Day",   "contract": "HE 0800-HE 2300"},
    {"symbol": "PDP D1-IUS", "description": "PJM RT Next Day",         "contract": "Next Day"},
    {"symbol": "PDP W0-IUS", "description": "PJM RT Balance of Week",  "contract": "Bal Week"},
    {"symbol": "PDP W1-IUS", "description": "PJM RT Week 1",           "contract": "Week 1"},
    {"symbol": "PDP W2-IUS", "description": "PJM RT Week 2",           "contract": "Week 2"},
    {"symbol": "PDP W3-IUS", "description": "PJM RT Week 3",           "contract": "Week 3"},
    {"symbol": "PDP W4-IUS", "description": "PJM RT Week 4",           "contract": "Week 4"},
    {"symbol": "PDA D1-IUS", "description": "PJM DA Next Day",         "contract": "Next Day"},
]

QUOTE_FIELDS: list[str] = [
    "Last",
    "Settle",
    "Recent Settlement",
    "Strip",
    "Startdt",
    "Enddt",
]

# ── Table schema (wide format) ──────────────────────────────────────────

TABLE_COLUMNS: list[str] = [
    "trade_date",
    "snapshot_at",
    "symbol",
    "strip",
    "start_date",
    "end_date",
    "last",
    "settle",
    "recent_settlement",
]

TABLE_DATA_TYPES: list[str] = [
    "DATE",
    "TIMESTAMP",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "VARCHAR",
    "FLOAT",
    "FLOAT",
    "FLOAT",
]

TABLE_PRIMARY_KEY: list[str] = [
    "trade_date",
    "snapshot_at",
    "symbol",
]


def _get_symbol_codes() -> list[str]:
    return [entry["symbol"] for entry in PNL_SYMBOLS]


# ── Pipeline stages ─────────────────────────────────────────────────────

def _pull(symbols: list[str]) -> list:
    """Fetch a single snapshot of quote data from ICE."""
    if not symbols:
        logger.warning("No symbols configured")
        return []

    logger.info(f"Requesting quotes for {len(symbols)} symbols: {symbols}")
    return ice_intraday_quotes_utils.get_quotes_snapshot(
        symbols=symbols,
        fields=QUOTE_FIELDS,
    )


def _format(raw_data: list, snapshot_at: datetime | None = None) -> pd.DataFrame:
    """Parse raw get_quotes response into wide-format rows."""
    if not raw_data or len(raw_data) <= 1:
        return pd.DataFrame(columns=TABLE_COLUMNS)

    # Normalise snapshot timestamp
    if snapshot_at is None:
        snapshot_at = datetime.now(pytz.utc)
    if snapshot_at.tzinfo is None:
        snapshot_at = MT.localize(snapshot_at)
    else:
        snapshot_at = snapshot_at.astimezone(MT)
    snapshot_at = snapshot_at.replace(second=0, microsecond=0, tzinfo=None)

    # Parse raw list-of-lists from get_quotes
    header = raw_data[0]
    rows = raw_data[1:]
    df = pd.DataFrame(rows, columns=header)

    # First column is the symbol
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "symbol"})

    # Rename ICE fields to DB columns
    df = df.rename(columns={
        "Last": "last",
        "Settle": "settle",
        "Recent Settlement": "recent_settlement",
        "Strip": "strip",
        "Startdt": "start_date",
        "Enddt": "end_date",
    })

    # Coerce numeric columns
    for col in ["last", "settle", "recent_settlement"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add metadata
    df["trade_date"] = snapshot_at.date()
    df["snapshot_at"] = snapshot_at

    # Drop rows with no symbol
    df = df.dropna(subset=["symbol"])

    # Select and order columns (only include columns that exist)
    output_cols = [c for c in TABLE_COLUMNS if c in df.columns]
    df = df[output_cols].reset_index(drop=True)

    if df.empty:
        logger.warning("Formatted DataFrame is empty")
    else:
        logger.info(f"Formatted {len(df)} rows at {snapshot_at.isoformat()}")

    return df


def _upsert(df: pd.DataFrame) -> None:
    """Upsert snapshot data to ice_python.ice_blotter_settles_v1_2026_apr_02."""
    if df.empty:
        return

    upsert_df = (
        df[TABLE_COLUMNS]
        .drop_duplicates(subset=TABLE_PRIMARY_KEY, keep="last")
        .reset_index(drop=True)
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=utils.DEFAULT_DATABASE,
        schema=utils.DEFAULT_SCHEMA,
        table_name=TARGET_TABLE_NAME,
        df=upsert_df,
        columns=TABLE_COLUMNS,
        data_types=TABLE_DATA_TYPES,
        primary_key=TABLE_PRIMARY_KEY,
    )


# ── Main ────────────────────────────────────────────────────────────────

def main():
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="ice_trade_blotters",
        target_table=f"{utils.DEFAULT_SCHEMA}.{TARGET_TABLE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        symbols = _get_symbol_codes()
        for entry in PNL_SYMBOLS:
            logger.info(f"  {entry['symbol']:<14} {entry['description']}")

        # Pull
        raw_data = _pull(symbols=symbols)
        if not raw_data or len(raw_data) <= 1:
            logger.warning("No quote data returned from ICE")
            run.success(rows_processed=0)
            return

        # Format
        snapshot_at = ice_intraday_quotes_utils.current_snapshot_at_mst()
        df = _format(raw_data=raw_data, snapshot_at=snapshot_at)
        if df.empty:
            run.success(rows_processed=0)
            return

        # Audit
        returned_symbols = set(df["symbol"].unique())
        requested_symbols = set(symbols)
        missing = requested_symbols - returned_symbols
        if missing:
            logger.warning(f"Missing {len(missing)} symbols: {sorted(missing)}")

        # Upsert
        logger.section(f"Upserting {len(df)} rows ...")
        _upsert(df)
        logger.success("Snapshot upserted")

        run.success(
            rows_processed=len(df),
            metadata={
                "symbols_requested": len(requested_symbols),
                "symbols_returned": len(returned_symbols),
                "symbols_missing": sorted(missing),
                "snapshot_at": snapshot_at.isoformat(),
            },
        )

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        run.failure(error=exc, log_file_path=logger.log_file_path)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
