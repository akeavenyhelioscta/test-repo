"""
ICE intraday quote snapshots for ERCOT power products.

Pulls Open/High/Low/Bid/Ask/Last/Volume/VWAP/Settle/Recent Settlement
for every symbol in the ERCOT symbol registry and upserts long-format rows to
``ice_python.intraday_quotes``.

Each invocation captures a single point-in-time snapshot.  The Prefect
schedule (see ``flows.py``) controls polling frequency — e.g. every
15 minutes during ERCOT market hours.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.symbols.ercot_short_term_symbols import (
    get_ercot_symbol_codes,
    resolve_ercot_symbol_entries,
)
from backend.scrapes.ice_python.intraday_quotes import ice_intraday_quotes_utils

API_SCRAPE_NAME = "runner_ercot_short_term"
TARGET_TABLE_NAME = ice_intraday_quotes_utils.INTRADAY_QUOTES_TABLE_NAME

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def _select_symbol_entries(symbols: list[str] | None = None) -> list[dict]:
    """Resolve the requested symbol list against the ERCOT symbol registry."""
    symbol_entries = resolve_ercot_symbol_entries(symbols=symbols)
    if symbol_entries:
        logger.info(
            f"Selected {len(symbol_entries)} ERCOT symbols from "
            "backend/scrapes/ice_python/symbols/ercot_short_term_symbols.py"
        )
    return symbol_entries


def _pull(
    symbols: list[str],
    fields: list[str] | None = None,
) -> list:
    """Fetch a single snapshot of quote data from ICE for selected ERCOT symbols."""
    fields = fields or ice_intraday_quotes_utils.DEFAULT_QUOTE_FIELDS

    if not symbols:
        logger.warning("No ERCOT symbols configured — nothing to pull")
        return []

    logger.info(f"Requesting quotes for {len(symbols)} symbols")
    return ice_intraday_quotes_utils.get_quotes_snapshot(
        symbols=symbols,
        fields=fields,
    )


def _format(
    raw_data: list,
    snapshot_at: datetime | None = None,
) -> pd.DataFrame:
    """Parse raw get_quotes response into long-format intraday quote rows."""
    snapshot_at = ice_intraday_quotes_utils.normalize_snapshot_at_mst(snapshot_at)
    df = ice_intraday_quotes_utils.format_intraday_quotes(
        raw_data=raw_data,
        snapshot_at=snapshot_at,
    )
    if df.empty:
        logger.warning("Formatted DataFrame is empty")
    else:
        logger.info(
            f"Formatted {len(df)} intraday quote rows at "
            f"{snapshot_at.isoformat()}"
        )
    return df


def _upsert(
    df: pd.DataFrame,
    database: str = utils.DEFAULT_DATABASE,
    schema: str = utils.DEFAULT_SCHEMA,
    table_name: str = TARGET_TABLE_NAME,
) -> None:
    """Upsert snapshot data to Azure PostgreSQL."""
    ice_intraday_quotes_utils.upsert_intraday_quotes(
        df=df,
        database=database,
        schema=schema,
        table_name=table_name,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def main(
    symbols: list[str] | None = None,
    fields: list[str] | None = None,
) -> pd.DataFrame:
    """Capture a single quote snapshot for ERCOT symbols.

    Parameters
    ----------
    symbols : list[str] | None
        Optional list of ICE symbol codes. If omitted, all symbols defined in
        ``backend/scrapes/ice_python/symbols/ercot_short_term_symbols.py`` are processed.
    fields : list[str] | None
        Override the quote fields (defaults to DEFAULT_QUOTE_FIELDS).
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="ice_python",
        target_table=f"{utils.DEFAULT_SCHEMA}.{TARGET_TABLE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)
        symbol_entries = _select_symbol_entries(symbols=symbols)
        selected_symbols = get_ercot_symbol_codes(symbol_entries)

        # Pull
        raw_data = _pull(symbols=selected_symbols, fields=fields)
        if not raw_data or len(raw_data) <= 1:
            logger.warning("No quote data returned from ICE")
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_requested": len(selected_symbols),
                    "symbols_returned": 0,
                    "symbols_selected": selected_symbols,
                },
            )
            return ice_intraday_quotes_utils.empty_intraday_quotes_frame()

        # Format
        snapshot_at = ice_intraday_quotes_utils.current_snapshot_at_mst()
        df = _format(raw_data=raw_data, snapshot_at=snapshot_at)

        if df.empty:
            logger.warning("All rows dropped during formatting")
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_requested": len(selected_symbols),
                    "symbols_returned": 0,
                    "symbols_selected": selected_symbols,
                },
            )
            return ice_intraday_quotes_utils.empty_intraday_quotes_frame()

        # Audit symbol coverage
        returned_symbols = set(df["symbol"].unique())
        requested_symbols = set(selected_symbols)
        missing_symbols = requested_symbols - returned_symbols
        requested_fields = set(fields or ice_intraday_quotes_utils.DEFAULT_QUOTE_FIELDS)
        returned_data_types = set(df["data_type"].unique())
        missing_data_types = requested_fields - returned_data_types
        if missing_symbols:
            logger.warning(
                f"Missing {len(missing_symbols)}/{len(requested_symbols)} "
                f"symbols: {sorted(missing_symbols)}"
            )
        if missing_data_types:
            logger.warning(
                f"Missing {len(missing_data_types)}/{len(requested_fields)} "
                f"data types: {sorted(missing_data_types)}"
            )

        # Upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)
        logger.success("Snapshot upserted successfully")

        run.success(
            rows_processed=len(df),
            metadata={
                "symbols_requested": len(requested_symbols),
                "symbols_returned": len(returned_symbols),
                "symbols_selected": selected_symbols,
                "symbols_missing": sorted(missing_symbols),
                "data_types_requested": sorted(requested_fields),
                "data_types_returned": sorted(returned_data_types),
                "data_types_missing": sorted(missing_data_types),
                "trade_date": snapshot_at.date().isoformat(),
                "snapshot_at": snapshot_at.isoformat(),
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
