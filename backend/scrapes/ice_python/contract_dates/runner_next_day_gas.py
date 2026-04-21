"""
ICE contract dates for next-day gas products.

Pulls Strip / Startdt / Enddt for every symbol in the next-day gas
symbol registry and upserts to ``ice_python.contract_dates``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.symbols.next_day_gas_symbols import (
    get_next_day_gas_symbol_codes,
)
from backend.scrapes.ice_python.contract_dates import ice_contract_dates_utils

API_SCRAPE_NAME = "runner_next_day_gas_contract_dates"
TARGET_TABLE_NAME = ice_contract_dates_utils.CONTRACT_DATES_TABLE_NAME

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def _pull(symbols: list[str]) -> list:
    if not symbols:
        logger.warning("No next-day gas symbols configured — nothing to pull")
        return []
    logger.info(f"Requesting contract dates for {len(symbols)} symbols")
    return ice_contract_dates_utils.get_contract_dates_snapshot(symbols=symbols)


def _format(raw_data: list) -> pd.DataFrame:
    trade_date = ice_contract_dates_utils.current_trade_date_mst()
    df = ice_contract_dates_utils.format_contract_dates(
        raw_data=raw_data,
        trade_date=trade_date,
    )
    if df.empty:
        logger.warning("Formatted DataFrame is empty")
    else:
        logger.info(
            f"Formatted {len(df)} contract date rows for "
            f"{trade_date.isoformat()}"
        )
    return df


def _upsert(
    df: pd.DataFrame,
    database: str = utils.DEFAULT_DATABASE,
    schema: str = utils.DEFAULT_SCHEMA,
    table_name: str = TARGET_TABLE_NAME,
) -> None:
    ice_contract_dates_utils.upsert_contract_dates(
        df=df,
        database=database,
        schema=schema,
        table_name=table_name,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def main() -> pd.DataFrame:
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
        selected_symbols = get_next_day_gas_symbol_codes()
        logger.info(
            f"Selected {len(selected_symbols)} next-day gas symbols from "
            "backend/scrapes/ice_python/symbols/next_day_gas_symbols.py"
        )

        raw_data = _pull(symbols=selected_symbols)
        if not raw_data or len(raw_data) <= 1:
            logger.warning("No contract date data returned from ICE")
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_requested": len(selected_symbols),
                    "symbols_returned": 0,
                },
            )
            return ice_contract_dates_utils.empty_contract_dates_frame()

        df = _format(raw_data=raw_data)
        if df.empty:
            logger.warning("All rows dropped during formatting")
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_requested": len(selected_symbols),
                    "symbols_returned": 0,
                },
            )
            return ice_contract_dates_utils.empty_contract_dates_frame()

        returned_symbols = set(df["symbol"].unique())
        missing_symbols = set(selected_symbols) - returned_symbols
        if missing_symbols:
            logger.warning(
                f"Missing {len(missing_symbols)}/{len(selected_symbols)} "
                f"symbols: {sorted(missing_symbols)}"
            )

        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)
        logger.success("Contract dates upserted successfully")

        run.success(
            rows_processed=len(df),
            metadata={
                "symbols_requested": len(selected_symbols),
                "symbols_returned": len(returned_symbols),
                "symbols_missing": sorted(missing_symbols),
                "trade_date": df["trade_date"].iloc[0].isoformat(),
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
