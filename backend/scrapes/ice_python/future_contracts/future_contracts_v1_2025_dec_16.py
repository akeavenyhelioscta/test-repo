from __future__ import annotations

from calendar import month_name
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.symbols.future_contracts_gas_symbols import (
    get_gas_futures_products,
    build_ice_symbol,
    STRIP_MAPPING,
)
from backend.scrapes.ice_python.symbols.future_contracts_power_pjm_symbols import (
    get_pjm_power_futures_products,
)
from backend.scrapes.ice_python.symbols.future_contracts_power_ercot_symbols import (
    get_ercot_power_futures_products,
)

API_SCRAPE_NAME = "future_contracts_v1_2025_dec_16"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# Minimum fraction of expected symbols that must return data for a run to be
# considered healthy.  Below this the run is logged as degraded.
COMPLETENESS_THRESHOLD = 0.70


# ---------------------------------------------------------------------------
# Product loading & validation
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = {"product", "description"}


def _load_products() -> list[dict]:
    """Load all futures products from the symbol registries and validate."""
    gas = get_gas_futures_products()
    power_pjm = get_pjm_power_futures_products()
    power_ercot = get_ercot_power_futures_products()
    products = gas + power_pjm + power_ercot

    if not products:
        raise ValueError(
            "No futures products returned from symbol registries. "
            "Check backend/scrapes/ice_python/symbols/future_contracts_*_symbols.py"
        )

    for idx, entry in enumerate(products):
        missing = _REQUIRED_KEYS - set(entry.keys())
        if missing:
            raise ValueError(
                f"Product entry [{idx}] is missing required keys: {missing}. "
                f"Entry: {entry}"
            )
        if not entry["product"] or not entry["product"].strip():
            raise ValueError(
                f"Product entry [{idx}] has an empty 'product' value. "
                f"Description: {entry.get('description', 'N/A')}"
            )

    return products


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_relevant_strips(
    contract_year: int,
    current_date: datetime | None = None,
    months_forward: int = 36,
    include_expired: bool = False,
) -> list[tuple[str, int, str]]:
    current_date = current_date or datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    strips_to_pull: list[tuple[str, int, str]] = []

    if contract_year < current_year and not include_expired:
        logger.info(f"Skipping expired year {contract_year}")
        return strips_to_pull

    if contract_year < current_year and include_expired:
        for month in range(1, 13):
            strips_to_pull.append((STRIP_MAPPING[month], month, month_name[month]))
        return strips_to_pull

    if contract_year == current_year:
        start_month = 1 if include_expired else current_month
        month_range = range(start_month, 13)
    else:
        month_range = range(1, 13)

    for month in month_range:
        contract_date = datetime(contract_year, month, 1)
        months_diff = (
            (contract_date.year - current_date.year) * 12
            + contract_date.month
            - current_date.month
        )
        if months_diff <= months_forward:
            strips_to_pull.append((STRIP_MAPPING[month], month, month_name[month]))

    return strips_to_pull


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def _pull(
    symbol: str,
    data_type: str = "Settlement",
    granularity: str = "D",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
    max_retries: int = 3,
) -> pd.DataFrame:
    return utils.get_timeseries_with_retry(
        symbol=symbol,
        data_type=data_type,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        date_col=date_col,
        date_format=date_format,
        max_retries=max_retries,
    )


def _format(
    df: pd.DataFrame,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    # keep_zeros=True: for basis futures, zero is a legitimate settlement
    # (location at parity with HH). Dropping zeros created artificial nulls
    # in downstream pivot queries.
    return utils.format_timeseries(
        df=df,
        date_col=date_col,
        date_format=date_format,
        keep_zeros=True,
    )


def _upsert(
    df: pd.DataFrame,
    database: str = utils.DEFAULT_DATABASE,
    schema: str = utils.DEFAULT_SCHEMA,
    table_name: str = API_SCRAPE_NAME,
) -> None:
    utils.upsert_timeseries(
        df=df,
        database=database,
        schema=schema,
        table_name=table_name,
    )


# ---------------------------------------------------------------------------
# Completeness audit
# ---------------------------------------------------------------------------

def _audit_completeness(
    expected: set[str],
    returned: set[str],
    label: str,
) -> dict:
    """Compare expected vs returned symbols and return an audit dict."""
    missing = expected - returned
    coverage = len(returned) / len(expected) if expected else 1.0
    if missing:
        logger.warning(
            f"[{label}] Missing {len(missing)}/{len(expected)} symbols: "
            f"{sorted(missing)}"
        )
    else:
        logger.info(f"[{label}] Full coverage: {len(returned)}/{len(expected)} symbols")
    return {
        "label": label,
        "expected": len(expected),
        "returned": len(returned),
        "missing_count": len(missing),
        "missing_symbols": sorted(missing),
        "coverage_pct": round(coverage * 100, 1),
    }


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def main(
    data_type: str = "Settlement",
    granularity: str = "D",
    contract_start_year: int | None = None,
    contract_end_year: int | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
    months_forward: int = 36,
    include_expired: bool = False,
    specific_strips: list[str] | None = None,
    max_retries: int = 3,
    completeness_threshold: float = COMPLETENESS_THRESHOLD,
) -> pd.DataFrame:
    current_date = datetime.now()
    contract_start_year = contract_start_year or current_date.year
    contract_end_year = contract_end_year or (current_date.year + 3)
    start_date = start_date or utils.default_start_date()
    end_date = end_date or utils.default_end_date()

    products = _load_products()

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="ice_python",
        target_table=f"{utils.DEFAULT_SCHEMA}.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    total_rows = 0
    pulled_contracts = 0
    skipped_contracts = 0
    failed_symbols: list[str] = []
    returned_symbols: set[str] = set()
    expected_symbols: set[str] = set()
    audits: list[dict] = []

    try:
        logger.header(API_SCRAPE_NAME)
        logger.info(
            f"Pulling contracts from {contract_start_year} to {contract_end_year}"
        )
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        logger.info(f"Retry policy: {max_retries} attempts per symbol")
        logger.info(f"Loaded {len(products)} products from symbol registries:")
        for entry in products:
            logger.info(
                f"  {entry['product']:<6} | {entry['description']:<40} | {entry.get('region', 'unknown')}"
            )

        for contract_year in range(contract_start_year, contract_end_year + 1):
            if specific_strips:
                strips_to_pull = [(strip, 0, strip) for strip in specific_strips]
            else:
                strips_to_pull = get_relevant_strips(
                    contract_year=contract_year,
                    current_date=current_date,
                    months_forward=months_forward,
                    include_expired=include_expired,
                )

            if not strips_to_pull:
                logger.info(f"Year {contract_year}: no strips to pull")
                continue

            logger.section(
                f"Year {contract_year}: "
                f"{', '.join(strip for strip, _, _ in strips_to_pull)}"
            )

            for strip, _, strip_name in strips_to_pull:
                strip_batch: list[pd.DataFrame] = []
                strip_expected: set[str] = set()
                strip_returned: set[str] = set()

                for product_entry in products:
                    ice_product = product_entry["product"]
                    description = product_entry["description"]
                    symbol = build_ice_symbol(
                        product=ice_product,
                        strip=strip,
                        contract_year=contract_year,
                    )
                    expected_symbols.add(symbol)
                    strip_expected.add(symbol)

                    logger.info(
                        f"Pulling {symbol} "
                        f"({description}, strip={strip_name}, "
                        f"year={contract_year})"
                    )

                    df = _pull(
                        symbol=symbol,
                        data_type=data_type,
                        granularity=granularity,
                        start_date=start_date,
                        end_date=end_date,
                        date_col=date_col,
                        date_format=date_format,
                        max_retries=max_retries,
                    )
                    df = _format(df=df, date_col=date_col, date_format=date_format)

                    if df.empty:
                        logger.warning(f"No data returned for {symbol}")
                        skipped_contracts += 1
                        failed_symbols.append(symbol)
                        continue

                    strip_batch.append(df)
                    strip_returned.add(symbol)
                    returned_symbols.add(symbol)
                    total_rows += len(df)
                    pulled_contracts += 1

                # Flush batch at strip boundary
                if strip_batch:
                    try:
                        combined = pd.concat(strip_batch, ignore_index=True)
                        _upsert(df=combined, table_name=API_SCRAPE_NAME)
                        logger.info(
                            f"Upserted {len(combined)} rows "
                            f"({len(strip_batch)} contracts) for "
                            f"{strip_name} {contract_year}"
                        )
                    except Exception as exc:
                        logger.warning(
                            f"Batch upsert failed for {strip_name} "
                            f"{contract_year}: {exc} -- falling back to "
                            f"per-contract upserts"
                        )
                        for batch_df in strip_batch:
                            try:
                                _upsert(df=batch_df, table_name=API_SCRAPE_NAME)
                            except Exception as inner_exc:
                                sym = batch_df["symbol"].iloc[0]
                                logger.error(
                                    f"Fallback upsert failed for {sym}: "
                                    f"{inner_exc}"
                                )

                # Per-strip audit
                audit = _audit_completeness(
                    expected=strip_expected,
                    returned=strip_returned,
                    label=f"{strip_name} {contract_year}",
                )
                audits.append(audit)

        # -- Overall completeness audit ----------------------------------------
        overall_coverage = (
            len(returned_symbols) / len(expected_symbols)
            if expected_symbols else 1.0
        )

        logger.section("Completeness Summary")
        logger.info(
            f"Overall: {len(returned_symbols)}/{len(expected_symbols)} symbols "
            f"({overall_coverage:.1%})"
        )
        logger.info(
            f"Pulled: {pulled_contracts} | Skipped: {skipped_contracts}"
        )

        if failed_symbols:
            logger.warning(
                f"{len(failed_symbols)} failed symbols: "
                f"{failed_symbols[:20]}{'...' if len(failed_symbols) > 20 else ''}"
            )

        metadata = {
            "contracts_pulled": pulled_contracts,
            "contracts_skipped": skipped_contracts,
            "overall_coverage_pct": round(overall_coverage * 100, 1),
            "failed_symbols_count": len(failed_symbols),
            "failed_symbols_sample": failed_symbols[:10],
        }

        if overall_coverage < completeness_threshold:
            logger.warning(
                f"DEGRADED RUN: coverage {overall_coverage:.1%} is below "
                f"threshold {completeness_threshold:.0%}"
            )
            metadata["degraded"] = True

        run.success(rows_processed=total_rows, metadata=metadata)

        # Return empty frame instead of accumulating everything in memory.
        # The data is already in PostgreSQL.
        return utils.empty_timeseries_frame(date_col=date_col)

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        run.failure(error=exc)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
