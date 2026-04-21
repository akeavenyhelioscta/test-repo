from __future__ import annotations

from calendar import month_name
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger

from backend.scrapes.ice_python import utils

API_SCRAPE_NAME = "future_contracts_v1_2025_dec_16"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# NOTE: NG LD1 Futures
ICE_PRODUCTS: list[str] = [
    "HNG",  # HH Nat Gas  .. https://www.ice.com/products/6590258

    # SOUTHEAST
    "TRZ",   # 'TRANSCO_ST85'
    "CGB",   # 'COLUMBIA_GULF 
    "CGM",   # 'ANR_SE_T'
    "TWB",   # 'TETCO_WLA'

    # EAST TEXAS
    "HXS",  # HSC Basis Future .. https://www.ice.com/products/6590137
    "WAH",  # Waha Basis Future .. https://www.ice.com/products/6590171
    "NTO",  # NGPL TXOK Basis Future .. https://www.ice.com/products/6590143

    # Northeast - BASIS
    "ALQ",  # Algonquin Citygates Basis Future .. https://www.ice.com/products/6590124
    "TMT",  # TETCO M3 Basis Future .. https://www.ice.com/products/6590161
    "T5B",  # Transco Zone 5 South Basis Future .. https://www.ice.com/products/82270888
    "IZB",  # Iroquois-Z2 Basis (Platts) Future .. https://www.ice.com/products/21587547
    "TZS", # TRANSCO_Z6_NY
    "DOM", # DOMINION_SOUTH
    
    # Southwest
    "SCB",  # 'SOCAL_CG'
    "PGE",  # 'PG&E_CG'

    # Rockies/Northwest
    "CRI", # 'CIG_MAINLINE'

    # TODO:
    # "NMC",  # Michcon Basis Future .. https://www.ice.com/products/6590140
    # "DGD",  # Chicago Basis Future .. https://www.ice.com/products/6590132
    # "AEC",  # AB NIT Basis Future .. https://www.ice.com/products/6590123

    # # POWER
    'PMI',  # PJM Western Hub RT Peak (1 MW) .. https://www.ice.com/products/6590369/PJM-Western-Hub-Real-Time-Peak-1-MW-Fixed-Price-Future
    'ERN',  # PJM Western Hub RT Peak (1 MW) .. https://www.ice.com/products/6590369/PJM-Western-Hub-Real-Time-Peak-1-MW-Fixed-Price-Future
]

STRIP_MAPPING = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}

"""
"""

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


def _build_ice_symbol(
    ice_product: str,
    strip: str,
    contract_year: int,
    suffix: str = "-IUS",
) -> str:
    return f"{ice_product} {strip}{str(contract_year)[-2:]}{suffix}"


def _pull(
    symbol: str,
    data_type: str = "Settlement",
    granularity: str = "D",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    return utils.get_timeseries(
        symbol=symbol,
        data_type=data_type,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        date_col=date_col,
        date_format=date_format,
    )


def _format(
    df: pd.DataFrame,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    return utils.format_timeseries(
        df=df,
        date_col=date_col,
        date_format=date_format,
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
) -> pd.DataFrame:
    current_date = datetime.now()
    contract_start_year = contract_start_year or current_date.year
    contract_end_year = contract_end_year or (current_date.year + 3)
    start_date = start_date or utils.default_start_date()
    end_date = end_date or utils.default_end_date()

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="ice_python",
        target_table=f"{utils.DEFAULT_SCHEMA}.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    frames: list[pd.DataFrame] = []
    total_rows = 0
    pulled_contracts = 0
    skipped_contracts = 0
    try:
        logger.header(API_SCRAPE_NAME)
        logger.info(
            f"Pulling contracts from {contract_start_year} to {contract_end_year}"
        )
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")

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
                f"Year {contract_year}: {', '.join(strip for strip, _, _ in strips_to_pull)}"
            )

            for strip, _, strip_name in strips_to_pull:
                strip_batch: list[pd.DataFrame] = []

                for ice_product in ICE_PRODUCTS:
                    symbol = _build_ice_symbol(
                        ice_product=ice_product,
                        strip=strip,
                        contract_year=contract_year,
                    )
                    try:
                        logger.info(
                            f"Pulling {symbol} "
                            f"(product={ice_product}, strip={strip_name}, year={contract_year})"
                        )
                        df = _pull(
                            symbol=symbol,
                            data_type=data_type,
                            granularity=granularity,
                            start_date=start_date,
                            end_date=end_date,
                            date_col=date_col,
                            date_format=date_format,
                        )
                        df = _format(df=df, date_col=date_col, date_format=date_format)

                        if df.empty:
                            logger.warning(f"No data returned for {symbol}")
                            skipped_contracts += 1
                            continue

                        strip_batch.append(df)
                        frames.append(df)
                        total_rows += len(df)
                        pulled_contracts += 1
                    except Exception as exc:
                        logger.error(f"Failed to pull {symbol}: {exc}")
                        skipped_contracts += 1

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
                            f"Batch upsert failed for {strip_name} {contract_year}: "
                            f"{exc} — falling back to per-contract upserts"
                        )
                        for batch_df in strip_batch:
                            try:
                                _upsert(df=batch_df, table_name=API_SCRAPE_NAME)
                            except Exception as inner_exc:
                                sym = batch_df["symbol"].iloc[0]
                                logger.error(f"Fallback upsert failed for {sym}: {inner_exc}")

        run.success(
            rows_processed=total_rows,
            metadata={
                "contracts_pulled": pulled_contracts,
                "contracts_skipped": skipped_contracts,
            },
        )
        return utils.combine_frames(frames, date_col=date_col)

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        run.failure(error=exc)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()

