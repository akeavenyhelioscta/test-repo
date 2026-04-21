"""
Genscape: Daily Pipeline Production

Pulls daily dry gas pipeline production data by region from the Genscape
Supply & Demand API, pivots regions to columns, and upserts to PostgreSQL.

API: https://developer.genscape.com/api-details#api=natgas-supply-demand&operation=GetDailyGasPipelineProduction
Target: genscape.daily_pipeline_production
Primary Key: [reportdate, date]
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.genscape.genscape_api_utils import make_request

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

API_SCRAPE_NAME = "daily_pipeline_production_v2_2026_mar_10"
API_URL = "https://api.genscape.com/natgas/supply-demand/v1/daily-pipeline-production?"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# --------------------------------------------------------------------------- #
# _pull
# --------------------------------------------------------------------------- #

def _pull(
    end_date: datetime = None,
    lookback: int = 7,
) -> pd.DataFrame:
    if end_date is None:
        end_date = datetime.now()

    min_date = (end_date - timedelta(days=lookback)).strftime("%Y-%m-%d")

    params = {
        "minDate": min_date,
        "limit": 5000,
        "offset": 0,
        "format": "json",
    }

    logger.info(
        f"Requesting daily pipeline production | "
        f"minDate={min_date} | end_date={end_date.strftime('%Y-%m-%d')} | lookback={lookback}"
    )

    df = make_request(url=API_URL, params=params, logger=logger)

    regions_in_response = df["region"].nunique() if "region" in df.columns else 0
    report_dates_in_response = df["reportDate"].nunique() if "reportDate" in df.columns else 0
    logger.info(
        f"Raw response: {len(df)} rows | "
        f"{regions_in_response} distinct regions | "
        f"{report_dates_in_response} distinct report dates"
    )

    return df


# --------------------------------------------------------------------------- #
# _format
# --------------------------------------------------------------------------- #

def _format(df: pd.DataFrame) -> pd.DataFrame:
    # pivot regions to columns
    df = pd.pivot_table(
        df,
        index=["reportDate", "date"],
        columns="region",
        values="dryGasProductionMMCF",
        aggfunc="sum",
    )
    df.reset_index(drop=False, inplace=True)

    # clean column names
    df.columns = (
        df.columns.str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("&", "and")
        .str.lower()
    )

    primary_keys = ["reportdate", "date"]
    value_cols = [col for col in df.columns if col not in primary_keys]

    for col in primary_keys:
        df[col] = pd.to_datetime(df[col])
    for col in value_cols:
        df[col] = df[col].astype(float)

    df = df[primary_keys + value_cols]
    df.sort_values(by=primary_keys, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(
        f"Formatted: {len(df)} rows x {len(df.columns)} cols | "
        f"date range: {df['date'].min()} to {df['date'].max()} | "
        f"report dates: {df['reportdate'].min()} to {df['reportdate'].max()} | "
        f"region columns: {len(value_cols)}"
    )

    return df


# --------------------------------------------------------------------------- #
# _upsert
# --------------------------------------------------------------------------- #

def _upsert(
    df: pd.DataFrame,
    database: str = "helioscta",
    schema: str = "genscape",
    table_name: str = API_SCRAPE_NAME,
) -> None:
    primary_keys = ["reportdate", "date"]
    logger.info(
        f"Upserting {len(df)} rows to {schema}.{table_name} | "
        f"primary_key={primary_keys}"
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        primary_key=primary_keys,
    )

    logger.info(f"Upsert complete: {len(df)} rows written to {schema}.{table_name}")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main(
    end_date: datetime = None,
    lookback: int = 7,
):
    if end_date is None:
        end_date = datetime.now()

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="natgas",
        priority="high",
        tags="genscape,pipeline_production",
        target_table=f"genscape.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"{API_SCRAPE_NAME}")
        logger.info(
            f"Pipeline started | end_date={end_date.strftime('%Y-%m-%d')} | lookback={lookback}"
        )

        # pull
        logger.section("Pulling data from Genscape API...")
        df = _pull(end_date=end_date, lookback=lookback)

        # format
        logger.section("Formatting data...")
        df = _format(df)

        # upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)

        min_date = (end_date - timedelta(days=lookback)).strftime("%Y-%m-%d")
        logger.success(
            f"Pipeline complete | {len(df)} rows upserted | "
            f"date window: {min_date} to {end_date.strftime('%Y-%m-%d')}"
        )
        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    return df


if __name__ == "__main__":
    df = main()
