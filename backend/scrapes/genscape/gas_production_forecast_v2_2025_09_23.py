"""
Genscape: Gas Production Forecast

Pulls monthly gas production forecast data (production, rig counts, YoY changes)
by region from the Genscape Supply & Demand API and upserts to PostgreSQL.

API: https://developer.genscape.com/api-details#api=natgas-supply-demand&operation=GetGasProductionForecast
Target: genscape.gas_production_forecast_v2_2025_09_23
Primary Key: [month, region, item, reportDate]

Note: The filename includes the version date from the original table migration.
The API_SCRAPE_NAME is "gas_production_forecast" for pipeline logging, but the
target table retains its versioned name for backward compatibility.
"""

from datetime import datetime
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

API_SCRAPE_NAME = "gas_production_forecast"
TABLE_NAME = "gas_production_forecast_v2_2025_09_23"
API_URL = "https://api.genscape.com/natgas/supply-demand/v1/production-forecast?"

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
    report_date: str = None,
    page_size: int = 5000,
) -> pd.DataFrame:
    min_month = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")

    logger.info(
        f"Requesting gas production forecast | "
        f"reportDate={report_date or 'latest'} | minMonth={min_month}"
    )

    all_pages = []
    offset = 0

    while True:
        params = {
            "reportDate": report_date,
            "minMonth": min_month,
            "limit": page_size,
            "offset": offset,
            "format": "json",
        }

        page_df = make_request(url=API_URL, params=params, logger=logger)
        all_pages.append(page_df)
        logger.info(f"Page {len(all_pages)}: {len(page_df)} rows (offset={offset})")

        if len(page_df) < page_size:
            break

        offset += page_size

    df = pd.concat(all_pages, ignore_index=True)

    regions = df["region"].nunique() if "region" in df.columns else 0
    items = df["item"].unique().tolist() if "item" in df.columns else []
    report_dates = df["reportDate"].unique().tolist() if "reportDate" in df.columns else []
    logger.info(
        f"Total response: {len(df)} rows ({len(all_pages)} pages) | "
        f"{regions} regions | items={items} | report_dates={report_dates}"
    )

    return df


# --------------------------------------------------------------------------- #
# _format
# --------------------------------------------------------------------------- #

def _format(df: pd.DataFrame) -> pd.DataFrame:
    primary_keys = ["month", "region", "item", "reportDate"]
    value_cols = ["value"]

    for col in ["month", "reportDate"]:
        df[col] = pd.to_datetime(df[col])
    for col in ["region", "item"]:
        df[col] = df[col].astype(str)
    for col in value_cols:
        df[col] = df[col].astype(float)

    df.sort_values(by=primary_keys, inplace=True)
    df.reset_index(drop=True, inplace=True)

    month_range = f"{df['month'].min()} to {df['month'].max()}"
    logger.info(
        f"Formatted: {len(df)} rows | "
        f"month range: {month_range} | "
        f"regions: {df['region'].nunique()} | "
        f"items: {df['item'].nunique()}"
    )

    return df


# --------------------------------------------------------------------------- #
# _upsert
# --------------------------------------------------------------------------- #

def _upsert(
    df: pd.DataFrame,
    database: str = "helioscta",
    schema: str = "genscape",
    table_name: str = TABLE_NAME,
) -> None:
    primary_keys = ["month", "region", "item", "reportDate"]

    data_types = azure_postgresql.get_table_dtypes(
        database=database,
        schema=schema,
        table_name=table_name,
    )

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
        data_types=data_types,
        primary_key=primary_keys,
    )

    logger.info(f"Upsert complete: {len(df)} rows written to {schema}.{table_name}")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main(
    report_date: str = None,
):
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="natgas",
        priority="high",
        tags="genscape,gas_production_forecast",
        target_table=f"genscape.{TABLE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"{API_SCRAPE_NAME}")
        logger.info(
            f"Pipeline started | report_date={report_date or 'latest'}"
        )

        # pull
        logger.section("Pulling data from Genscape API...")
        df = _pull(report_date=report_date)

        # format
        logger.section("Formatting data...")
        df = _format(df)

        # upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)

        logger.success(
            f"Pipeline complete | {len(df)} rows upserted | "
            f"report_date={report_date or 'latest'}"
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
