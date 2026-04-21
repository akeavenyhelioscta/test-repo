"""
Genscape: Daily Power Estimate

Pulls daily power generation burn estimates by region from the Genscape
Pipeline Fundamentals API, pivots regions to columns, and upserts to PostgreSQL.

API: https://developer.genscape.com/api-details#api=natgas-pipeline-fundamentals&operation=GetPowerDailyEstimateData
Target: genscape.daily_power_estimate
Primary Key: [gasday, power_burn_variable, modeltype]
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

API_SCRAPE_NAME = "daily_power_estimate"
API_URL = "https://api.genscape.com/natgas/pipeline-fundamentals/v1/power-estimate/daily?"

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

    params = {
        "endDate": end_date.strftime("%Y-%m-%d"),
        "daysBack": lookback,
        "limit": 5000,
        "offset": 0,
        "format": "json",
    }

    logger.info(
        f"Requesting daily power estimates | "
        f"endDate={params['endDate']} | daysBack={lookback}"
    )

    df = make_request(url=API_URL, params=params, logger=logger)

    regions = df["regionName"].nunique() if "regionName" in df.columns else 0
    model_types = df["modelType"].unique().tolist() if "modelType" in df.columns else []
    gas_days = df["gasDay"].nunique() if "gasDay" in df.columns else 0
    logger.info(
        f"Raw response: {len(df)} rows | "
        f"{regions} regions | {gas_days} gas days | "
        f"model_types={model_types}"
    )

    return df


# --------------------------------------------------------------------------- #
# _format
# --------------------------------------------------------------------------- #

def _format(df: pd.DataFrame) -> pd.DataFrame:
    # melt value columns to long format, then pivot regions to columns
    df_melt = df.melt(
        id_vars=["gasDay", "regionName", "modelType"],
        var_name="power_burn_variable",
        value_name="value",
    )
    df_pivot = pd.pivot_table(
        df_melt,
        index=["gasDay", "power_burn_variable", "modelType"],
        columns="regionName",
        values="value",
        aggfunc="sum",
    )
    df_pivot.reset_index(drop=False, inplace=True)
    df_pivot.columns.name = ""
    df = df_pivot.copy()

    # clean column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    region_cols = [c for c in df.columns if c not in ("gasday", "power_burn_variable", "modeltype")]
    logger.info(
        f"Formatted: {len(df)} rows x {len(df.columns)} cols | "
        f"gas_day range: {df['gasday'].min()} to {df['gasday'].max()} | "
        f"power_burn_variables: {df['power_burn_variable'].nunique()} | "
        f"region columns: {len(region_cols)} ({region_cols})"
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
    primary_keys = ["gasday", "power_burn_variable", "modeltype"]

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
    end_date: datetime = None,
    lookback: int = 7,
):
    if end_date is None:
        end_date = datetime.now()

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="natgas",
        priority="high",
        tags="genscape,power_estimate",
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

        logger.success(
            f"Pipeline complete | {len(df)} rows upserted | "
            f"date window: {(end_date - timedelta(days=lookback)).strftime('%Y-%m-%d')} "
            f"to {end_date.strftime('%Y-%m-%d')}"
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
