import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.power.ercot import ercot_api_utils as ercot_api

# SCRAPE
API_SCRAPE_NAME = "seven_day_load_forecast"
ENDPOINT = "np3-565-cd/lf_by_model_weather_zone"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
Seven-Day Load Forecast by Model and Weather Zone:
https://apiexplorer.ercot.com/api-details#api=pubapi-apim-api&operation=getData_101
"""


def _format(
    df: pd.DataFrame,
    ) -> pd.DataFrame:

    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()

    df.drop(columns=["inuseflag", "dstflag"], inplace=True, errors='ignore')

    df["posteddatetime"] = pd.to_datetime(df["posteddatetime"])
    df["deliverydate"] = pd.to_datetime(df["deliverydate"]).dt.date
    df["hourending"] = df["hourending"].str.split(':').str[0].astype(str).str.zfill(2)

    keys = ["posteddatetime", "deliverydate", "hourending", 'model']
    df = df[keys + [col for col in df.columns if col not in keys]]

    df = df.sort_values(by=["posteddatetime", "deliverydate", "hourending"]).reset_index(drop=True)

    return df


def _pull(
        start_date: datetime = (datetime.now() + timedelta(days=0)),
        end_date: datetime = (datetime.now() + timedelta(days=0)),
    ) -> pd.DataFrame:

    params = {
        'deliveryDateFrom': start_date.strftime('%Y-%m-%d'),
        'deliveryDateTo': end_date.strftime('%Y-%m-%d'),
        'inUseFlag': "true",
        'DSTFlag': "false",
    }

    response = ercot_api.make_request(ENDPOINT, params, logger=logger)
    df = ercot_api.parse_response(response)

    logger.info(f"Rows pulled for delivery date {start_date.strftime('%Y-%m-%d')}: {len(df)}")

    df = _format(df)

    return df


def _helper(
        start_date: datetime = (datetime.now() + timedelta(days=0)),
        end_date: datetime = (datetime.now() + timedelta(days=7)),
    ) -> pd.DataFrame:

    df = pd.DataFrame()
    for current_datetime in pd.date_range(start=start_date, end=end_date).to_pydatetime().tolist():
        df_current = _pull(current_datetime, current_datetime)
        df = pd.concat([df, df_current])
    df.reset_index(drop=True, inplace=True)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "ercot",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_key_candidates = ["posteddatetime", "deliverydate", "hourending", "model"]
    primary_keys = [col for col in primary_key_candidates if col in df.columns]
    if not primary_keys:
        raise ValueError(
            f"No valid primary keys found for {schema}.{table_name}. "
            f"Expected one of {primary_key_candidates}, got columns={df.columns.tolist()}"
        )

    data_types = azure_postgresql.get_table_dtypes(
        database = database,
        schema = schema,
        table_name = table_name,
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database = database,
        schema = schema,
        table_name = table_name,
        df = df,
        columns = df.columns.tolist(),
        data_types = data_types,
        primary_key = primary_keys,
    )


def main(
        start_date: datetime = (datetime.now() + timedelta(days=0)),
        end_date: datetime = (datetime.now() + timedelta(days=7)),
    ):

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"ercot.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"{API_SCRAPE_NAME}")
        logger.info(f"Endpoint: {ENDPOINT}")
        logger.info(f"Delivery date window: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        t0 = time.time()
        logger.section(f"Pulling data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
        df = _helper(
            start_date=start_date,
            end_date=end_date,
        )
        pull_duration = time.time() - t0
        run.log_stage(stage_name="pull", rows=len(df), duration_seconds=round(pull_duration, 2))

        if df.empty:
            logger.warning("No data returned from ERCOT API")

        t1 = time.time()
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        upsert_duration = time.time() - t1
        run.log_stage(stage_name="upsert", rows=len(df), duration_seconds=round(upsert_duration, 2))

        logger.info(f"Total rows processed: {len(df)}")
        logger.success(f"Successfully pulled and upserted data!")

        run.success(rows_processed=len(df))

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        raise

    finally:
        logging_utils.close_logging()

    if 'df' in locals() and df is not None:
        return df

"""
"""

if __name__ == "__main__":
    df = main()
