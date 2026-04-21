import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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
API_SCRAPE_NAME = "dam_stlmnt_pnt_prices"
ENDPOINT = "np4-190-cd/dam_stlmnt_pnt_prices"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
DAM Settlement Point Prices:
https://www.ercot.com/mp/data-products/data-product-details?id=np4-190-cd
https://apiexplorer.ercot.com/api-details#api=pubapi-apim-api&operation=getData_28
"""


def _format(
        df: pd.DataFrame,
    ) -> pd.DataFrame:

    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()

    df = df.drop(columns=['dstflag'], errors='ignore')

    df['deliverydate'] = pd.to_datetime(df['deliverydate']).dt.date

    df['hourending'] = df['hourending'].astype(str).str.replace(':00', '', regex=False).astype(int)

    return df


def _pull(
        start_date: datetime = datetime.now(),
        end_date: datetime = datetime.now(),
        settlement_point: str = "HB_NORTH",
    ) -> pd.DataFrame:

    params = {
        'deliveryDateFrom': start_date.strftime('%Y-%m-%d'),
        'deliveryDateTo': end_date.strftime('%Y-%m-%d'),
        'settlementPoint': settlement_point,
        'DSTFlag': "false",
    }

    logger.info(f"Settlement point: {settlement_point}")
    response = ercot_api.make_request(ENDPOINT, params, logger=logger)
    df = ercot_api.parse_response(response)

    logger.info(f"Rows pulled for {settlement_point}: {len(df)}")

    df = _format(df)

    return df


def _helper(
        start_date: datetime = datetime.now(),
        end_date: datetime = datetime.now(),
        settlement_points: list[str] = ["HB_NORTH", "HB_SOUTH", "HB_WEST", "HB_HOUSTON"],
    ) -> pd.DataFrame:

    df = pd.DataFrame()
    for settlement_point in settlement_points:
        df = pd.concat([df, _pull(start_date, end_date, settlement_point)])
    df.reset_index(drop=True, inplace=True)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "ercot",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_key_candidates = ["deliverydate", "hourending", "settlementpoint"]
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
        start_date: datetime = (datetime.now() - relativedelta(days=7)),
        end_date: datetime = (datetime.now() + relativedelta(days=0)),
        delta: relativedelta = relativedelta(days=1),
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
        logger.info(f"Settlement points: HB_NORTH, HB_SOUTH, HB_WEST, HB_HOUSTON")

        total_rows = 0
        current_date = start_date
        while current_date <= end_date:

            t0 = time.time()
            logger.section(f"Pulling data for {current_date.strftime('%Y-%m-%d')}...")
            df = _helper(
                start_date=current_date,
                end_date=current_date,
            )
            pull_duration = time.time() - t0

            t1 = time.time()
            logger.section(f"Upserting {len(df)} rows...")
            _upsert(df)
            upsert_duration = time.time() - t1

            run.log_stage(
                stage_name=f"pull_{current_date.strftime('%Y-%m-%d')}",
                rows=len(df),
                duration_seconds=round(pull_duration, 2),
            )
            run.log_stage(
                stage_name=f"upsert_{current_date.strftime('%Y-%m-%d')}",
                rows=len(df),
                duration_seconds=round(upsert_duration, 2),
            )

            total_rows += len(df)
            logger.success(f"Date {current_date.strftime('%Y-%m-%d')}: {len(df)} rows pulled and upserted")

            current_date += delta

        logger.info(f"Total rows processed: {total_rows}")
        run.success(rows_processed=total_rows)

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        raise

    finally:
        logging_utils.close_logging()

    return total_rows


"""
"""

if __name__ == "__main__":
    df = main()
