import requests
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "energy_storage_resources_daily"
ENDPOINT_URL = "https://www.ercot.com/api/1/services/read/dashboards/energy-storage-resources.json"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
ERCOT Dashboard API — Energy Storage Resources (no OAuth required).
Returns previous-day and current-day battery storage data.
"""


def _get_json(url, retries=None, **kwargs):
    """Makes a GET request and returns JSON. Retries with exponential backoff."""
    max_attempts = 1 if retries is None else retries + 1
    attempt = 0
    while attempt < max_attempts:
        try:
            logger.info(f"Requesting {url}")
            r = requests.get(url, **kwargs)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            attempt += 1
            if attempt >= max_attempts:
                raise
            wait_time = 2 ** (attempt - 1)
            logger.warning(
                f"Attempt {attempt}/{max_attempts}: Request failed: {e}. "
                f"Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)


def _format(
        df: pd.DataFrame,
    ) -> pd.DataFrame:

    def _format_timestamp(date_str, str_format: str):
        return datetime.strptime(f"{date_str}", str_format).astimezone(timezone.utc)

    def _format_date(date_str, str_format: str):
        return datetime.strptime(f"{date_str}", str_format).date()

    def _format_time(date_str, str_format: str):
        return datetime.strptime(f"{date_str}", str_format).time()

    # General formatting
    df.reset_index(drop=True, inplace=True)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '_')

    # DTYPES
    df["timestamp"] = df.apply(lambda df: _format_timestamp(df["timestamp"], "%Y-%m-%d %H:%M:%S%z"), axis=1)
    df["date"] = df.apply(lambda df: _format_date(df["timestamp"], "%Y-%m-%d %H:%M:%S%z"), axis=1)
    df["time"] = df.apply(lambda df: _format_time(df["timestamp"], "%Y-%m-%d %H:%M:%S%z"), axis=1)
    df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
    df['totalcharging'] = df['totalcharging'].astype(float)
    df['totaldischarging'] = df['totaldischarging'].astype(float)
    df['netoutput'] = df['netoutput'].astype(float)

    # Select Cols
    keys = ['timestamp', 'datetime', 'date', 'time']
    values = ['totalcharging', 'totaldischarging', 'netoutput']
    df = df.reindex(columns=keys+values)

    # Sort
    df.sort_values(by=keys, inplace=True)

    # CHECK DTYPES
    logger.info(f"DTYPES ... {[f'{column}: {type(df[column][0])}' for column in list(df.columns)]}")

    return df


def _pull(
    ) -> pd.DataFrame:
    """Get energy storage resources. Always returns previous and current day data."""

    data = _get_json(ENDPOINT_URL)

    df = pd.DataFrame(data["previousDay"]["data"] + data["currentDay"]["data"])

    logger.info(f"Rows pulled: {len(df)}")

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "ercot",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_key_candidates = ["timestamp"]
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


def main():

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
        logger.info(f"Endpoint: {ENDPOINT_URL}")

        t0 = time.time()
        logger.section("Pulling data...")
        df = _pull()
        pull_duration = time.time() - t0
        run.log_stage(stage_name="pull", rows=len(df), duration_seconds=round(pull_duration, 2))

        if df.empty:
            logger.warning("No data returned from ERCOT dashboard API")

        logger.section("Formatting data...")
        df = _format(df)

        t1 = time.time()
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        upsert_duration = time.time() - t1
        run.log_stage(stage_name="upsert", rows=len(df), duration_seconds=round(upsert_duration, 2))

        logger.info(f"Total rows processed: {len(df)}")
        logger.success("Successfully pulled and upserted data!")

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
