import requests
import time
from datetime import datetime, timedelta
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "weekly_underground_storage"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""


def _format(df: pd.DataFrame) -> pd.DataFrame:

    # rename cols
    df.rename(columns={'period': 'eia_week_ending'}, inplace=True)
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

    # extract region
    df['region'] = df['series_description'].str.extract(r'Weekly\s+(.*?)\s+Natural Gas')
    df['region'] = df['region'].str.replace(r'\s+(Region|States)\s*$', '', regex=True)

    # data types
    df["value"] = pd.to_numeric(df["value"], errors='coerce').fillna(0).astype(float)

    return df


def _pull(
        start_date: str = (datetime.now() - timedelta(days=61)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now()).strftime("%Y-%m-%d"),
    ) -> pd.DataFrame:
    """
    NOTE: API pulls from: https://www.eia.gov/opendata/browser/natural-gas/stor/wkly
    """

    base_url = (
        "https://api.eia.gov/v2/natural-gas/stor/wkly/data/?"
        "frequency=weekly&"
        "data[0]=value&"
        f"start={start_date}&"
        f"end={end_date}&"
        "sort[0][column]=period&"
        "sort[0][direction]=desc&"
        "offset=0&"
        "length=5000&"
        f"api_key={secrets.EIA_API_KEY}"
    )

    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_date_dt - start_date_dt).days

    all_data = []
    offset = 0

    # Setup retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    while True:
        url = f"{base_url}&offset={offset}"

        try:
            response = http.get(url)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data['response']['data'])

            if df.empty:
                break

            all_data.append(df)

            # Update offset for the next request
            offset += len(df)

            # Log progress
            latest_date = pd.to_datetime(df['period'].min())
            days_processed = (end_date_dt - latest_date).days
            progress = min(100, int((days_processed / total_days) * 100))
            logger.info(f"Fetched {len(df)} rows. Total rows: {offset}. Latest date: {latest_date}. Progress: {progress}%")

            # Check if we've reached the start date
            if latest_date <= start_date_dt:
                break

            # Add a small delay between requests
            time.sleep(2)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error occurred: {e}")
            logger.error(f"URL: {url}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.content}")
            continue

    if not all_data:
        raise RuntimeError(f"No data found for {start_date} to {end_date}")

    return pd.concat(all_data, ignore_index=True)


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "eia",
        table_name: str = API_SCRAPE_NAME,
    ):

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
        primary_key = ["eia_week_ending", "region"],
    )


def main(
        start_date: str = (datetime.now() - timedelta(days=61)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now()).strftime("%Y-%m-%d"),
    ):

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="eia",
        target_table=f"eia.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:

        logger.header(f"{API_SCRAPE_NAME}")

        # pull
        logger.section(f"Pulling data from {start_date} to {end_date}...")
        df = _pull(
            start_date=start_date,
            end_date=end_date,
        )

        # format
        logger.section(f"Formatting {len(df)} rows...")
        df = _format(df)

        # upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)
        logger.success(f"Successfully pulled and upserted {len(df)} rows!")

        run.success(rows_processed=len(df) if 'df' in locals() else 0)

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        # raise exception
        raise

    finally:
        logging_utils.close_logging()

    if 'df' in locals() and df is not None:
        return df

"""
"""

if __name__ == "__main__":
    df = main()
