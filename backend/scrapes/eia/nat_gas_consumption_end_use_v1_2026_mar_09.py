import requests
import time
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
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
API_SCRAPE_NAME = "nat_gas_consumption_end_use"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""


def _pull(
        start_date: str = (datetime.now() - relativedelta(months=6)).strftime("%Y-%m"),
        end_date: str = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m"),
    ) -> pd.DataFrame:
    """
    Natural Gas Consumption by End Use (Monthly).
    API: https://www.eia.gov/opendata/browser/natural-gas/cons
    """

    base_url = "https://api.eia.gov/v2/natural-gas/cons/sum/data/"

    params = {
        "frequency": "monthly",
        "data[0]": "value",
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
        "api_key": secrets.EIA_API_KEY,
    }

    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    all_data = []

    while True:
        response = http.get(base_url, params=params)
        response.raise_for_status()

        json_data = response.json()
        batch = json_data.get("response", {}).get("data", [])
        all_data.extend(batch)

        total_records = int(json_data.get("response", {}).get("total", 0))
        logger.info(f"Fetched {len(batch)} rows. Total so far: {len(all_data)} / {total_records}")

        if params["offset"] + params["length"] >= total_records:
            break

        params["offset"] += params["length"]
        time.sleep(2)

    if not all_data:
        raise RuntimeError(f"No data found for {start_date} to {end_date}")

    df = pd.DataFrame(all_data)
    return df


def _format(df: pd.DataFrame) -> pd.DataFrame:

    # column names
    df.columns = df.columns.str.lower().str.replace("-", "_")

    # string columns
    str_cols = [
        "period", "duoarea", "area_name", "product", "product_name",
        "process", "process_name", "series", "series_description", "units",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # numeric columns
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0).astype(float)

    # drop duplicates
    df = df.drop_duplicates(
        subset=[
            "period", "duoarea", "area_name", "product", "product_name",
            "process", "process_name", "series", "series_description", "value", "units",
        ]
    )

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "eia",
        table_name: str = API_SCRAPE_NAME,
    ):

    data_types = azure_postgresql.get_table_dtypes(
        database=database,
        schema=schema,
        table_name=table_name,
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=["period", "duoarea", "area_name", "process", "process_name", "value"],
    )


def main(
        start_date: str = (datetime.now() - relativedelta(months=6)).strftime("%Y-%m"),
        end_date: str = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m"),
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

        run.success(rows_processed=len(df))

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        raise

    finally:
        logging_utils.close_logging()

    if "df" in locals() and df is not None:
        return df

"""
"""

if __name__ == "__main__":
    df = main()
