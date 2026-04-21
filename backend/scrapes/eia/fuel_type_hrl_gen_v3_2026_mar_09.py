import requests
import time
from datetime import datetime, timedelta
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
API_SCRAPE_NAME = "fuel_type_hrl_gen_v3_2026_mar_09"

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
    df.rename(columns={'period': 'datetime_utc'}, inplace=True)

    # pivot the data
    df = df.pivot(index=['datetime_utc', 'respondent'], columns='type-name', values='value').reset_index()

    # rename cols
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # get date from datetime
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"])
    df["date"] = df["datetime_utc"].dt.date
    df["hour"] = df["datetime_utc"].dt.hour

    # NOTE: Check cols
    primary_keys = ['datetime_utc', 'date', 'hour', 'respondent']
    gen_mix_cols = [
        'battery_storage',
        'coal',
        'geothermal',
        'hydro',
        'natural_gas',
        'nuclear',
        'other',
        'other_energy_storage',
        'petroleum',
        'pumped_storage',
        'solar',
        'solar_with_integrated_battery_storage',
        'unknown',
        'unknown_energy_storage',
        'wind',
        'wind_with_integrated_battery_storage',
    ]

    for col in gen_mix_cols:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # raise error if any cols are missing
    missing_cols = set(gen_mix_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    # select cols
    cols = primary_keys + gen_mix_cols
    df = df[cols]

    # convert dtypes
    for col in primary_keys:
        df[col] = df[col].astype(str)
    for col in gen_mix_cols:
        df[col] = df[col].astype(float)

    # merge duplicate gen_mix_cols
    logger.info("Merging duplicate gen_mix_cols ...")
    logger.info(f"Before: {df.columns}")
    unique_cols = df.columns.unique()
    result = pd.DataFrame(index=df.index)
    for col in unique_cols:
        if df.columns.tolist().count(col) > 1 and col in gen_mix_cols:
            # Sum duplicate columns
            result[col] = df.loc[:, df.columns == col].sum(axis=1)
        else:
            # Keep single columns as-is
            result[col] = df[col]
    logger.info(f"After: {df.columns}")

    df = result.copy()

    return df


def _pull(
        start_date: str = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
    ) -> pd.DataFrame:
    """
    NOTE: Check against https://www.eia.gov/electricity/gridmonitor/dashboard/daily_generation_mix/US48/US48
          API pulls from: https://www.eia.gov/opendata/browser/electricity/rto/fuel-type-data
    """

    base_url = (
        "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?"
        "frequency=hourly&"
        "data[0]=value&"
        f"start={start_date}T00&"
        f"end={end_date}T00&"
        "sort[0][column]=period&"
        "sort[0][direction]=desc&"
        "length=5000&"
        f"api_key={secrets.EIA_API_KEY}&"
        # US48
        "facets[respondent][]=US48&"
        # NE
        "facets[respondent][]=ISNE&"
        # NY
        "facets[respondent][]=NYIS&"
        # MIDW
        "facets[respondent][]=MISO&"
        "facets[respondent][]=AECI&"
        "facets[respondent][]=LGEE&"
        # MIDA
        "facets[respondent][]=PJM&"
        # TEN
        "facets[respondent][]=TVA&"
        # CAR
        "facets[respondent][]=CPLE&"
        "facets[respondent][]=CPLW&"
        "facets[respondent][]=DUK&"
        "facets[respondent][]=SC&"
        "facets[respondent][]=SCEG&"
        "facets[respondent][]=YAD&"
        # SE
        "facets[respondent][]=SE&"
        "facets[respondent][]=SEPA&"
        "facets[respondent][]=SOCO&"
        # FLA
        "facets[respondent][]=FMPP&"
        "facets[respondent][]=FPC&"
        "facets[respondent][]=FPL&"
        "facets[respondent][]=GVL&"
        "facets[respondent][]=HST&"
        "facets[respondent][]=JEA&"
        # CENT
        "facets[respondent][]=SWPP&"
        # TEX
        "facets[respondent][]=ERCO&"
        # NW
        "facets[respondent][]=NW&"
        "facets[respondent][]=AVA&"
        "facets[respondent][]=AVRN&"
        "facets[respondent][]=BPAT&"
        "facets[respondent][]=CHPD&"
        "facets[respondent][]=DOPD&"
        "facets[respondent][]=GCPD&"
        "facets[respondent][]=GRID&"
        "facets[respondent][]=GWA&"
        "facets[respondent][]=IPCO&"
        "facets[respondent][]=NEVP&"
        "facets[respondent][]=NWMT&"
        "facets[respondent][]=PACE&"
        "facets[respondent][]=PACW&"
        "facets[respondent][]=PGE&"
        "facets[respondent][]=PSCO&"
        "facets[respondent][]=PSEI&"
        "facets[respondent][]=SCL&"
        "facets[respondent][]=TPWR&"
        "facets[respondent][]=WACM&"
        "facets[respondent][]=WAUW&"
        "facets[respondent][]=WWA&"
        # SW
        "facets[respondent][]=SW&"
        "facets[respondent][]=AZPS&"
        "facets[respondent][]=DEAA&"
        "facets[respondent][]=EPE&"
        # CAL
        "facets[respondent][]=BANC&"
        "facets[respondent][]=CISO&"
        "facets[respondent][]=IID&"
        "facets[respondent][]=LDWP&"
        "facets[respondent][]=TIDC&"
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
        primary_key = ["datetime_utc", "date", "hour", "respondent"],
    )


def backfill(
        start_date: str = (datetime(2019, 1, 1)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
        delta: relativedelta = relativedelta(days=14),
    ):

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        params = {
            "start_date": current_date.strftime("%Y-%m-%d"),
            "end_date": (current_date + delta).strftime("%Y-%m-%d"),
        }
        print(f"Upserting from {params['start_date']} to {params['end_date']} ...")

        main(
            start_date=params['start_date'],
            end_date=params['end_date'],
        )

        current_date += delta


def main(
        start_date: str = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
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
    # backfill()
