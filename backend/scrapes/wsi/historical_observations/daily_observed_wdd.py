from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from typing import List

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

from backend.scrapes.wsi import utils

# SCRAPE
API_SCRAPE_NAME = "daily_observed_wdd"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
WSI Trader API - Section 10: Historical Observations
Endpoint: GetHistoricalObservations
HistoricalProductID: HISTORICAL_WEIGHTED_DEGREEDAYS

Returns either the Daily or EIA Week observed CDD and HDD for the entered
North America Gas Region for each day between the StartDate and EndDate entered.

IsDaily:
    true  -> weighted forecasts only for individual days
    false -> weekly average (EIA week for gas, M-F for temperature)

DataTypes (HISTORICAL_WEIGHTED_DEGREEDAYS):
    gas_hdd, gas_cdd, oil_hdd, oil_cdd,
    electric_hdd, electric_cdd, population_hdd, population_cdd
"""

REGION_LIST: List[str] = [
    "EAST", "MIDWEST", "MOUNTAIN", "PACIFIC", "SOUTHCENTRAL",
    "CONUS", "GASPRODUCING", "GASCONSEAST", "GASCONSWEST",
]

DATA_TYPES: List[str] = [
    "gas_hdd", "gas_cdd", "oil_hdd", "oil_cdd",
    "electric_hdd", "electric_cdd", "population_hdd", "population_cdd",
]

PRIMARY_KEYS: List[str] = [
    "date",
    "data_type",
]


def _format(
        df: pd.DataFrame,
        region: str,
    ) -> pd.DataFrame:

    # clean up cols
    df.columns = df.columns.str.upper()
    df = df.rename({"SITE_ID": "REGION", "VALID_TIME": "DATE"}, axis=1)

    return df


def _pull(
        start_date: str,
        end_date: str,
        region: str,
        TempUnits: str = "F",
        IsDaily: str = "true",
        DataTypes: List[str] = DATA_TYPES,
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetHistoricalObservations",
        HistoricalProductID: str = "HISTORICAL_WEIGHTED_DEGREEDAYS",
    ) -> pd.DataFrame:

    params_dict = {
        "StartDate": start_date,
        "EndDate": end_date,
        "TempUnits": TempUnits,
        "HistoricalProductID": HistoricalProductID,
        "IsDaily": IsDaily,
        "CityIds[]": region,
        "DataTypes[]": DataTypes,
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    # pull csv file
    df = utils._pull_wsi_trader_csv_data(
        base_url=base_url,
        params_dict=params_dict,
        skiprows=1,
    )

    # format dataframe
    df = _format(df, region)

    return df


def _format_helper(
        df: pd.DataFrame,
        region_list: List[str],
    ) -> pd.DataFrame:

    # clean up cols
    df.columns = df.columns.str.upper()

    # melt columns
    df_melt = df.melt(id_vars=['DATE', 'REGION'], var_name='DATA_TYPE', value_name='DATA_TYPE_VALUES')
    # create pivot on region
    df_pivot = pd.pivot_table(
        df_melt,
        index=['DATE', 'DATA_TYPE'],
        columns=['REGION'],
        values='DATA_TYPE_VALUES',
        aggfunc='sum',
        fill_value=0,
    )
    # clean up pivot
    df = df_pivot.reset_index()
    df.columns = list(df.columns)

    # DTYPES
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    df['DATA_TYPE'] = df['DATA_TYPE'].astype(str)
    for region in region_list:
        df[region] = df[region].astype(float)

    # sort dataframe
    df = df.sort_values(['DATE', 'DATA_TYPE'])
    df = df.reset_index(drop=True)
    df = df[['DATE', 'DATA_TYPE'] + region_list]

    # lowercase column names
    df.columns = df.columns.str.lower()

    return df


def _pull_helper(
        start_date: str,
        end_date: str,
        region_list: List[str] = REGION_LIST,
        DataTypes: List[str] = DATA_TYPES,
        IsDaily: str = "true",
    ) -> pd.DataFrame:

    frames: list[pd.DataFrame] = []
    for region in region_list:
        logger.info(f"region: {region}")

        df_region = _pull(
            start_date=start_date,
            end_date=end_date,
            region=region,
            IsDaily=IsDaily,
            DataTypes=DataTypes,
        )
        frames.append(df_region)

    df = pd.concat(frames, axis=0, ignore_index=True)
    # drop duplicate columns
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # format dataframe
    df = _format_helper(df=df, region_list=region_list)

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=PRIMARY_KEYS,
    )


def _backfill(
        start_date: datetime = datetime(2010, 1, 1),
        end_date: datetime = datetime.today(),
        delta: relativedelta = relativedelta(months=6),
    ):

    current_date = start_date
    while current_date <= end_date:

        params = {
            "start_date": current_date.strftime("%m/%d/%Y"),
            "end_date": (current_date + delta).strftime("%m/%d/%Y"),
        }
        logger.info(f"Upserting from {params['start_date']} to {params['end_date']} ...")

        main(
            start_date=params['start_date'],
            end_date=params['end_date'],
        )

        current_date += delta


def main(
        start_date: str = (datetime.today() - timedelta(days=7)).strftime("%m/%d/%Y"),
        end_date: str = datetime.today().strftime("%m/%d/%Y"),
    ):

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="wsi",
        target_table=f"wsi.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        logger.section(f"Pulling observed WDD data from {start_date} to {end_date}...")
        df = _pull_helper(
            start_date=start_date,
            end_date=end_date,
        )

        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)

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
    # df = _backfill()
