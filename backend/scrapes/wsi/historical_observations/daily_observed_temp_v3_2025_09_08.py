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

from backend.scrapes.wsi import utils

# SCRAPE
API_SCRAPE_NAME = "daily_observed_temp_v3_2025_09_08"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def _format(
        df: pd.DataFrame,
        region: str,
        site_id: str,
        station_name: str,
        is_temp: str,
    ) -> pd.DataFrame:
    """
    Clean column names and add station metadata.

    IsTemp="true"  response cols: Date, MIN (F), MAX (F), AVG (F), PRECIP (IN)
    IsTemp="false" response cols: Date, HDD, CDD  (with a trailing "Total" row)
    """

    # drop "Total" summary row from HDD/CDD response
    if is_temp.lower() == "false":
        df = df[df["Date"] != "Total"]

    # clean up cols
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[()]", "", regex=True)
    )

    # rename to match existing table schema
    df = df.rename(columns={
        "min_f": "min",
        "max_f": "max",
        "avg_f": "avg",
        "precip_in": "precip",
    })

    # add station metadata
    df["region"] = region
    df["site_id"] = site_id
    df["station_name"] = station_name

    return df


def _pull(
        start_date: str,
        end_date: str,
        region: str,
        site_id: str,
        station_name: str,
        is_temp: str = "true",
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetHistoricalObservations",
        HistoricalProductID: str = "HISTORICAL_DAILY_OBSERVED",
    ) -> pd.DataFrame:
    """
    HISTORICAL_DAILY_OBSERVED
        - Returns observed temperature and precipitation data for each day
          between StartDate and EndDate.

    IsTemp:
        - "true"  → Min Temp, Max Temp, Avg Temp, Precip
        - "false" → HDD, CDD
    """

    params_dict = {
        "StartDate": start_date,
        "EndDate": end_date,
        "TempUnits": "F",
        "HistoricalProductID": HistoricalProductID,
        "IsTemp": is_temp,
        "CityIds[]": site_id,
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    df = utils._pull_wsi_trader_csv_data(
        base_url=base_url,
        params_dict=params_dict,
        skiprows=2,
        required_columns=["Date"],
    )

    df = _format(
        df=df,
        region=region,
        site_id=site_id,
        station_name=station_name,
        is_temp=is_temp,
    )

    return df


def _pull_helper(
        start_date: str,
        end_date: str,
        wsi_trader_city_ids: dict,
    ) -> pd.DataFrame:

    frames: list[pd.DataFrame] = []

    for region in wsi_trader_city_ids.keys():
        logger.info(f"{region}")

        region_site_ids = wsi_trader_city_ids[region].keys()
        region_station_names = wsi_trader_city_ids[region].values()
        for site_id, station_name in zip(region_site_ids, region_station_names):
            logger.info(f"\t{station_name} .. {site_id}")

            # pull temperature data (min, max, avg, precip)
            df_temp = _pull(
                start_date=start_date,
                end_date=end_date,
                region=region,
                site_id=site_id,
                station_name=station_name,
                is_temp="true",
            )

            # pull degree day data (hdd, cdd)
            df_dd = _pull(
                start_date=start_date,
                end_date=end_date,
                region=region,
                site_id=site_id,
                station_name=station_name,
                is_temp="false",
            )

            # merge temp + degree day data horizontally, drop duplicate columns
            df_station = pd.concat([df_temp, df_dd], axis=1)
            df_station = df_station.loc[:, ~df_station.columns.duplicated()].copy()

            frames.append(df_station)

    df = pd.concat(frames, axis=0, ignore_index=True)

    # data types
    df["date"] = pd.to_datetime(df["date"], format="mixed").dt.date
    for col in ["region", "site_id", "station_name"]:
        df[col] = df[col].astype(str)
    value_cols = [c for c in df.columns if c not in ["date", "region", "site_id", "station_name"]]
    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # sort
    keys: list[str] = ["date", "region", "site_id", "station_name"]
    df = df[keys + [col for col in df.columns if col not in keys]]
    df = df.sort_values(by=keys)
    df = df.reset_index(drop=True)

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    PRIMARY_KEYS: list[str] = [
        "date",
        "region",
        "site_id",
        "station_name",
    ]

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=PRIMARY_KEYS,
    )


def main(
        start_date: str = (datetime.today() - timedelta(days=7)).strftime("%m/%d/%Y"),
        end_date: str = (datetime.today() + timedelta(days=0)).strftime("%m/%d/%Y"),
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

        # get site ids
        logger.section("Getting WSI site IDs...")
        wsi_trader_city_ids = utils._get_wsi_site_ids()[0]

        # pull
        logger.section("Pulling daily observed temperature data...")
        df = _pull_helper(
            start_date=start_date,
            end_date=end_date,
            wsi_trader_city_ids=wsi_trader_city_ids,
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


if __name__ == "__main__":
    df = main()

    # start_date: str = (datetime.today() - timedelta(days=60)).strftime("%m/%d/%Y")
    # end_date: str = (datetime.today() + timedelta(days=0)).strftime("%m/%d/%Y")
    # df = main(start_date=start_date, end_date=end_date)