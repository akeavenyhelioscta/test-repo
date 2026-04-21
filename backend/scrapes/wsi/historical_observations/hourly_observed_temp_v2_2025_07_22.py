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
API_SCRAPE_NAME = "hourly_observed_temp_v2_20250722"

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
    ) -> pd.DataFrame:

    # clean up cols
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[()]", "", regex=True)
    )

    # remove % from cloud cover column
    df["cloud_cover"] = df["cloud_cover"].str.replace('%', '', regex=False).astype(float)
    df.rename(columns={"cloud_cover": "cloud_cover_pct"}, inplace=True)

    # add params
    df["region"] = region
    df["site_id"] = site_id
    df["station_name"] = station_name

    # Data Types
    df["date"] = pd.to_datetime(df["date"]).dt.date

    return df


def _pull(
        start_date: str,
        end_date: str,
        region: str,
        site_id: str,
        station_name: str,
        DataTypes: list[str] = ["temperature", "dewpoint", "cloudCover", "windDirection", "windSpeed", "heatIndex", "windChill", "relativeHumidity", "precipitation"],
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetHistoricalObservations",
        HistoricalProductID: str = "HISTORICAL_HOURLY_OBSERVED",
    ) -> pd.DataFrame:
    """
    HISTORICAL_HOURLY_OBSERVED
        - Returns the observed data for each hour of each day between the StartDate and EndDate entered

    DataTypes[] - This parameter is only applicable for the HISTORICAL_HOURLY_OBSERVED and HISTORICAL_WEIGHTED_DEGREEDAYS products. Accepted values are for each product is outlined below:

        HISTORICAL_HOURLY_OBSERVED:
            temperature - Temperature
            dewpoint - Dewpoint
            cloudCover - % Cloud Cover
            windDirection - Wind Direction in Degrees
            windSpeed - Wind Speed in mph
            heatIndex - Heat Index
            windChill - Wind Chill
            relativeHumidity - Relative Humidity
            precipitation - Precipitation
    """

    params_dict = {
        "StartDate": start_date,
        "EndDate": end_date,
        "CityIds[]": site_id,
        "HistoricalProductID": HistoricalProductID,
        "DataTypes[]": DataTypes,
        "TempUnits": "F",
        "timeutc": "false",  # true – Date and Time are returned in UTC format
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    # pull csv file
    df = utils._pull_wsi_trader_csv_data(
        base_url=base_url,
        params_dict=params_dict,
        skiprows=1,
        required_columns=["Date", "Hour"],
    )

    # format dataframe
    df = _format(df=df, region=region, site_id=site_id, station_name=station_name)

    return df


def _pull_helper(
        start_date: str,
        end_date: str,
        wsi_trader_city_ids: dict,
        DataTypes: list[str] = ["temperature", "dewpoint", "cloudCover", "windDirection", "windSpeed", "heatIndex", "windChill", "relativeHumidity", "precipitation"],
    ) -> pd.DataFrame:

    # pull data
    frames: list[pd.DataFrame] = []

    for region in wsi_trader_city_ids.keys():
        logger.info(f"{region}")

        region_site_ids = wsi_trader_city_ids[region].keys()
        region_station_names = wsi_trader_city_ids[region].values()
        for site_id, station_name in zip(region_site_ids, region_station_names):
            logger.info(f"\t{station_name} .. {site_id}")

            df_station = _pull(
                start_date = start_date,
                end_date = end_date,
                region = region,
                site_id = site_id,
                station_name = station_name,
                DataTypes = DataTypes,
            )
            frames.append(df_station)

    df = pd.concat(frames, axis=0, ignore_index=True)

    # sort
    keys: list[str] = ["date", "hour", "region", "site_id", "station_name"]
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
        "hour",
        "region",
        "site_id",
        "station_name",
    ]

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema = schema,
        table_name = table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=PRIMARY_KEYS,
    )


def _backfill(
        start_date: str = (datetime(1995, 1, 1)),
        end_date: str = datetime.today(),
        delta: relativedelta = relativedelta(months=1),
    ):

    current_date = start_date
    while current_date <= end_date:

        # dates
        params = {
            "start_date": (current_date).strftime("%m/%d/%Y"),
            "end_date": (current_date + delta).strftime("%m/%d/%Y"),
        }
        logger.info(f"Upserting from {params['start_date']} to {params['end_date']} ...")

        # pull and upsert
        main(
            start_date = params['start_date'],
            end_date = params['end_date'],
        )

        # increment
        current_date += delta


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
        logger.section("Pulling hourly observed data...")
        df = _pull_helper(
            start_date = start_date,
            end_date = end_date,
            wsi_trader_city_ids = wsi_trader_city_ids,
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
    # df = _backfill()
