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
API_SCRAPE_NAME = "hourly_forecast_temp_v4_2025_jan_12"

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
    # ['LocalTime', ' Temp', ' TempDiff', ' TempNormal', ' DewPoint', ' Cloud Cover', ' FeelsLikeTemp', ' FeelsLikeTempDiff', ' Precip', ' WindDir', ' WindSpeed(mph)', ' GHIrradiance ']
    df.rename(columns={'LocalTime': 'local_time'}, inplace=True)

    # lower case and normalize
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("(", "_", regex=False)
        .str.replace(")", "", regex=False)
    )

    # add params
    df["region"] = region
    df["site_id"] = site_id
    df["station_name"] = station_name

    # Data Types
    df["local_time"] = pd.to_datetime(df["local_time"])

    return df


def _pull(
        region: str,
        site_id: str,
        station_name: str,
        base_url: str,
        wsi_region: str,
        TempUnits: str,
        timeutc: str,
    ) -> pd.DataFrame:
    """
    >>> Return the data from the WSI Hourly Forecast for multiple cities in North America in degrees Fahrenheit
    >>> https://www.wsitrader.com/Services/CSVDownloadService.svc/GetHourlyForecast?Account=username&Profile=name@wsi.com&Password=password&region=NA&SiteIds[]=KBOS&SiteIds[]=KLAX&TempUnits=Fs=F
    """

    params_dict = {
        "region": wsi_region,
        "SiteIds[]": site_id,
        "TempUnits": TempUnits,
        "timeutc": timeutc,
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    # pull csv file
    df = utils._pull_wsi_trader_csv_data(
        base_url=base_url,
        params_dict=params_dict,
        skiprows=1,
        required_columns=["LocalTime"],
    )

    # format dataframe
    df = _format(df=df, region=region, site_id=site_id, station_name=station_name)

    return df


def _pull_helper(
        wsi_trader_city_ids: dict,
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetHourlyForecast",
        wsi_region: str = "NA",
        TempUnits: str = "F",
        timeutc: str = "false",  # false – Date and Time are returned in local prevailing timezone (Default)
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
                region = region,
                site_id = site_id,
                station_name = station_name,
                base_url = base_url,
                wsi_region = wsi_region,
                TempUnits = TempUnits,
                timeutc = timeutc,
            )
            frames.append(df_station)

    df = pd.concat(frames, axis=0, ignore_index=True)

    # sort
    keys: list[str] = ["local_time", "region", "site_id", "station_name"]
    df = df[keys + [col for col in df.columns if col not in keys]]
    df = df.sort_values(by=keys)
    df = df.reset_index(drop=True)

    return df

def _upsert(
        df: pd.DataFrame,
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    # move key columns to the front
    PRIMARY_KEYS: list[str] = [
        "local_time",
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


def main():

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
        logger.section("Pulling hourly forecast data...")
        df = _pull_helper(wsi_trader_city_ids=wsi_trader_city_ids)

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
