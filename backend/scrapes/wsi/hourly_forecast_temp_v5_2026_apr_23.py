"""
WSI Hourly Forecast Temp — v5 (revision-tracking, PJM only).

Differences vs v4:
  - PJM stations only (~34 sites instead of all ~440 across regions)
  - Adds `forecast_execution_datetime_utc` column, parsed from the WSI CSV
    header line `Hourly Forecast Made <Mon> <DD> <YYYY> <HHMM> UTC`
  - PK is `(forecast_execution_datetime_utc, local_time, region, site_id,
    station_name)` so each scrape preserves prior revisions instead of
    overwriting them.

Idempotency: if two scrapes see the same WSI revision (same `Made` time) the
second is a per-row upsert no-op. Safe to run more often than once a day, but
the production schedule is daily at 09:00 ET.
"""

import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

from backend.scrapes.wsi import utils

API_SCRAPE_NAME = "hourly_forecast_temp_v5_2026_apr_23"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# Matches: "NA-KBOS , Hourly Forecast Made Apr 22 2026 2102 UTC"
_MADE_REGEX = re.compile(r"Hourly Forecast Made (\w+ \d+ \d{4} \d{4}) UTC")


def _parse_made_timestamp(first_line: str) -> datetime:
    """Parse the WSI CSV header's 'Made <date> UTC' field into a naive UTC datetime."""
    match = _MADE_REGEX.search(first_line)
    if not match:
        raise ValueError(
            f"Could not parse 'Hourly Forecast Made ... UTC' from header line: {first_line!r}"
        )
    return datetime.strptime(match.group(1), "%b %d %Y %H%M")


def _format(
        df: pd.DataFrame,
        region: str,
        site_id: str,
        station_name: str,
        forecast_execution_datetime_utc: datetime,
    ) -> pd.DataFrame:

    df.rename(columns={"LocalTime": "local_time"}, inplace=True)

    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("(", "_", regex=False)
        .str.replace(")", "", regex=False)
    )

    df["forecast_execution_datetime_utc"] = forecast_execution_datetime_utc
    df["region"] = region
    df["site_id"] = site_id
    df["station_name"] = station_name

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
    >>> WSI Hourly Forecast for North American cities (degrees Fahrenheit).
    >>> Returns the issue time (`Made ... UTC`) attached to every row so PK can
    >>> include it without an extra API call.
    """

    params_dict = {
        "region": wsi_region,
        "SiteIds[]": site_id,
        "TempUnits": TempUnits,
        "timeutc": timeutc,
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    # Pull raw text first so we can parse the issue time from the header line
    # before handing the body to pandas.
    content = utils._pull_wsi_trader_text_data(
        base_url=base_url,
        params_dict=params_dict,
        min_lines=2,
    )
    lines = content.splitlines()
    forecast_execution_datetime_utc = _parse_made_timestamp(lines[0])

    df = utils._read_csv_from_content(content=content, skiprows=1)
    utils._validate_dataframe(
        df=df,
        context=base_url,
        required_columns=["LocalTime"],
    )

    df = _format(
        df=df,
        region=region,
        site_id=site_id,
        station_name=station_name,
        forecast_execution_datetime_utc=forecast_execution_datetime_utc,
    )

    return df


def _pull_helper(
        wsi_trader_city_ids: dict,
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetHourlyForecast",
        wsi_region: str = "NA",
        TempUnits: str = "F",
        timeutc: str = "false",  # local prevailing timezone (default)
    ) -> pd.DataFrame:

    frames: list[pd.DataFrame] = []

    for region in wsi_trader_city_ids.keys():
        logger.info(f"{region}")

        region_site_ids = wsi_trader_city_ids[region].keys()
        region_station_names = wsi_trader_city_ids[region].values()
        for site_id, station_name in zip(region_site_ids, region_station_names):
            logger.info(f"\t{station_name} .. {site_id}")

            df_station = _pull(
                region=region,
                site_id=site_id,
                station_name=station_name,
                base_url=base_url,
                wsi_region=wsi_region,
                TempUnits=TempUnits,
                timeutc=timeutc,
            )
            frames.append(df_station)

    df = pd.concat(frames, axis=0, ignore_index=True)

    keys: list[str] = [
        "forecast_execution_datetime_utc",
        "local_time",
        "region",
        "site_id",
        "station_name",
    ]
    df = df[keys + [col for col in df.columns if col not in keys]]
    df = df.sort_values(by=keys)
    df = df.reset_index(drop=True)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    PRIMARY_KEYS: list[str] = [
        "forecast_execution_datetime_utc",
        "local_time",
        "region",
        "site_id",
        "station_name",
    ]

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=PRIMARY_KEYS,
    )


def main():

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="weather",
        target_table=f"wsi.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        logger.section("Getting WSI site IDs (PJM only)...")
        all_city_ids = utils._get_wsi_site_ids()[0]
        wsi_trader_city_ids = {"PJM": all_city_ids["PJM"]}
        logger.info(f"\t{len(wsi_trader_city_ids['PJM'])} PJM stations")

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

    if "df" in locals() and df is not None:
        return df


if __name__ == "__main__":
    df = main()
