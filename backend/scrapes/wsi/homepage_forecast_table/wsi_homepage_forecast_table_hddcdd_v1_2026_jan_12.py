"""
Return the WSI HDD/CDD forecast in degrees Fahrenheit for all cities in each configured pool region.
"""

from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

from backend.scrapes.wsi import utils
from backend.scrapes.wsi.homepage_forecast_table import utils as homepage_utils

# SCRAPE
API_SCRAPE_NAME = "wsi_homepage_forecast_table_hddcdd_v1_2026_jan_12"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def _parse_content(
        content: str,
        region: str,
        metric: str,
    ) -> pd.DataFrame:

    return homepage_utils.parse_content(
        content=content,
        region=region,
        metric=metric,
        mode="hddcdd",
        logger=logger,
    )


def _pivot_forecast(
        df: pd.DataFrame,
    ) -> pd.DataFrame:

    return homepage_utils.pivot_forecast(
        df=df,
        normal_columns=['normals'],
    )


def _format(
        df: pd.DataFrame,
    ) -> pd.DataFrame:

    return homepage_utils.format_forecast(
        df=df,
        primary_keys=['forecast_datetime', 'forecast_date', 'wsi_forecast_table_city', 'region', 'site_id', 'station_name'],
    )


def _pull(
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetCityTableForecast",
        region: str = "NEISO",
        metric: str = "DegreeDays",
    ) -> pd.DataFrame:

    region_formatted_str = region.replace('-pool', '') if region.endswith('-pool') else region
    region_formatted_str = region_formatted_str.replace('STANDARD', '') if region_formatted_str.endswith('STANDARD') else region_formatted_str

    params_dict = {
        "SiteId": f'{region}',
        "CurrentTabName": metric,
        "IsCustom": False,
        "TempUnits": "F",
        "Region": "NA",
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")
    logger.info(f"	region_formatted_str: {region_formatted_str}")

    content = utils._pull_wsi_trader_text_data(
        base_url=base_url,
        params_dict=params_dict,
        min_lines=3,
    )

    df = _parse_content(content=content, region=region_formatted_str, metric=metric)
    if df.empty:
        raise ValueError(f"Homepage DegreeDays response parsed to empty dataframe for region={region}")
    df = _pivot_forecast(df=df)
    df = _format(df=df)

    return df


def _pull_helper() -> pd.DataFrame:

    regions: list[str] = utils._get_wsi_forecast_table_city_ids()
    regions = [region for region in regions if region.endswith('-pool')]

    df = pd.DataFrame()
    for region in regions:
        logger.info(f"region: {region}")
        df_region = _pull(region=region)
        df = pd.concat([df, df_region])

    primary_keys: list[str] = [
        'forecast_datetime',
        'forecast_date',
        'wsi_forecast_table_city',
        'region',
        'site_id',
        'station_name',
    ]

    df.reset_index(drop=True, inplace=True)
    df = df.drop_duplicates(subset=primary_keys)

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_keys: list[str] = [
        'forecast_datetime',
        'forecast_date',
        'wsi_forecast_table_city',
        'region',
        'site_id',
        'station_name',
    ]

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_keys,
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

        df = _pull_helper()
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
