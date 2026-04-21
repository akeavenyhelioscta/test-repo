from io import StringIO
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
API_SCRAPE_NAME = "daily_forecast_bcf_v1_2026_mar_06"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
WSI Trader API - Section 16: Natural Gas Demand Forecasts (BCF)
Endpoint: GetModelBCFForecast
Models: WSI, GFS_OP, GFS_ENS, ECMWF_OP, ECMWF_ENS
ForecastType: Daily, Period
"""

REGIONS = ["CONUS", "EAST", "MIDWEST", "MOUNTAIN", "PACIFIC", "SOUTH CENTRAL"]

PRIMARY_KEYS: List[str] = [
    "forecast_execution_date",
    "forecast_date",
    "period",
    "model",
    "cycle",
    "region",
]


def _parse_content_for_region(content: str, region: str) -> pd.DataFrame:

    # get idx for given region
    start_index = content.splitlines().index(region)
    end_index = 18  # region, cols, Days 1 to 15
    lines = content.splitlines()[start_index:start_index + end_index]

    # get raw data
    region_name = lines[0]
    raw_cols = lines[1]
    raw_rows = lines[2:-1]

    # clean up columns
    cols = ['region', 'forecast_date_merged'] + [col.strip().strip("'") for col in raw_cols.split(',') if col.strip()]
    df_region = pd.DataFrame(columns=cols)

    for idx, row in enumerate(raw_rows):
        row_data = [region_name] + [value.strip().strip("'") for value in raw_rows[idx].split(',') if value.strip()]
        df_region.loc[idx] = row_data

    return df_region


def _pivot_with_duplicate_guard(
        df: pd.DataFrame,
        index_cols: list[str],
        columns_col: str,
        value_col: str,
        context: str,
    ) -> pd.DataFrame:

    duplicate_keys = index_cols + [columns_col]
    duplicate_mask = df.duplicated(subset=duplicate_keys, keep=False)

    if duplicate_mask.any():
        duplicate_rows = df.loc[duplicate_mask, duplicate_keys + [value_col]]
        duplicate_group_count = int(duplicate_rows[duplicate_keys].drop_duplicates().shape[0])
        conflicting_group_count = int(
            duplicate_rows.groupby(duplicate_keys, dropna=False)[value_col]
            .nunique(dropna=False)
            .gt(1)
            .sum()
        )
        logger.warning(
            f"{context}: found {duplicate_rows.shape[0]} duplicate rows across "
            f"{duplicate_group_count} key groups before pivot; "
            f"conflicting_groups={conflicting_group_count}. Using first value per key."
        )
        sample = duplicate_rows.sort_values(duplicate_keys).head(10)
        logger.info(f"{context}: duplicate sample (up to 10 rows):\n{sample.to_string(index=False)}")

    df_pivot = df.pivot_table(
        index=index_cols,
        columns=columns_col,
        values=value_col,
        aggfunc="first",
    ).reset_index()
    df_pivot.columns.name = None

    return df_pivot


def _format(df: pd.DataFrame) -> pd.DataFrame:

    # drop columns that contain "Differences"
    df = df.loc[:, ~df.columns.str.contains('Differences')]

    # melt
    value_cols = [c for c in df.columns if c not in ('region', 'forecast_date_merged', 'model')]
    df = pd.melt(
        df,
        id_vars=['region', 'forecast_date_merged', 'model'],
        value_vars=value_cols,
        var_name='data_type_merged',
        value_name='value',
    )
    df["data_type_merged"] = df["data_type_merged"].astype(str).str.strip()

    # extract merged values
    df["period"] = df["forecast_date_merged"].str.split("-").str[0]
    df["forecast_date"] = df["forecast_date_merged"].str.split("-").str[1]
    date_pattern = r"\d{1,2}/\d{1,2}/\d{4}"
    df["forecast_execution_date"] = df["data_type_merged"].str.extract(rf"({date_pattern})")[0]
    df["data_type"] = df["data_type_merged"].str.extract(rf"^(.*?)(?={date_pattern})")[0]
    df["data_type"] = df["data_type"].fillna(df["data_type_merged"]).str.strip()
    df["cycle"] = df["data_type_merged"].str.extract(rf"{date_pattern}(.*)")[0].fillna("").str.strip()

    # format data types
    df["forecast_date"] = pd.to_datetime(df["forecast_date"], errors="coerce").dt.date
    df["forecast_execution_date"] = pd.to_datetime(df["forecast_execution_date"], errors="coerce").dt.date
    missing_execution_date_mask = df["forecast_execution_date"].isna()
    if missing_execution_date_mask.any():
        fallback_execution_date = pd.Timestamp.today().normalize()
        logger.warning(
            f"Missing forecast_execution_date in {int(missing_execution_date_mask.sum())} rows; "
            f"using run date {fallback_execution_date}."
        )
        df.loc[missing_execution_date_mask, "forecast_execution_date"] = fallback_execution_date
    for col in ['period', 'model', 'cycle', 'region']:
        df[col] = df[col].astype(str)

    # normals
    normal_cols = ["BCF Normals", "BCF Industrial Normals", "BCF Powerburns Normals", "BCF Total Normals"]
    normal_keys = ['forecast_date', 'period', 'model', 'region']
    normal_mask = df["data_type_merged"].isin(normal_cols) | df["data_type_merged"].str.contains(
        r"Normals",
        case=False,
        na=False,
    )
    df_normals = df[normal_mask]
    df_normals = _pivot_with_duplicate_guard(
        df=df_normals,
        index_cols=normal_keys,
        columns_col="data_type_merged",
        value_col="value",
        context="normals pivot",
    )

    # forecast
    forecast_keys = ['forecast_execution_date', 'forecast_date', 'period', 'model', 'cycle', 'region']
    df_forecast = df[~normal_mask]
    df_forecast = _pivot_with_duplicate_guard(
        df=df_forecast,
        index_cols=forecast_keys,
        columns_col="data_type",
        value_col="value",
        context="forecast pivot",
    )

    # merge
    df = pd.merge(df_forecast, df_normals, on=normal_keys, how='left')
    df = df.sort_values(forecast_keys)
    df.reset_index(drop=True, inplace=True)
    df.columns.name = None

    # column names
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.rstrip('_')

    # NOTE: period is formatted as "01"
    df["period"] = df["period"].str.split(" ").str[1].str.zfill(2)

    return df


def _pull(
        model: str = "GFS_ENS",
        regions: List[str] = REGIONS,
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetModelBCFForecast",
    ) -> pd.DataFrame:

    params_dict = {
        "forecasttype": "Daily",
        "Model": model,
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    content = utils._pull_wsi_trader_text_data(
        base_url=base_url,
        params_dict=params_dict,
        min_lines=3,
    )

    # Parse content based on regions
    frames: list[pd.DataFrame] = []
    for region in regions:
        df_region = _parse_content_for_region(content, region)
        frames.append(df_region)

    df = pd.concat(frames, ignore_index=True)

    # add model for "WSI" (WSI response doesn't include model column)
    if "model" not in df.columns:
        df['model'] = model

    # format
    df = _format(df)

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


def main(
        models: List[str] = ["WSI", "GFS_OP", "GFS_ENS", "ECMWF_OP", "ECMWF_ENS"],
        regions: List[str] = REGIONS,
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

        rows_processed = 0

        for model in models:
            logger.info(f"model: {model}")

            df = _pull(model=model, regions=regions)
            if df.empty:
                logger.warning(f"No rows returned for model={model}; skipping upsert.")
                continue

            _upsert(df=df)

            rows_processed += len(df)

        run.success(rows_processed=rows_processed)

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
