from datetime import datetime
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
API_SCRAPE_NAME = "weighted_temp_daily_forecast_iso_wsi_v2_2026_jan_12"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""

def _format(df: pd.DataFrame, model, bias_corrected) -> pd.DataFrame:
    """
    """

    # add model and bias_corrected
    if "model" not in df.columns: df['model'] = model
    if "bias_corrected" not in df.columns: df['bias_corrected'] = bias_corrected

    # add cols
    df["period"] = df["forecast_date_merged"].str.split("-").str[0]
    df["forecast_date"] = df["forecast_date_merged"].str.split("-").str[1]
    df["forecast_execution_date"] = datetime.today().date()

    # column names
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.rstrip('_')

    # data types
    date_cols = ['forecast_execution_date', 'forecast_date'] 
    for col in date_cols:
        df[col] = pd.to_datetime(df[col]).dt.date
    str_cols = ['period', 'model', 'bias_corrected', 'region']
    for col in str_cols:
        df[col] = df[col].astype(str)
    float_cols = ['min_temp', 'max_temp', 'min_temp_diff', 'max_temp_diff', 'min_temp_dfn', 'max_temp_dfn', 'average_temp_dfn', 'hdd', 'hdd_diff', 'cdd', 'cdd_diff', 'heat_index', 'heat_index_diff', 'diff_from_max_temp']
    for col in float_cols:
        df[col] = df[col].replace('', pd.NA)
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    cols = date_cols + str_cols + float_cols
    missing_cols = [column for column in cols if column not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns after formatting WSI ISO forecast: {missing_cols}")
    df = df[cols]

    return df


def _parse_content_for_regions(content: str, region: str) -> pd.DataFrame:

    # get idx for given region
    start_index = content.splitlines().index(region)
    end_index = content.splitlines()[start_index:].index('')
    lines = content.splitlines()[start_index:start_index+end_index]

    # get raw data
    region = lines[0]
    raw_cols = lines[1]
    raw_rows = lines[2:-1]

    # clean up columns
    cols = ['region', 'forecast_date_merged'] + [col.strip().strip("'") for col in raw_cols.split(',') if col.strip()]
    df_region = pd.DataFrame(columns=cols)

    for idx, row in enumerate(raw_rows):
        row = [region] + [value.strip().strip("'") for value in raw_rows[idx].split(',') if value.strip()]
        df_region.loc[idx] = row
    
    return df_region


def _pull(
        model: str,
        bias_corrected: str,
        base_url: str,
    ) -> pd.DataFrame:

    params_dict = {
        "Model": model,
        "BiasCorrected": bias_corrected,
        "Region": "NA",
        "forecasttype": "Daily",
        "TempUnits": "F",
        "ShowDifferences": "true",
        "showdecimals": "true",
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    content = utils._pull_wsi_trader_text_data(
        base_url=base_url,
        params_dict=params_dict,
        min_lines=3,
    )

    # TODO: get regions
    # regions = ['NEISO','NYISO','IESO','PJM','PJM EAST','PJM WEST','PJM SOUTH','SOUTHEAST','ERCOT','MISO','MISO NORTH','MISO SOUTH','SPP','NWPP','INTERIOR NORTHWEST','PACIFIC NORTHWEST','ROCKY MOUNTAIN','SOUTHWEST','AESO','CAISO','NORTHERN CAISO','SOUTHERN CAISO','US EASTERN INTERCONNECT','US NATIONAL']
    df_regions = pd.read_csv(StringIO(content), delimiter=',', skiprows=0, on_bad_lines='skip')
    regions = df_regions.iloc[:,0].unique().tolist()
    if not regions:
        raise ValueError(f"No regions found in WSI ISO forecast response for model={model}, bias_corrected={bias_corrected}")
    logger.info(f"regions: {regions}")

    # Parse content based on regions
    df = pd.DataFrame()
    for region in regions:
        df_region = _parse_content_for_regions(content, region)
        df = pd.concat([df, df_region])
    df.reset_index(drop=True, inplace=True)
    if df.empty:
        raise ValueError(f"Parsed WSI ISO forecast response is empty for model={model}, bias_corrected={bias_corrected}")

    # format
    df = _format(
        df=df, 
        model=model, 
        bias_corrected=bias_corrected,
    )
    
    return df


def _pull_helper(
        models: List[str] = ["WSI"],
        bias_corrected: str = ["false", "true"],
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetModelForecast",
    ) -> pd.DataFrame:

    df = pd.DataFrame()
    for model in models:
        for bias in bias_corrected:
            df_model = _pull(
                model=model, 
                bias_corrected=bias,
                base_url=base_url,
            )
            df = pd.concat([df, df_model])
            
    df.reset_index(drop=True, inplace=True)

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    # move key columns to the front
    PRIMARY_KEYS: List[str] = [
        'forecast_execution_date', 
        'forecast_date', 
        'period', 
        'model', 
        'bias_corrected', 
        'region',
    ]

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)
    # logger.info(data_types)

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

        # pull
        df = _pull_helper()

        # upsert
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
