import logging
from io import StringIO
from typing import List

import pandas as pd

from backend.utils import azure_postgresql_utils as azure_postgresql

from backend.scrapes.wsi import utils

logger = logging.getLogger(__name__)

"""
"""

# move key columns to the front
PRIMARY_KEYS: List[str] = [
    'period_start', 
    'period_end',
    'period', 
    'model', 
    'site_id', 
    'bias_corrected',
    'init_time', 
]

def _format(df: pd.DataFrame) -> pd.DataFrame:

    # Keep as datetime objects, don't convert to strings
    df["init_time"] = pd.to_datetime(df['init_time'])
    
    # Keep as datetime objects
    for col in ['period_start', 'period_end']: 
        df[col] = pd.to_datetime(df[col]).dt.date

    logger.debug(f"Primary Keys: {PRIMARY_KEYS}")
    other_columns = [col for col in df.columns if col not in PRIMARY_KEYS]
    df = df[PRIMARY_KEYS + other_columns]

    # floats
    for col in df.columns:
        if col not in PRIMARY_KEYS:
            df[col] = df[col].astype(float)

    # NOTE: check dtypes
    sql_data_types: list = azure_postgresql.infer_sql_data_types(df=df)
    logger.debug(f"Data Types: {sql_data_types}")

    for col, data_type in zip(df.columns, sql_data_types):
        logger.debug(f"\t{col}: {data_type}")

    return df


def _pull(
        model: str,
        bias_corrected, 
        data_types: List[str],
        stations: List[str],
        forecast_type: str = "Daily",
        region: str = "NA",
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetWeightedDegreeDayForecast",
    ) -> pd.DataFrame:

    params_dict = {
        "forecasttype": forecast_type,
        "Model": model,
        "BiasCorrected": bias_corrected,
        "DataTypes[]": data_types, 
        "Stations[]": stations,
        "Region": region,
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    content = utils._pull_wsi_trader_text_data(
        base_url=base_url,
        params_dict=params_dict,
        min_lines=3,
    )
         
    # get data
    df = pd.read_csv(StringIO(content), header=None, skiprows=2)
    
    # NOTE: Drop columns that are NaN
    logger.debug(f'Before dropping: {df.shape}')
    df = df.dropna(axis=1, how='all')
    logger.debug(f'After dropping: {df.shape}')

    # NOTE: add back columns
    columns = pd.read_csv(StringIO(content), skiprows=1).columns.tolist()
    df.columns = columns
    logger.debug(f'Final shape: {df.shape}')

    # NOTE: add model for "WSI"
    if "model" not in df.columns: df['model'] = model
    if "bias_corrected" not in df.columns: df['bias_corrected'] = bias_corrected

    if df.empty:
        raise ValueError(f"WDD response parsed to empty dataframe for model={model}, bias_corrected={bias_corrected}")
    required_columns = ['period_start', 'period_end', 'period', 'site_id', 'init_time']
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(
            f"WDD response missing required columns for model={model}, bias_corrected={bias_corrected}. "
            f"Missing={missing_columns}, Actual={df.columns.tolist()}"
        )
    
    return df


def pull(
        model: str = "WSI",
        bias_corrected: str = "false", 
        data_types: List[str] = ["gas_hdd", "gas_cdd", "electric_hdd", "electric_cdd", "population_hdd", "population_cdd"],
        stations: List[str] = ["CONUS", "EAST", "MIDWEST", "SOUTHCENTRAL", "MOUNTAIN", "PACIFIC", "GASCONSEAST", "GASPRODUCING", "GASCONSWEST"],
    ):

    df: pd.DataFrame = _pull(
        model=model,
        bias_corrected=bias_corrected,
        data_types=data_types,
        stations=stations,
    )

    df: pd.DataFrame = _format(df=df)

    return df

"""
NOTE: examples
"""

def _example_pull_wsi(
        model: str = "WSI",
    ):

    df = pull(model=model)
    return df

def _example_pull_gfs_op(
        model: str = "GFS_OP",
    ):

    df = pull(model=model)
    return df

def _example_pull_gfs_ens(
        model: str = "GFS_ENS",
    ):

    df = pull(model=model)
    return df

def _example_pull_ecmwf_op(
        model: str = "ECMWF_OP",
    ):

    df = pull(model=model)
    return df

def _example_pull_ecmwf_ens(
        model: str = "ECMWF_ENS",
    ):

    df = pull(model=model)
    return df

"""
"""

if __name__ == "__main__":
    
    # df_wsi = _example_pull_wsi()
    # df_gfs_op = _example_pull_gfs_op()
    # df_gfs_ens = _example_pull_gfs_ens()
    # df_ecmwf_op = _example_pull_ecmwf_op()
    df_ecmwf_ens = _example_pull_ecmwf_ens()
