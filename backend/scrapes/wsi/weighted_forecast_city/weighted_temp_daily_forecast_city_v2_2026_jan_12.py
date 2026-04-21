"""
Download the forecast data from the Primary forecast issued at 6:30am ET in degrees Fahrenheit 
https://www.wsitrader.com/Services/CSVDownloadService.svc/GetWsiForecastForDDModelCities?Account=username&Profile=name@wsi.com&Password=password&Region=NA&ForecastType=Primary&TempUnits=F&ForecastType=Primary

https://www.wsitrader.com/Services/CSVDownloadService.svc/GetWsiForecastForDDModelCities?Account=helios&Profile=Kapil.Saxena@helioscta.com&Password=calgaryabwx24&Region=NA&ForecastType=Primary&TempUnits=F&ForecastType=Primary
"""

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
API_SCRAPE_NAME = "weighted_temp_daily_forecast_city_v2_2026_jan_12"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""

def _format(
        df: pd.DataFrame,
    ) -> pd.DataFrame:

    logger.info(f"df.columns: {df.columns.tolist()}")
    # ['InitDate (UTC)', 'ValidDate', 'StationName', 'ICAO', 'MaxTemp', 'MinTemp', 'AvgTemp', 'HDD', 'CDD']

    # clean up cols
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    df.columns = df.columns.str.lower().str.strip().str.replace("(", "")
    df.columns = df.columns.str.lower().str.strip().str.replace(")", "")
    logger.info(f"df.columns: {df.columns.tolist()}")

    # Data Types
    df["initdate_utc"] = pd.to_datetime(df["initdate_utc"])
    df["validdate"] = pd.to_datetime(df["validdate"])
    df["forecast_type"] = df["forecast_type"].astype(str).str.strip().str.lower()

    # sort
    keys: List[str] = ["initdate_utc", "validdate", "stationname", "icao", "forecast_type"]
    df = df[keys + [col for col in df.columns if col not in keys]]
    df = df.sort_values(by=keys, kind="mergesort")
    duplicated_count = int(df.duplicated(subset=keys).sum())
    if duplicated_count:
        logger.warning(
            f"dropping {duplicated_count} duplicate rows on primary key candidate columns: {keys}"
        )
        df = df.drop_duplicates(subset=keys, keep="last")
    df = df.reset_index(drop=True)
    
    return df


def _pull(
        forecast_type: str = "Primary",
        base_url: str = "https://www.wsitrader.com/Services/CSVDownloadService.svc/GetWsiForecastForDDModelCities",
    ) -> pd.DataFrame:

    """
    Required Parameters:   
        
        ForecastType - The specific forecast to be retrieved.  Available options are: 
            Primary 
            Latest 
        
        TempUnits - The units you want the temperature data displayed in.  Accepted values are:  
            F - Fahrenheit  
            C - Celsius  

        Region - The region for which to receive forecast data for.  Accepted values are:  
            NA - North America 
    """

    params_dict = {
        "ForecastType": forecast_type,
        "TempUnits": "F",
        "Region": "NA",
    }

    logger.info(f"wsi_request: {utils._get_sanitized_request_context(base_url, params_dict)}")

    # pull csv file
    df = utils._pull_wsi_trader_csv_data(
        base_url=base_url,
        params_dict=params_dict,
        skiprows=0,
        required_columns=["InitDate (UTC)", "ValidDate", "StationName", "ICAO"],
    )
    df["forecast_type"] = forecast_type
    
    return df

def _pull_helper(
        forecast_type_list: List[str] = ["Primary", "Latest"],
    ) -> pd.DataFrame:

    df = pd.DataFrame()
    for forecast_type in forecast_type_list:
        df_forecast_type = _pull(
            forecast_type=forecast_type,
        )
        df = pd.concat([df, df_forecast_type])
            
    df.reset_index(drop=True, inplace=True)

    # NOTE: format
    df = _format(df=df)

    return df

def _upsert(
        df: pd.DataFrame,
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    # move key columns to the front
    PRIMARY_KEYS: List[str] = [
        "initdate_utc",
        "validdate",
        "stationname",
        "icao",
        "forecast_type",
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
