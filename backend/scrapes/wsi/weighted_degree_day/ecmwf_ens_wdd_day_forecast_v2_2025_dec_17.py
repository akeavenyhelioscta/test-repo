from typing import List, Tuple
from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)


from backend.scrapes.wsi.weighted_degree_day import utils

# SCRAPE
API_SCRAPE_NAME = "ecmwf_ens_wdd_day_forecast_v2_2025_dec_17"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""

def _upsert(
        df: pd.DataFrame,
        schema: str = "wsi",
        table_name: str = API_SCRAPE_NAME,
    ):

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)
    primary_keys: list = utils.PRIMARY_KEYS

    azure_postgresql.upsert_to_azure_postgresql(
        schema = schema,      
        table_name = table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_keys,
    )


def main(
    model_list: str = ['ECMWF_ENS'],
    bias_corrected_list: List[str] = ["false", "true"],
    data_types: List[str] = ["gas_hdd", "gas_cdd", "electric_hdd", "electric_cdd", "population_hdd", "population_cdd"],
    stations: List[str] = ["CONUS", "EAST", "MIDWEST", "SOUTHCENTRAL", "MOUNTAIN", "PACIFIC", "GASCONSEAST", "GASPRODUCING", "GASCONSWEST"],
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

        for model in model_list:
            for bias_corrected in bias_corrected_list:
    
                df = utils.pull(
                    model=model,
                    bias_corrected=bias_corrected,
                    data_types=data_types,
                    stations=stations,
                )

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