"""
Energy Aspects — Monthly ISO Load Model.

Mapping ID: 474
Dataset IDs: 146228-146243
Source: .refactor/energy_aspects_v1_2025_dec_28/monthly_iso_load_model.csv
"""

from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.energy_aspects import energy_aspects_api_utils as ea_api

API_SCRAPE_NAME = "monthly_iso_load_model"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

DATASET_IDS = list(range(146228, 146244))  # 146228-146243

COLUMN_MAP = {
    "146228": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_pjm_mw",
    "146229": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_ercot_mw",
    "146230": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_nyiso_mw",
    "146231": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_caiso_mw",
    "146232": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_isone_mw",
    "146233": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_spp_mw",
    "146234": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_miso_mw",
    "146235": "ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_us48_mw",
    "146236": "ea_actual_load_fcst_load_norm_weather_pjm_mw",
    "146237": "ea_actual_load_fcst_load_norm_weather_ercot_mw",
    "146238": "ea_actual_load_fcst_load_norm_weather_nyiso_mw",
    "146239": "ea_actual_load_fcst_load_norm_weather_caiso_mw",
    "146240": "ea_actual_load_fcst_load_norm_weather_spp_mw",
    "146241": "ea_actual_load_fcst_load_norm_weather_miso_mw",
    "146242": "ea_actual_load_fcst_load_norm_weather_isone_mw",
    "146243": "ea_actual_load_fcst_load_norm_weather_us48_mw",
}

# Note: The v1 CSV also has 8 "ea_mod_hist_fcst_p90_peak_load_*_mw" columns
# that are not in the mapping_id=474 dataset list. These may come from a
# different mapping or be derived. They are omitted until identified.


def _pull(
    date_from: str = "2018-01-01",
    date_to: str = "2079-01-01",
) -> pd.DataFrame:
    logger.info(f"Pulling {len(DATASET_IDS)} datasets from EA API...")
    df = ea_api.pull_timeseries(DATASET_IDS, date_from=date_from, date_to=date_to)
    return df


def _format(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_MAP)
    return ea_api.make_postgres_safe_columns(df)


def _upsert(
    df: pd.DataFrame,
    schema: str = "energy_aspects",
    table_name: str = API_SCRAPE_NAME,
) -> None:
    data_types = azure_postgresql.infer_sql_data_types(df=df)
    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=["date"],
    )


def main():
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="energy_aspects",
        target_table=f"energy_aspects.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        df = _pull()

        if df.empty:
            logger.section("No data returned, skipping upsert.")
        else:
            df = _format(df)
            logger.section(f"Upserting {len(df)} rows, {len(df.columns)} columns...")
            _upsert(df)
            logger.success(f"Upserted {len(df)} rows.")

        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    return df


if __name__ == "__main__":
    df = main()
