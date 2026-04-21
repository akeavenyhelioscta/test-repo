"""
Energy Aspects — US Regional Power Model.

Comprehensive regional power generation + demand + natural gas demand model
across all major US ISOs/regions (CAISO, ERCOT, ISONE, MISO, NYISO, PJM,
SPP, Northwest, Southwest, Southeast, West, US48).

175 datasets: 139 power (generation/demand in MW) + 36 natural gas (demand in bcf/d).
Source: .refactor/energy_aspects_v1_2025_dec_28/us_regional_power_model.csv
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

API_SCRAPE_NAME = "us_regional_power_model"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# Regional power model datasets (power generation/demand + natural gas demand)
# Identified from catalog: category=power with US sub_regions + category=natural_gas with bcf/d unit
DATASET_IDS = [
    # MISO power + gas
    6574, 6575, 6576, 6577, 6578, 6579, 6580, 6581, 6582, 6583, 6586, 6587, 6588, 6589,
    # SPP power + gas
    6590, 6591, 6592, 6593, 6594, 6595, 6596, 6597, 6598, 6599, 6600, 6603, 6604, 6605, 6606, 6607,
    # CAISO power + gas
    6608, 6609, 6610, 6611, 6612, 6613, 6614, 6615, 6616, 6617, 6618, 6619, 6620, 6621,
    # ISONE power + gas
    6622, 6623, 6624, 6625, 6626, 6627, 6628, 6629, 6630, 6633, 6634, 6635, 6636, 6637,
    # NYISO power + gas
    6638, 6639, 6640, 6641, 6642, 6643, 6644, 6645, 6648, 6649, 6650, 6651, 6652, 6653, 6654,
    # ERCOT power + gas
    6655, 6656, 6657, 6658, 6659, 6660, 6661, 6662, 6663, 6664, 6665, 6666, 6667, 6668, 6669,
    # PJM power + gas
    6670, 6671, 6672, 6673, 6674, 6675, 6676, 6677, 6680, 6681, 6682, 6683, 6684, 6685, 6686,
    # Northwest power + gas
    6687, 6688, 6689, 6690, 6691, 6692, 6693, 6694, 6697, 6698, 6699, 6700, 6701, 6702, 6703,
    # Southwest power + gas
    6704, 6705, 6706, 6707, 6708, 6709, 6710, 6711, 6712, 6713, 6714, 6715, 6716,
    # US48 power + gas
    6717, 6718, 6719, 6720, 6721, 6722, 6723, 6724, 6725, 6726, 6729, 6730, 6731, 6732, 6733,
    # West power + gas
    6734, 6735, 6736, 6737, 6738, 6739, 6740, 6741, 6742, 6743, 6746, 6747, 6748, 6749, 6750, 6751, 6752,
    # Southeast power + gas
    6753, 6754, 6755, 6756, 6757, 6758, 6759, 6760, 6763, 6764, 6765, 6766,
]


def _pull(
    date_from: str = "2019-01-01",
    date_to: str = "2079-01-01",
) -> pd.DataFrame:
    logger.info(f"Pulling {len(DATASET_IDS)} datasets from EA API (batched)...")
    df = ea_api.pull_timeseries(DATASET_IDS, date_from=date_from, date_to=date_to)
    return df


def _format(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building column name mapping from API metadata...")
    column_map = ea_api.build_column_map(DATASET_IDS)
    df = df.rename(columns=column_map)
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
