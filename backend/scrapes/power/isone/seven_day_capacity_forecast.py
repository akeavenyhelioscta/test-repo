from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path

import pandas as pd

from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.power.isone import isone_api_utils as isone_api

# SCRAPE
API_SCRAPE_NAME = "seven_day_capacity_forecast"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def _format(
        df: pd.DataFrame,
        start_date: datetime = None,
    ) -> pd.DataFrame:
    """"""

    # if already formatted, return as-is (runner may call _format twice)
    if 'date' in df.columns and 'forecast_execution_date' in df.columns:
        return df

    # drop the D column (useless, all values are "D")
    drop_cols = [col for col in df.columns if col.strip().lower() == 'd']
    df.drop(columns=drop_cols, inplace=True, errors='ignore')

    # set first column (metric names like "High Temperature - Boston") as index, then transpose
    df = df.set_index(df.columns[0])
    df = df.T
    df.index.name = "Date"
    df.reset_index(inplace=True)
    df.columns.name = None

    # clean column names: drop nan/empty, deduplicate
    df.columns = df.columns.astype(str)
    df = df.loc[:, (df.columns != 'nan') & (df.columns != 'None') & (df.columns != '')]
    df = df.loc[:, ~df.columns.duplicated()]

    # Convert to date (coerce to handle non-date strings)
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df = df.dropna(subset=["Date"])
    df["Date"] = df["Date"].dt.date
    df["forecast_execution_date"] = pd.to_datetime(start_date).date()

    # format columns ... underscores and lower case
    df.columns = df.columns.astype(str).str.strip().str.replace(' - ', '_').str.replace(' ', '_').str.lower()

    # rename
    df.rename(columns={
        "total_capacity_supply_obligation_(cso)": "total_capacity_supply_obligation",
        "anticipated_de-list_mw_offered": "anticipated_de_list_mw_offered",
        "projected_surplus/(deficiency)": "projected_surplus_deficiency",
        }, inplace=True)

    # reindex
    df = df[["forecast_execution_date", "date"] + [col for col in df.columns if col not in ["forecast_execution_date", "date"]]]

    # validate expected columns exist
    expected_cols = ["forecast_execution_date", "date"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns after formatting: {missing}. Got: {df.columns.tolist()}")

    return df


def _pull(
        start_date: datetime = None,
    ) -> pd.DataFrame:
    """
    Seven-Day Capacity Forecast
    https://www.iso-ne.com/markets-operations/system-forecast-status/seven-day-capacity-forecast

    Example:
    >>> https://www.iso-ne.com/transform/csv/sdf?start=20240822"
    """
    if start_date is None:
        start_date = datetime.now() + timedelta(days=1)

    # build url
    url = f"https://www.iso-ne.com/transform/csv/sdf?start={start_date.strftime('%Y%m%d')}"

    # get response
    response = isone_api.make_request(url=url, logger=logger)

    # pull data
    df = isone_api.parse_csv_response(
        response,
        skiprows=[0, 1, 2, 3, 4, 5, 7, 12, 27, 28],
    )

    # format
    df = _format(df=df, start_date=start_date)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "isone",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_key_candidates = ["forecast_execution_date", "date"]
    primary_keys = [col for col in primary_key_candidates if col in df.columns]
    if not primary_keys:
        raise ValueError(
            f"No valid primary keys found for {schema}.{table_name}. "
            f"Expected one of {primary_key_candidates}, got columns={df.columns.tolist()}"
        )

    data_types = azure_postgresql.get_table_dtypes(
        database=database,
        schema=schema,
        table_name=table_name,
        columns=df.columns.tolist(),
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_keys,
    )


def main(
        start_date: datetime = None,
        end_date: datetime = None,
        delta: relativedelta = relativedelta(days=1),
    ):
    if start_date is None:
        start_date = datetime.now() - relativedelta(days=3)
    if end_date is None:
        end_date = datetime.now() + relativedelta(days=1)

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"isone.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    total_rows = 0
    dfs = []

    try:
        logger.header(f"{API_SCRAPE_NAME}")

        current_date = start_date
        while current_date <= end_date:
            try:
                logger.section(f"Pulling data for {current_date}...")
                df = _pull(
                    start_date=current_date,
                )

                logger.section(f"Upserting {len(df)} rows...")
                _upsert(df)
                total_rows += len(df)
                dfs.append(df)

                logger.success(f"Successfully pulled and upserted data for {current_date}!")

            except Exception as e:
                logger.warning(f"Skipping {current_date}: {e}")

            current_date += delta

        if total_rows == 0:
            raise RuntimeError("No data was successfully processed across all dates")

        run.success(rows_processed=total_rows)

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        raise

    finally:
        logging_utils.close_logging()

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


if __name__ == "__main__":
    df = main()
