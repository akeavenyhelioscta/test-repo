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
API_SCRAPE_NAME = "three_day_reliability_region_demand_forecast"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def _format(df: pd.DataFrame) -> pd.DataFrame:

    # format columns ... underscores and lower case
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_').str.lower()
    df.rename(columns={'%': 'percentage'}, inplace=True)

    # Drop unwanted columns
    df.drop(columns=['h'], inplace=True, errors='ignore')

    # Convert to datetime
    df['published_date'] = pd.to_datetime(df['published_date'])
    # Convert to date
    df['forecast_date'] = pd.to_datetime(df['forecast_date']).dt.date

    # data types
    df['hour'] = df['hour'].astype(int)
    df['reliability_region'] = df['reliability_region'].astype(str)
    df['mw'] = df['mw'].astype(float)
    df['percentage'] = df['percentage'].astype(float)

    # re-order columns
    cols = ['published_date', 'forecast_date', 'hour', 'reliability_region', 'mw', 'percentage']
    df = df.reindex(columns=cols)

    return df


def _pull(
        start_date: datetime = None,
    ) -> pd.DataFrame:
    """
    Three-Day Reliability Region Demand Forecast
    https://www.iso-ne.com/isoexpress/web/reports/load-and-demand/-/tree/three-day-reliability-region-demand-forecast

    Example:
    >>> https://www.iso-ne.com/transform/csv/reliabilityregionloadforecast?start=20241029"
    """
    if start_date is None:
        start_date = datetime.now()

    # three day forecast
    forecast_dates = []
    for i in range(0, 3):
        forecast_dates.append(start_date + timedelta(days=i))

    df = pd.DataFrame()
    for forecast_date in forecast_dates:

        # build url
        url = f"https://www.iso-ne.com/transform/csv/reliabilityregionloadforecast?start={forecast_date.strftime('%Y%m%d')}"

        # get response
        response = isone_api.make_request(url=url, logger=logger)

        # pull data
        day_df = isone_api.parse_csv_response(response)

        # format data
        day_df = _format(day_df)

        df = pd.concat([df, day_df])

    df.reset_index(drop=True, inplace=True)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "isone",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_key_candidates = ["published_date", "forecast_date", "hour", "reliability_region"]
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
