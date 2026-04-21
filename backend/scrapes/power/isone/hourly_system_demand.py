from datetime import datetime
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
API_SCRAPE_NAME = "hourly_system_demand"

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

    # Drop unwanted columns
    df.drop(columns=['h'], inplace=True, errors='ignore')

    # Convert to date
    df['date'] = pd.to_datetime(df['date']).dt.date

    # pad hour ending
    df['hour_ending'] = df['hour_ending'].astype(str).str.pad(2, side='left', fillchar='0')

    # drop if X appears in hour ending
    df = df[~df['hour_ending'].str.contains('X')]

    return df


def _pull(
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> pd.DataFrame:
    """
    https://www.iso-ne.com/isoexpress/web/reports/load-and-demand

    Example:
    >>> https://www.iso-ne.com/transform/csv/hourlysystemdemand?start=20240814&end=20240814"
    """
    if start_date is None:
        start_date = datetime.now()
    if end_date is None:
        end_date = datetime.now()

    # build url
    url = f"https://www.iso-ne.com/transform/csv/hourlysystemdemand?start={start_date.strftime('%Y%m%d')}&end={end_date.strftime('%Y%m%d')}"

    # get response
    response = isone_api.make_request(url=url, logger=logger)

    # pull data
    df = isone_api.parse_csv_response(response)

    # format data
    df = _format(df)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "isone",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_key_candidates = ["date", "hour_ending"]
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
        start_date = datetime.now() - relativedelta(days=7)
    if end_date is None:
        end_date = datetime.now()

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
                    end_date=current_date + delta,
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
