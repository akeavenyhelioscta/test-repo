import os
import glob
import logging
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import paramiko

from backend.utils import (
    azure_postgresql_utils,
    logging_utils,
    pipeline_run_logger,
)

from backend import (
    secrets,
)

from backend.scrapes.postions_and_trades import (
    settings,
)

# SCRAPE
API_SCRAPE_NAME = "send_clear_street_trades_to_mufg_v1_2026_feb_02"

# LOGGING
LOGGING_SOURCE = "positions_and_trades"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "sftp,clear_street,mufg"
LOGGING_TARGET_TABLE = "trades_v1_2026_feb_02.marts_v1_clear_street_trades"
LOGGING_OPERATION_TYPE = "consume"

# SQL CONFIG
SQL_FILENAME = "clear_street_trades_mufg_latest.sql"
CSV_FILENAME_PATTERN = "Helios_Transactions"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
    level = logging.INFO,
)

"""
"""

def _pull_from_db(
        sql_filename: str,
        sql_dir: Path,
    ) -> pd.DataFrame:
    """"""

    sql_file_path = sql_dir / sql_filename

    with open(sql_file_path, 'r') as f:
        query = f.read()

    df = azure_postgresql_utils.pull_from_db(query=query)
    logger.info(f"Pulled {len(df)} rows from {sql_filename} ...")

    return df


def _get_latest_trade_file(
    df: pd.DataFrame,
    SFTP_DATE_COL: str = "sftp_date",
) -> datetime:
    """"""
    sftp_date = pd.to_datetime(df[SFTP_DATE_COL].max()).date()
    return sftp_date


def _connect_to_mufg_sftp(
        host: str,
        username: str,
        password: str,
        port: int,
    ) -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    """"""
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def _upload_trades_file(
    sftp: paramiko.SFTPClient,
    remote_dir: str,
    file_name: str,
) -> None:
    """"""
    # upload the trades file to the MUFG SFTP server
    with open(file_name, "rb") as f:
        sftp.putfo(f, f"{remote_dir}/{Path(file_name).name}")
    logger.success(f"Uploaded {file_name} to {remote_dir} ..")


def main(
    sql_filename: str = SQL_FILENAME,
    csv_filename_pattern: str = CSV_FILENAME_PATTERN,
    sftp_dir: Path = settings.MUFG_EOD_TRADES_SFTP_DIR,
    mufg_host: str = secrets.MUFG_SFTP_HOST,
    mufg_username: str = secrets.MUFG_SFTP_USER,
    mufg_password: str = secrets.MUFG_SFTP_PASSWORD,
    mufg_port: int = secrets.MUFG_SFTP_PORT,
    mufg_remote_dir: str = secrets.MUFG_SFTP_REMOTE_DIR,
    validate_date: bool = False,
    expected_date: date | None = None,
):

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source=LOGGING_SOURCE,
        priority=LOGGING_PRIORITY,
        tags=LOGGING_TAGS,
        log_file_path=logger.log_file_path,
        target_table=LOGGING_TARGET_TABLE,
        operation_type=LOGGING_OPERATION_TYPE,
    )

    try:
        logger.header(f"{API_SCRAPE_NAME}")
        run.start()

        logger.section(f"SQL Config ...")
        logger.info(f"SQL Config: {sql_filename} .. {csv_filename_pattern}")

        logger.section(f"Pulling from database ...")
        df: pd.DataFrame = _pull_from_db(
            sql_dir=settings.SQL_DIR,
            sql_filename=sql_filename,
        )

        latest_sftp_date: datetime = _get_latest_trade_file(df=df)
        logger.info(f"Latest SFTP Date: {latest_sftp_date}")
        filename = f"{csv_filename_pattern}_{latest_sftp_date.strftime('%Y%m%d')}_filtered.csv"

        if validate_date:
            _expected = expected_date or date.today()
            if latest_sftp_date != _expected:
                raise ValueError(
                    f"Trade file date mismatch: sftp_date={latest_sftp_date}, expected={_expected}"
                )
            logger.success(f"Date validation passed: {latest_sftp_date}")

        logger.section(f"Saving CSV to local directory ...")
        filtered_trade_file_path = sftp_dir / filename
        df.to_csv(filtered_trade_file_path, index=False)
        logger.success(f"Saved filtered trades file to {filtered_trade_file_path}")

        # connect to MUFG SFTP server
        logger.section(f"Connecting to MUFG SFTP server")
        mufg_sftp, mufg_transport = _connect_to_mufg_sftp(
            host=mufg_host,
            username=mufg_username,
            password=mufg_password,
            port=mufg_port,
        )
        logger.success(f"Connected to MUFG SFTP server ..")

        # # upload the trades file to the MUFG SFTP server
        # logger.section(f"Uploading trades file to MUFG SFTP server")
        # _upload_trades_file(
        #     sftp=mufg_sftp,
        #     remote_dir=mufg_remote_dir,
        #     file_name=filtered_trade_file_path,
        # )
        # logger.success(f"Uploaded trades file to MUFG SFTP server ..")

        run.success()

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e, log_file_path=logger.log_file_path)
        raise

    finally:
        logging_utils.close_logging()

if __name__ == "__main__":
    main()
