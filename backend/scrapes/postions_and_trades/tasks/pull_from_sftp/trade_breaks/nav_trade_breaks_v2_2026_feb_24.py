import glob
import fnmatch
import os
import pytz
import re
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import paramiko

from backend.utils import (
    azure_postgresql_utils,
    logging_utils,
    pipeline_run_logger,
    slack_utils,
)

from backend import (
    secrets,
)

from backend.scrapes.postions_and_trades import (
    settings,
)

# SCRAPE
API_SCRAPE_NAME = "nav_trade_breaks_v2_2026_feb_24"

# LOGGING
LOGGING_SOURCE = "positions_and_trades"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "trade_breaks,nav"
LOGGING_TARGET_TABLE = "nav.nav_trade_breaks_v2_2026_feb_24"
LOGGING_OPERATION_TYPE = "upsert"

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

def _convert_to_mst(timestamp) -> datetime:
    """Convert a datetime object to Mountain Time."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=pytz.UTC)

    mst_timezone = pytz.timezone('US/Mountain')
    return timestamp.astimezone(mst_timezone)


def _connect_to_nav_sftp(
        host: str,
        port: int,
        username: str,
        password: str,
    ) -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    """"""
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def _pull(
        trade_file_pattern: str,
        lookback_days: int,
        sftp_dir: Path,
        sftp_host: str,
        sftp_port: int,
        sftp_user: str,
        sftp_password: str,
        sftp_remote_dir: str,
    ) -> None:
    """"""

    try:

        # Establish SFTP connection with SSH key
        logger.section(f"Connecting to NAV SFTP server")
        sftp, transport = _connect_to_nav_sftp(
            host=sftp_host,
            port=sftp_port,
            username=sftp_user,
            password=sftp_password,
        )
        logger.success(f"Connected to SFTP server ..")

        # lookback
        filenames = sorted([file_attr.filename for file_attr in sftp.listdir_attr(sftp_remote_dir) if fnmatch.fnmatchcase(file_attr.filename.upper(), trade_file_pattern.upper())], reverse=True)
        logger.info(f"Found {len(filenames)} files ...")
        for filename in filenames[:lookback_days]:
            logger.info(f"SFTP ... {filename}")

            # Get file attributes including timestamp
            file_attr = sftp.stat(os.path.join(sftp_remote_dir, filename))
            upload_timestamp_utc = datetime.fromtimestamp(file_attr.st_mtime)
            upload_timestamp_mst = _convert_to_mst(upload_timestamp_utc)
            upload_timestamp = upload_timestamp_utc
            logger.info(f"File upload timestamp .. utc: {upload_timestamp_utc}, mst: {upload_timestamp_mst}")

            # Create new filename with timestamp
            filename_without_ext = os.path.splitext(filename)[0]
            timestamp_str = upload_timestamp.strftime('%Y%m%d_%H%M%S')
            new_filename = f"{filename_without_ext}.{timestamp_str}.xlsx"

            sftp.get(os.path.join(sftp_remote_dir, filename), os.path.join(sftp_dir, new_filename))
            logger.info(f"Downloaded {new_filename} to {sftp_dir}")

    except Exception as e:
        logger.error(f"Error pulling {filename} ... {e}")
        raise Exception(f"Error pulling {filename} ... {e}")

    finally:
        # Clean up
        sftp.close()
        transport.close()
        logger.info(f"Closed SFTP connection ..")


def _read_file(
        filepath: str,
    ) -> pd.DataFrame:
    """"""

    logger.info(f"\t{filepath} ...")

    # Extract the date and timestamp
    # NOTE: Trade Breaks Detail Report_20260202_HELIOS COMMODITY ADVISORS LTD.20260203_123456.xlsx
    pattern = r'Trade Breaks Detail Report_(\d+)_HELIOS COMMODITY ADVISORS LTD\.(\d+)_(\d+)\.xlsx'
    match = re.search(pattern, os.path.basename(filepath), re.IGNORECASE)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {os.path.basename(filepath)}")

    nav_date_from_sftp = match.group(1)
    date_part = match.group(2)
    time_part = match.group(3)
    sftp_upload_timestamp_str = f"{date_part}_{time_part}"
    sftp_upload_timestamp = datetime.strptime(sftp_upload_timestamp_str, '%Y%m%d_%H%M%S')

    logger.info(f"\tnav_date_from_sftp: {nav_date_from_sftp} ...")
    logger.info(f"\tsftp_upload_timestamp: {sftp_upload_timestamp} ...")

    # Read Excel file (skiprows=2: row 0 is "Client Name", row 1 is report title/date)
    df: pd.DataFrame = pd.read_excel(
        filepath,
        sheet_name="Trade Breaks",
        skiprows=2,
        engine='openpyxl',
    )

    # Clean column names
    df.columns = (
        df.columns
        .str.replace(r'\n', '', regex=True, flags=re.IGNORECASE)
        .str.replace(r'_x000a_', '', regex=True, flags=re.IGNORECASE)
        .str.strip()
    )

    # Drop fully empty rows
    df.dropna(how='all', inplace=True)

    # Drop metadata/junk rows
    df = df[~df.apply(lambda row: any(isinstance(cell, str) and "No Trade Break found in Reconciliation" in cell for cell in row), axis=1)]
    df = df[~df.apply(lambda row: any(isinstance(cell, str) and "Color Scheme & Notation reference" in cell for cell in row), axis=1)]
    df = df[~df.apply(lambda row: any(isinstance(cell, str) and "Current Day Trade Breaks" in cell for cell in row), axis=1)]
    df = df[~df.apply(lambda row: any(isinstance(cell, str) and "Previous Day Trade Breaks" in cell for cell in row), axis=1)]

    df["nav_date_from_sftp"] = nav_date_from_sftp
    df["sftp_upload_timestamp"] = sftp_upload_timestamp

    df = df.reset_index(drop=True)

    return df


def _format(
        df: pd.DataFrame
    ) -> pd.DataFrame:

    # format cols
    df.columns = df.columns.str.lower()

    # further formatting
    df.columns = df.columns.str.replace('#', '')
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("(", "")
    df.columns = df.columns.str.replace(")", "")
    df.columns = df.columns.str.replace(".", "")
    df.columns = df.columns.str.replace("/", "_")
    df.columns = df.columns.str.replace("\n", "")
    df.columns = df.columns.str.replace('__', '_')

    primary_keys = [
        'nav_date_from_sftp',
        'sftp_upload_timestamp',
        'broker',
        'account_group',
        'account',
        'commodity',
        'month_year',
        'call_put',
        'strike_price',
        'p_s',
        'quantity',
        'trade_price',
        'trade_date',
        'source',
        'add_del',
    ]
    pk_present = [col for col in primary_keys if col in df.columns]
    df = df[pk_present + [col for col in df.columns if col not in pk_present]]

    # floats
    float_cols = ['strike_price', 'quantity', 'trade_price', 'original_price']
    for col in [c for c in float_cols if c in df.columns]:
        df[col] = df[col].fillna(0.0).astype(float)

    # dates — convert to string to prevent fillna(0) from producing invalid date values
    date_cols = ["trade_date"]
    for col in [c for c in date_cols if c in df.columns]:
        df[col] = df[col].fillna('').astype(str)

    # strings
    for col in [c for c in df.columns if c not in date_cols and c not in float_cols]:
        df[col] = df[col].fillna('').astype(str)

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "nav",
        table_name: str = API_SCRAPE_NAME,
    ) -> None:

    primary_keys = [
        'nav_date_from_sftp',
        'sftp_upload_timestamp',
        'broker',
        'account_group',
        'account',
        'commodity',
        'month_year',
        'call_put',
        'strike_price',
        'p_s',
        'quantity',
        'trade_price',
        'trade_date',
        'source',
        'add_del',
    ]

    azure_postgresql_utils.upsert_to_azure_postgresql(
        schema = schema,
        table_name = table_name,
        df = df,
        columns = df.columns.tolist(),
        primary_key = primary_keys,
    )


def main(
    # SFTP params
    trade_file_pattern: str = f"Trade Breaks Detail Report_*_HELIOS COMMODITY ADVISORS LTD.XLSX",  # NOTE: Trade Breaks Detail Report_20260202_HELIOS COMMODITY ADVISORS LTD.XLSX
    lookback_days: int = 5,
    sftp_dir: Path = settings.NAV_TRADE_BREAKS_SFTP_DIR,
    sftp_host: str = secrets.NAV_SFTP_HOST,
    sftp_port: int = secrets.NAV_SFTP_PORT,
    sftp_user: str = secrets.NAV_SFTP_USER,
    sftp_password: str = secrets.NAV_SFTP_PASSWORD,
    sftp_remote_dir: str = secrets.NAV_SFTP_REMOTE_DIR,
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

        logger.section(f"Pulling trade files ...")
        _pull(
            trade_file_pattern=trade_file_pattern,
            lookback_days=lookback_days,
            sftp_dir=sftp_dir,
            sftp_host=sftp_host,
            sftp_port=sftp_port,
            sftp_user=sftp_user,
            sftp_password=sftp_password,
            sftp_remote_dir=sftp_remote_dir,
        )

        # Process each file individually: read -> format -> upsert
        filepaths = glob.glob(os.path.join(sftp_dir, "Trade Breaks Detail Report_*_HELIOS COMMODITY ADVISORS LTD.*.xlsx"))
        logger.info(f"Found {len(filepaths)} files ...")
        run.log_files_processed(len(filepaths))

        for filepath in sorted(filepaths, reverse=True)[:lookback_days]:
            logger.section(f"Processing {os.path.basename(filepath)} ...")

            df = _read_file(filepath=filepath)
            if df.empty:
                logger.info(f"Skipping {os.path.basename(filepath)} (0 rows)")
                continue
            df = _format(df=df)
            _upsert(df=df)
            run.log_rows_processed(len(df))

            logger.success(f"Upserted {os.path.basename(filepath)}")

        run.success()

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e, log_file_path=logger.log_file_path)
        raise

    finally:
        logging_utils.close_logging()

if __name__ == "__main__":
    main()
