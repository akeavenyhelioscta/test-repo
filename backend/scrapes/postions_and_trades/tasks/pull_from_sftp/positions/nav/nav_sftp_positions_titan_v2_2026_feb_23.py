import glob
import fnmatch
import os
import logging
import pytz
import re
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
API_SCRAPE_NAME = "nav_sftp_positions_titan_v2_2026_feb_23"

# LOGGING
LOGGING_SOURCE = "positions_and_trades"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "positions,nav"
LOGGING_TARGET_TABLE = "nav.nav_sftp_positions_titan_v2_2026_feb_23"
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
        sftp_pass: str,
        sftp_remote_dir: str,
    ) -> None:

    try:
        # Establish SFTP connection with SSH key
        logger.section(f"Connecting to NAV SFTP server")
        sftp, transport = _connect_to_nav_sftp(
            host=sftp_host,
            port=sftp_port,
            username=sftp_user,
            password=sftp_pass,
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
    pattern = r'Position Valuation Detail Report_(\d+)_ESKER POINT LP\.(\d+)_(\d+)\.xlsx'
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

    # Read Excel file
    df: pd.DataFrame = pd.read_excel(
        filepath,
        skiprows=3,
        engine='openpyxl',
    )

    # Clean column names
    df.columns = (
        df.columns
        .str.replace(r'\n', '', regex=True, flags=re.IGNORECASE)
        .str.replace(r'_x000a_', '', regex=True, flags=re.IGNORECASE)
        .str.strip()
    )

    df["nav_date_from_sftp"] = nav_date_from_sftp
    df["sftp_upload_timestamp"] = sftp_upload_timestamp

    # Drop rows containing "Total"
    df = df[~df.apply(lambda row: any(isinstance(cell, str) and "Total" in cell for cell in row), axis=1)]

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
    df.columns = df.columns.str.replace("/", "")
    df.columns = df.columns.str.replace("\n", "")
    df.columns = df.columns.str.replace("_$", "", regex=False)
    df.columns = df.columns.str.replace(r"_\$", "")
    df.columns = df.columns.str.replace("ccy2", "ccy2_")
    df.columns = df.columns.str.replace('__', '_')

    # dates
    date_cols = ["trade_date", "opt_exp_date"]

    # floats
    float_cols = ['strike_price', 'quantity_1', 'cost_in_native_currency', 'open_exchange_rate', 'cost_in_base_currency', 'market_settlement_price', 'market_value_in_native_currency', 'close_exchangerate', 'market_value_in_base_currency']
    for col in [c for c in float_cols if c in df.columns]:
        df[col] = df[col].fillna(0.0).astype(float)

    # strings
    string_cols: list[str] = [col for col in df.columns if col not in date_cols and col not in float_cols]
    for col in [c for c in df.columns if c not in date_cols and c not in float_cols]:
        df[col] = df[col].fillna('').astype(str)

    primary_keys = [
        'nav_date_from_sftp',
        'sftp_upload_timestamp',
        'trade_date',
        'broker_name',
        'account_group',
        'account',
        'product',
        'month_year',
        'strike_price',
        'call_put',
        'long_short',
        'quantity_1',
        'trade_price',
        'source_3_symbol',
    ]
    pk_present = [col for col in primary_keys if col in df.columns]
    df = df[pk_present + [col for col in df.columns if col not in pk_present]]

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "nav",
        table_name: str = API_SCRAPE_NAME,
    ) -> None:

    primary_keys = [
        'nav_date_from_sftp',
        'sftp_upload_timestamp',
        'trade_date',
        'broker_name',
        'account_group',
        'account',
        'product',
        'month_year',
        'strike_price',
        'call_put',
        'long_short',
        'quantity_1',
        'trade_price',
        'source_3_symbol',
    ]

    azure_postgresql_utils.upsert_to_azure_postgresql(
        schema = schema,
        table_name = table_name,
        df = df,
        columns = df.columns.tolist(),
        primary_key = primary_keys,
    )


def main(
        trade_file_pattern: str = f"Position Valuation Detail Report_*_Esker Point LP.XLSX",
        lookback_days: int = 5,
        sftp_dir: Path = settings.NAV_POSITIONS_TITAN_SFTP_DIR,
        sftp_host: str = secrets.NAV_SFTP_HOST,
        sftp_port: int = secrets.NAV_SFTP_PORT,
        sftp_user: str = secrets.NAV_SFTP_USER,
        sftp_pass: str = secrets.NAV_SFTP_PASSWORD,
        sftp_remote_dir: str = secrets.NAV_SFTP_REMOTE_DIR,
    ) -> None:

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

        # Pull from sftp
        logger.section(f"Pulling from sftp")
        _pull(
            trade_file_pattern=trade_file_pattern,
            lookback_days=lookback_days,
            sftp_dir=sftp_dir,
            sftp_host=sftp_host,
            sftp_port=sftp_port,
            sftp_user=sftp_user,
            sftp_pass=sftp_pass,
            sftp_remote_dir=sftp_remote_dir,
        )

        # Process each file individually: read -> format -> upsert
        filepaths = glob.glob(os.path.join(sftp_dir, "Position Valuation Detail Report_*_ESKER POINT LP.*.xlsx"))
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
