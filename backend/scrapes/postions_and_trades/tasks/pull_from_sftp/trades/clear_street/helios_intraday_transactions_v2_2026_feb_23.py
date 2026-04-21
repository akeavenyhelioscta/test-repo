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
API_SCRAPE_NAME = "helios_intraday_transactions_v2_2026_feb_23"

# LOGGING
LOGGING_SOURCE = "positions_and_trades"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "trades,clear_street"
LOGGING_TARGET_TABLE = "clear_street.helios_intraday_transactions_v2_2026_feb_23"
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

# Explicit dtypes for pd.read_csv to prevent pandas from inferring
# inconsistent types across files (e.g., int vs float when NaN present)
CSV_DTYPES = {
    "TRADE_DATE": str,
    "CUSIP": str,
    "TRACER": str,
    "ORDER_NUMBER": str,
    "INSTRUMENT_DESCRIPTION": str,
    "DESCRIPTION": str,
    "CONTRACT_YEAR": str,
    "CONTRACT_MONTH": str,
    "CONTRACT_DAY": str,
    "PUT_CALL": str,
    "STRIKE_PRICE": str,
    "B_S": str,
    "B/S": str,
    "QTY": str,
    "TRADE_PRICE": str,
    "EXCHANGE": str,
    "SYMBOL": str,
    "SUB_ACCOUNT": str,
    "SPREAD_CODE": str,
    "COMMENT_CODE": str,
    "OPP_BROKER": str,
    "OPP_FIRM": str,
    "CURR_SYMBOL": str,
    "EXEC_TIME": str,
    "OPT_EXP_DATE": str,
    "LAST_TRD_DATE": str,
    "SUB_EXCHANGE": str,
    "ISIN": str,
    "TRADE_TYPE": str,
    "GI_CODE": str,
    "GI_FIRM": str,
}

def _convert_to_mst(timestamp) -> datetime:
    """Convert a datetime object to Mountain Time."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=pytz.UTC)

    mst_timezone = pytz.timezone('US/Mountain')
    return timestamp.astimezone(mst_timezone)


def _connect_to_clear_street_sftp(
        host: str,
        port: int,
        username: str,
        private_key_path: str,
    ) -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    """"""
    import io
    import os

    transport = paramiko.Transport((host, port))
    key_content = os.getenv("CLEAR_STREET_SSH_KEY_CONTENT")
    private_key = paramiko.RSAKey.from_private_key(io.StringIO(key_content))
    transport.connect(username=username, pkey=private_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def _pull(
        trade_file_pattern: str,
        lookback_days: int,
        sftp_dir: Path,
        sftp_host: str,
        sftp_port: int,
        sftp_user: str,
        sftp_private_key_path: str,
        sftp_remote_dir: str,
    ) -> None:
    """"""

    try:
        # Establish SFTP connection with SSH key
        logger.section(f"Connecting to Clear Street SFTP server")
        sftp, transport = _connect_to_clear_street_sftp(
            host=sftp_host,
            port=sftp_port,
            username=sftp_user,
            private_key_path=sftp_private_key_path,
        )
        logger.success(f"Connected to SFTP server ..")

        # lookback
        filenames = sorted([file_attr.filename for file_attr in sftp.listdir_attr(sftp_remote_dir) if fnmatch.fnmatchcase(file_attr.filename.upper(), trade_file_pattern.upper())], reverse=True)
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
            new_filename = f"{filename_without_ext}.{timestamp_str}.csv"

            new_filename = new_filename.replace(" ", "_")
            new_filename = re.sub(r'_+', '_', new_filename)  # Replace 1+ underscores with single underscore

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
    pattern = r'Helios_Intraday_Transactions_(\d+)\.(\d+)_(\d+)\.csv'
    match = re.search(pattern, os.path.basename(filepath), re.IGNORECASE)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {os.path.basename(filepath)}")

    trade_date_from_sftp = match.group(1)
    date_part = match.group(2)
    time_part = match.group(3)
    sftp_upload_timestamp_str = f"{date_part}_{time_part}"
    sftp_upload_timestamp = datetime.strptime(sftp_upload_timestamp_str, '%Y%m%d_%H%M%S')

    logger.info(f"\ttrade_date_from_sftp: {trade_date_from_sftp} ...")
    logger.info(f"\tsftp_upload_timestamp: {sftp_upload_timestamp} ...")

    # Read CSV with explicit dtypes to prevent inconsistent type inference
    df: pd.DataFrame = pd.read_csv(
        filepath,
        dtype=CSV_DTYPES,
    )

    # Clean column names
    df.columns = (
        df.columns
        .str.replace(r'\n', '', regex=True, flags=re.IGNORECASE)
        .str.replace(r'_x000a_', '', regex=True, flags=re.IGNORECASE)
        .str.strip()
    )
    logger.debug(f"\tcolumns: {df.columns.tolist()} ...")

    df["row_number_for_trades"] = df.index
    df["trade_date_from_sftp"] = trade_date_from_sftp
    df["sftp_upload_timestamp"] = sftp_upload_timestamp

    return df


def _format(
        df: pd.DataFrame
    ) -> pd.DataFrame:

    # format cols
    df.columns = df.columns.str.lower()

    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('.', '_')

    primary_keys = [
        'trade_date_from_sftp',
        'sftp_upload_timestamp',
        'trade_date',
        'cusip',
        'tracer',
        'order_number',
        'instrument_description',
        'description',
        'contract_year',
        'contract_month',
        'contract_day',
        'put_call',
        'strike_price',
        'b_s',
        'qty',
        'trade_price',
    ]
    pk_present = [col for col in primary_keys if col in df.columns]
    df = df[pk_present + [col for col in df.columns if col not in pk_present]]

    # NOTE: manually set data types
    string_cols: list = [
        'exchange',
        'symbol',
        'put_call',
        'contract_year',
        'contract_month',
        'contract_day',
        'trade_date',
        'sub_account',
        'spread_code',
        'comment_code',
        'opp_broker',
        'opp_firm',
        'curr_symbol',
        'exec_time',
        'opt_exp_date',
        'last_trd_date',
        'sub_exchange',
        'isin',
        'trade_type',
        'gi_code',
        'gi_firm',
    ]
    for col in [c for c in string_cols if c in df.columns]:
        df[col] = df[col].fillna('').astype(str)

    float_cols: list = [
        'strike_price',
        'trade_price',
    ]
    for col in [c for c in float_cols if c in df.columns]:
        df[col] = df[col].fillna(0.0).astype(float)

    integer_cols: list = [
        'row_number_for_trades',
    ]
    for col in [c for c in integer_cols if c in df.columns]:
        df[col] = df[col].fillna(0).astype(int)

    # # data types
    # cols = df.columns.tolist()
    # data_types: list = azure_postgresql_utils.infer_sql_data_types(df=df)
    # for col, dtype in zip(cols, data_types):
    #     logger.info(f"\t{col} .. {dtype}")

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "clear_street",
        table_name: str = API_SCRAPE_NAME,
    ) -> None:

    # primary keys
    PRIMARY_KEYS: list[str] = [
        'trade_date_from_sftp',
        'sftp_upload_timestamp',
        'trade_date',
        'cusip',
        'tracer',
        'order_number',
        'instrument_description',
        'description',
        'contract_year',
        'contract_month',
        'contract_day',
        'put_call',
        'strike_price',
        'b_s',
        'qty',
        'trade_price',
    ]

    azure_postgresql_utils.upsert_to_azure_postgresql(
        schema = schema,
        table_name = table_name,
        df = df,
        columns = df.columns.tolist(),
        primary_key = PRIMARY_KEYS,
    )


def main(
    # SFTP params
    trade_file_pattern: str = f"Helios_Intraday Transactions_*.csv",  # NOTE: Helios_Transactions_20260106.csv
    lookback_days: int = 5,
    sftp_dir: Path = settings.CLEAR_STREET_INTRADAY_TRADES_SFTP_DIR,
    sftp_host: str = secrets.CLEAR_STREET_SFTP_HOST,
    sftp_port: int = secrets.CLEAR_STREET_SFTP_PORT,
    sftp_user: str = secrets.CLEAR_STREET_SFTP_USER,
    sftp_private_key_path: str = secrets.CLEAR_STREET_SSH_KEY_CONTENT,
    sftp_remote_dir: str = secrets.CLEAR_STREET_SFTP_REMOTE_DIR,
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
            sftp_private_key_path=sftp_private_key_path,
            sftp_remote_dir=sftp_remote_dir,
        )

        # Process each file individually: read -> format -> upsert
        filepaths = glob.glob(os.path.join(sftp_dir, "Helios_Intraday_Transactions_*.*.CSV"))
        logger.info(f"Found {len(filepaths)} files ...")
        run.log_files_processed(len(filepaths))

        # NOTE: Filter to only keep the latest file per trade date
        pattern = r'Helios_Intraday_Transactions_(\d+)\.(\d+)_(\d+)\.csv'
        files_by_trade_date = {}
        for filepath in filepaths:
            match = re.search(pattern, os.path.basename(filepath), re.IGNORECASE)
            if match:
                trade_date = match.group(1)
                upload_timestamp_str = f"{match.group(2)}_{match.group(3)}"

                if trade_date not in files_by_trade_date:
                    files_by_trade_date[trade_date] = (filepath, upload_timestamp_str)
                else:
                    if upload_timestamp_str > files_by_trade_date[trade_date][1]:
                        files_by_trade_date[trade_date] = (filepath, upload_timestamp_str)

        filepaths = [fp for fp, _ in files_by_trade_date.values()]
        logger.info(f"Filtered to {len(filepaths)} files (latest per trade date) ...")

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
