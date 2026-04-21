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
API_SCRAPE_NAME = "helios_transactions_v2_2026_feb_23"

# LOGGING
LOGGING_SOURCE = "positions_and_trades"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "trades,clear_street"
LOGGING_TARGET_TABLE = "clear_street.helios_transactions_v2_2026_feb_23"
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
    # identifiers / codes
    "RECORD_ID": str,
    "FIRM": str,
    "ORGANIZATION": str,
    "ACCOUNT_NUMBER": str,
    "ACCOUNT_TYPE": str,
    "CURRENCY_SYMBOL": str,
    "RR": str,
    "TRADE_DATE": str,
    "DATE": str,
    "EXCHANGE": str,
    "SUB_EXCHANGE": str,
    "EXCHANGE_NAME": str,
    "EXCH_COMM_CD": str,
    "FUTURES_CODE": str,
    "SYMBOL": str,
    "SECURITY_DESCRIPTION": str,
    "SECURITY_TYPE_CODE": str,
    "INSTRUMENT_DESCRIPTION": str,
    "INSTR_TYPE": str,
    "CASH_SETTLED": str,
    "CUSIP": str,
    "ISIN": str,
    "MIC": str,
    # trade identifiers
    "ORDER_NUMBER": str,
    "TRACE_NUM_OR_UNIQUE_IDENTIFIER": str,
    "TRADE_TYPE": str,
    "OPEN_CLOSE_CODE": str,
    "BUY_SELL": str,
    "PUT_CALL": str,
    "PRINTABLE_PRICE": str,
    # broker / give-in-out
    "COMMENT_CODE": str,
    "GIVE_IN_OUT_CODE": str,
    "GIVE_IN_OUT_FIRM_NUM": str,
    "SPREAD_CODE": str,
    "ROUND_TURN_HALF_TURN_ACCOUNT": str,
    "EXECUTING_BROKER": str,
    "OPPOSING_BROKER": str,
    "OPPOS_FIRM": str,
    "BROKER": str,
    "TRADED_EXCHG": str,
    # fee type codes
    "COMM_ACT_TYPE": str,
    "FEE_1_ATYPE": str,
    "FEE_2_ATYPE": str,
    "FEE_3_ATYPE": str,
    "BRKRAGE_ATYPE": str,
    "GIVE_IO_ATYPE": str,
    "OTHER_ATYPE": str,
    "WIRE_CHG_ATYPE": str,
    "FEE_TYPE_6_ATYPE": str,
    "FEE_4_ATYPE": str,
    "FEE_5_ATYPE": str,
    "FEE_7_ATYPE": str,
    "FEE_8_ATYPE": str,
    "FEE_9_ATYPE": str,
    "FEE_10_ATYPE": str,
    "FEE_11_ATYPE": str,
    "FEE_12_ATYPE": str,
    "FEE_13_ATYPE": str,
    # dates / times
    "OPTION_EXP_DATE": str,
    "LAST_TRD_DATE": str,
    "CLEARING_TIME_HHMMSS": str,
    # amounts kept as str to avoid float precision issues
    "NET_AMOUNT": str,
    "MULTIPLICATION_FACTOR": str,
    "SUBACCOUNT": str,
}

def _convert_to_mst(timestamp) -> datetime:
    """Convert a datetime object to Mountain Time."""
    # If timestamp is naive (no timezone info), assume it's in UTC
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=pytz.UTC)

    # Convert to Mountain Time
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

            sftp.get(os.path.join(sftp_remote_dir, filename), os.path.join(sftp_dir, new_filename))
            logger.info(f"Downloaded {new_filename} to {sftp_dir}")

    except Exception as e:
        _fname = filename if 'filename' in dir() else "unknown"
        logger.error(f"Error pulling {_fname} ... {e}")
        raise

    finally:
        # Clean up
        if 'sftp' in dir():
            sftp.close()
        if 'transport' in dir():
            transport.close()
        logger.info(f"Closed SFTP connection ..")


def _read_file(
        filepath: str,
    ) -> pd.DataFrame:
    """"""

    logger.info(f"\t{filepath} ...")

    # Extract the date and timestamp
    pattern = r'Helios_Transactions_(\d+).(\d+)_(\d+)\.csv'
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

    primary_keys = [
        'trade_date_from_sftp',              # VARCHAR
        'sftp_upload_timestamp',             # TIMESTAMP
        'trade_date',                        # INTEGER
        'cusip',                             # VARCHAR
        'trace_num_or_unique_identifier',    # VARCHAR
        'order_number',                      # VARCHAR
        'instrument_description',            # VARCHAR
        'security_description',              # VARCHAR
        'contract_year_month',               # INTEGER
        'prompt_day',                        # FLOAT
        'put_call',                          # FLOAT
        'strike_price',                      # INTEGER
        'buy_sell',                          # INTEGER
        'quantity',                          # INTEGER
        'trade_price',                       # FLOAT
    ]
    pk_present = [col for col in primary_keys if col in df.columns]
    df = df[pk_present + [col for col in df.columns if col not in pk_present]]

    # NOTE: manually set data types
    string_cols: list = [
        # identifiers / codes
        'record_id',
        'firm',
        'organization',
        'account_number',
        'account_type',
        'currency_symbol',
        'rr',
        'trade_date',
        'date',
        'exchange',
        'sub_exchange',
        'exchange_name',
        'exch_comm_cd',
        'futures_code',
        'symbol',
        'security_description',
        'security_type_code',
        'instrument_description',
        'instr_type',
        'cash_settled',
        'cusip',
        'isin',
        'mic',
        # trade identifiers
        'order_number',
        'trace_num_or_unique_identifier',
        'trade_type',
        'open_close_code',
        'buy_sell',
        'put_call',
        'printable_price',
        # broker / give-in-out
        'comment_code',
        'give_in_out_code',
        'give_in_out_firm_num',
        'spread_code',
        'round_turn_half_turn_account',
        'executing_broker',
        'opposing_broker',
        'oppos_firm',
        'broker',
        'traded_exchg',
        # fee type codes
        'comm_act_type',
        'fee_1_atype',
        'fee_2_atype',
        'fee_3_atype',
        'brkrage_atype',
        'give_io_atype',
        'other_atype',
        'wire_chg_atype',
        'fee_type_6_atype',
        'fee_4_atype',
        'fee_5_atype',
        'fee_7_atype',
        'fee_8_atype',
        'fee_9_atype',
        'fee_10_atype',
        'fee_11_atype',
        'fee_12_atype',
        'fee_13_atype',
        # dates / times
        'option_exp_date',
        'last_trd_date',
        'clearing_time_hhmmss',
        # amounts kept as str
        'net_amount',
        'multiplication_factor',
        'subaccount',
    ]
    for col in [c for c in string_cols if c in df.columns]:
        df[col] = df[col].fillna('').astype(str)

    float_cols: list = [
        'strike_price',
        'trade_price',
        'commission',
        'brokerage',
        'give_io_charge',
        'other_charges',
        'wire_charge',
        'fee_type_6',
        'fee_amt_1',
        'fee_amt_2',
        'fee_amt_3',
        'fee_amt_4',
        'fee_amt_5',
        'fee_amt_7',
        'fee_amt_8',
        'fee_amt_9',
        'fee_amt_10',
        'fee_amt_11',
        'fee_amt_12',
        'fee_amt_13',
        'settlement_price',
    ]
    for col in [c for c in float_cols if c in df.columns]:
        df[col] = df[col].fillna(0.0).astype(float)

    integer_cols: list = [
        'contract_year_month',
        'prompt_day',
        'quantity',
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
        'trade_date_from_sftp',              # VARCHAR
        'sftp_upload_timestamp',             # TIMESTAMP
        'trade_date',                        # INTEGER
        'account_number',                    #
        'cusip',                             # VARCHAR
        'trace_num_or_unique_identifier',    # VARCHAR
        'order_number',                      # VARCHAR
        'instrument_description',            # VARCHAR
        'security_description',              # VARCHAR
        'contract_year_month',               # INTEGER
        'prompt_day',                        # FLOAT
        'put_call',                          # FLOAT
        'strike_price',                      # INTEGER
        'buy_sell',                          # INTEGER
        'quantity',                          # INTEGER
        'trade_price',                       # FLOAT
        'row_number_for_trades',             # INTEGER
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
    trade_file_pattern: str = f"Helios_Transactions_*.csv",  # NOTE: Helios_Transactions_20260106.csv
    lookback_days: int = 5,
    sftp_dir: Path = settings.CLEAR_STREET_EOD_TRADES_SFTP_DIR,
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
        filepaths = glob.glob(os.path.join(sftp_dir, "Helios_Transactions_*.*.csv"))
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
