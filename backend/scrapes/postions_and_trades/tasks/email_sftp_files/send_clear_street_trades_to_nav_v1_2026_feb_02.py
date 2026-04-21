import os
import glob
import logging
from datetime import datetime, date
from pathlib import Path

from backend.utils import (
    azure_email_utils,
    logging_utils,
    pipeline_run_logger,
)

from backend.scrapes.postions_and_trades import (
    settings,
)

# SCRAPE
API_SCRAPE_NAME = "send_clear_street_trades_to_nav_v1_2026_feb_02"

# LOGGING
LOGGING_SOURCE = "positions_and_trades"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "email,clear_street,nav"
LOGGING_TARGET_TABLE = "trades_v1_2026_feb_02.marts_v1_clear_street_trades"
LOGGING_OPERATION_TYPE = "consume"

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

def main(
    sftp_dir: Path = settings.CLEAR_STREET_EOD_TRADES_SFTP_DIR,
    trade_file_pattern: str = "Helios_Transactions_*.*.csv",  # NOTE: Helios_Transactions_20260106.20260106_191837.csv
    sender_email_address: str = settings.SENDER_EMAIL_ADDRESS,
    recipient_email_addresses: list[str] = settings.RECIPIENT_EMAIL_ADDRESSES,
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

        logger.section(f"Getting trade files from {sftp_dir}")

        # get trade files
        filepaths = glob.glob(os.path.join(sftp_dir, trade_file_pattern))

        # Sort by date extracted from filename and get the latest
        filepaths_sorted = sorted(filepaths, reverse=True)

        # get latest trade file
        latest_trade_file = filepaths_sorted[0] if filepaths_sorted else None

        # get date from trade file
        date_str = Path(latest_trade_file).name.split("_")[2].split(".")[0]  # Extract '20260127' from filename
        date_from_trade_file = datetime.strptime(date_str, "%Y%m%d").strftime("%a %b-%d %Y")
        logger.info(f"Latest trade file ({date_from_trade_file}): {Path(latest_trade_file).name}")

        validation_passed = False
        if validate_date:
            _expected = expected_date or date.today()
            trade_file_date = datetime.strptime(date_str, "%Y%m%d").date()
            if trade_file_date != _expected:
                raise ValueError(
                    f"Trade file date mismatch: file_date={trade_file_date}, expected={_expected}"
                )
            validation_passed = True
            logger.success(f"Date validation passed: {trade_file_date}")

        logger.section(f"Sending email with attachment")
        azure_email_utils.send_outlook_email_with_attachments(
            sender_email_address=sender_email_address,
            recipient_email_addresses=recipient_email_addresses,
            subject=f"Clear Street - Helios Transactions - {date_from_trade_file}",
            attachments=[latest_trade_file],
        )

        run.success(metadata={
            "date_validation_enabled": validate_date,
            "date_validation_passed": validation_passed,
            "trade_file_date": date_str,
            "expected_date": str(expected_date or date.today()),
            "trade_file_name": Path(latest_trade_file).name,
        })

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e, log_file_path=logger.log_file_path)
        raise

    finally:
        logging_utils.close_logging()

if __name__ == "__main__":
    main()
