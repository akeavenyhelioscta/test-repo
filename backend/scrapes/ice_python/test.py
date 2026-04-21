from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils

from backend.scrapes.ice_python import utils

API_SCRAPE_NAME = "ice_python_smoke_test"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def _pull(
    product: str = "PMI",
    strip: str = "H",
    year: int | None = None,
    suffix: str = "-IUS",
    data_type: str = "Settlement",
    granularity: str = "D",
) -> pd.DataFrame:
    year = year or datetime.today().year
    symbol = f"{product} {strip}{str(year)[-2:]}{suffix}"
    return utils.get_timeseries(
        symbol=symbol,
        data_type=data_type,
        granularity=granularity,
    )


def _format(
    df: pd.DataFrame,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    return utils.format_timeseries(
        df=df,
        date_col=date_col,
        date_format=date_format,
    )


def main() -> pd.DataFrame:
    try:
        logger.header(API_SCRAPE_NAME)
        df = _format(_pull())
        if df.empty:
            logger.warning("No rows returned from smoke test.")
        else:
            logger.info(df.head().to_string(index=False))
        return df
    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()

