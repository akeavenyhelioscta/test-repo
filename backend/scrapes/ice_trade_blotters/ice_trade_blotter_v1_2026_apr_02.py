"""
ICE Exchange Activity Report — manual CSV ingest pipeline.

Workflow:
    1. Download DealReport CSV from ICE Exchange Activity Report tool
    2. Drop the file into ``backend/scrapes/ice_trade_blotters/inbox/``
    3. Run this script — it parses, upserts to PostgreSQL, and moves
       the file to ``inbox/processed/``

The DealReport CSV contains multiple sections (Bilateral Deals, Cleared
Deals, Futures Deals) each with its own column header row.  This script
parses all sections, tags each row with a ``deal_type``, and upserts
everything into a single table.
"""

from __future__ import annotations

import glob
import io
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# ── Config ──────────────────────────────────────────────────────────────

API_SCRAPE_NAME = "ice_trade_blotter_v1_2026_apr_02"
TABLE_NAME = API_SCRAPE_NAME

LOGGING_SOURCE = "ice_trade_blotters"
LOGGING_PRIORITY = "high"
LOGGING_TAGS = "trades,ice,trade_blotter"

DATABASE = "helioscta"
SCHEMA = "ice_trade_blotters"

INBOX_DIR = Path(__file__).resolve().parent / "inbox"
PROCESSED_DIR = INBOX_DIR / "processed"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
    level=logging.INFO,
)

# Known section names for detection.
SECTION_NAMES = {"Bilateral Deals", "Cleared Deals", "Futures Deals"}

# ── Primary key ─────────────────────────────────────────────────────────

PRIMARY_KEY: list[str] = [
    "file_name",
    "deal_type",
    "trade_date",
    "trade_time_micros",
    "deal_id",
    "orig_id",
    "b_s",
    "product",
    "hub",
    "contract",
    "price",
    "lots",
]

# Numeric columns to cast from string.
NUMERIC_COLS: list[str] = [
    "price",
    "strike",
    "strike2",
    "strike_2",
    "lots",
    "total_quantity",
    "qty_per_period",
    "periods",
]


# ── CSV parsing ─────────────────────────────────────────────────────────

def _pull() -> list[str]:
    """Return paths to all CSV files sitting in the inbox folder."""
    patterns = [
        os.path.join(INBOX_DIR, "*.csv"),
        os.path.join(INBOX_DIR, "*.CSV"),
    ]
    filepaths = []
    for pattern in patterns:
        filepaths.extend(glob.glob(pattern))
    filepaths = sorted(set(filepaths))
    logger.info(f"Found {len(filepaths)} CSV file(s) in inbox")
    return filepaths


def _parse_deal_report(filepath: str) -> dict[str, pd.DataFrame]:
    """Parse a multi-section DealReport CSV into per-section DataFrames.

    Returns a dict mapping section name -> DataFrame (only sections with
    at least one data row are included).
    """
    with open(filepath, "r", encoding="utf-8-sig") as f:
        raw_lines = f.readlines()

    # Identify section boundaries.
    section_starts: list[tuple[int, str]] = []
    for i, line in enumerate(raw_lines):
        first_cell = line.split(",")[0].strip().strip('"')
        if first_cell in SECTION_NAMES:
            section_starts.append((i, first_cell))

    if not section_starts:
        logger.warning(f"No recognised sections in {filepath}")
        return {}

    results: dict[str, pd.DataFrame] = {}

    for idx, (start_line, section_name) in enumerate(section_starts):
        if idx + 1 < len(section_starts):
            end_line = section_starts[idx + 1][0]
        else:
            end_line = len(raw_lines)

        section_lines = raw_lines[start_line:end_line]

        # Find the column header row (contains "Trade Date").
        header_offset = None
        for j, sline in enumerate(section_lines):
            if j == 0:
                continue
            stripped = sline.strip().strip(",").strip()
            if not stripped:
                continue
            if "Trade Date" in sline:
                header_offset = j
                break

        if header_offset is None:
            logger.info(f"Section '{section_name}': no header row found, skipping")
            continue

        data_lines = section_lines[header_offset:]
        while data_lines and not data_lines[-1].strip().strip(",").strip():
            data_lines.pop()

        if len(data_lines) <= 1:
            logger.info(f"Section '{section_name}': header only, 0 data rows")
            continue

        csv_block = "".join(data_lines)
        df = pd.read_csv(
            io.StringIO(csv_block),
            dtype=str,
            keep_default_na=False,
            skipinitialspace=True,
        )

        # Drop empty trailing columns and fully-empty rows.
        df = df.loc[:, ~(df.columns.str.startswith("Unnamed") | (df.columns == ""))]
        df = df[df.apply(lambda row: any(cell.strip() for cell in row), axis=1)]

        if df.empty:
            logger.info(f"Section '{section_name}': all rows empty after cleaning")
            continue

        logger.info(f"Section '{section_name}': {len(df)} data rows, {len(df.columns)} columns")
        results[section_name] = df

    return results


# ── Formatting ──────────────────────────────────────────────────────────

def _clean_column_name(col: str) -> str:
    """Convert a raw CSV header to a snake_case DB column name."""
    col = col.strip()
    col = re.sub(r"[/\-.()\s]+", "_", col)
    col = re.sub(r"_+", "_", col)
    col = col.strip("_").lower()
    return col


def _format_section(
    df: pd.DataFrame,
    file_name: str,
    section_name: str,
) -> pd.DataFrame:
    """Clean columns, add metadata, and normalise section-specific fields."""

    df = df.copy()
    df.columns = [_clean_column_name(c) for c in df.columns]

    # Tag with deal type and file name.
    df.insert(0, "file_name", file_name)
    df.insert(1, "deal_type", section_name)

    # ── Normalise section-specific columns ──────────────────────────
    # Bilateral uses "strip" instead of "contract" — copy into contract.
    if "strip" in df.columns and "contract" not in df.columns:
        df["contract"] = df["strip"]

    # Bilateral uses "counterparty" instead of "clearing_firm".
    if "counterparty" in df.columns and "clearing_firm" not in df.columns:
        df["clearing_firm"] = df["counterparty"]

    # Bilateral / Futures may lack trade_time_micros — fall back to trade_time.
    if "trade_time_micros" not in df.columns and "trade_time" in df.columns:
        df["trade_time_micros"] = df["trade_time"]

    # Bilateral uses qty_per_period instead of lots.
    if "lots" not in df.columns and "qty_per_period" in df.columns:
        df["lots"] = df["qty_per_period"]

    # ── Coerce numeric columns ──────────────────────────────────────
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Fill PK nulls so upsert constraint doesn't fail ─────────────
    for pk_col in PRIMARY_KEY:
        if pk_col in df.columns:
            if pk_col in NUMERIC_COLS:
                df[pk_col] = df[pk_col].fillna(0)
            else:
                df[pk_col] = df[pk_col].fillna("").astype(str)

    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    return df


def _format(
    sections: dict[str, pd.DataFrame],
    file_name: str,
) -> pd.DataFrame:
    """Format all sections and combine into a single DataFrame."""
    frames: list[pd.DataFrame] = []

    for section_name, df in sections.items():
        formatted = _format_section(df, file_name=file_name, section_name=section_name)
        frames.append(formatted)
        logger.info(f"  {section_name}: {len(formatted)} rows formatted")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    return combined


# ── Upsert ──────────────────────────────────────────────────────────────

def _upsert(df: pd.DataFrame) -> None:
    """Upsert the combined DataFrame to Azure PostgreSQL."""

    pk = [col for col in PRIMARY_KEY if col in df.columns]
    if not pk:
        logger.warning("No PK columns found, skipping upsert")
        return

    # ICE exports sometimes contain fully-duplicate rows — dedup on PK.
    before = len(df)
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    if before != len(df):
        logger.info(f"  Deduped {before} -> {len(df)} rows")

    azure_postgresql.upsert_to_azure_postgresql(
        database=DATABASE,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        df=df,
        columns=df.columns.tolist(),
        primary_key=pk,
    )


# ── Main ────────────────────────────────────────────────────────────────

def main():
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source=LOGGING_SOURCE,
        priority=LOGGING_PRIORITY,
        tags=LOGGING_TAGS,
        log_file_path=logger.log_file_path,
        target_table=f"{SCHEMA}.{TABLE_NAME}",
        operation_type="upsert",
    )

    try:
        logger.header(API_SCRAPE_NAME)
        run.start()

        filepaths = _pull()
        if not filepaths:
            logger.info("No CSV files in inbox — nothing to do")
            run.success(rows_processed=0)
            return

        total_rows = 0

        for filepath in filepaths:
            file_name = os.path.basename(filepath)
            logger.section(f"Processing {file_name}")

            sections = _parse_deal_report(filepath)
            if not sections:
                logger.warning(f"No data sections found in {file_name}")
                continue

            df = _format(sections, file_name=file_name)
            if df.empty:
                logger.warning(f"No rows after formatting {file_name}")
                continue

            logger.info(f"  Upserting {len(df)} rows to {SCHEMA}.{TABLE_NAME} ...")
            _upsert(df)
            total_rows += len(df)
            logger.success(f"  {file_name}: {len(df)} rows upserted")

            # Move processed file.
            dest = PROCESSED_DIR / file_name
            shutil.move(filepath, str(dest))
            logger.info(f"  Moved to {dest}")

        run.success(rows_processed=total_rows)
        logger.success(f"Done — {total_rows} total rows upserted")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e, log_file_path=logger.log_file_path)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
