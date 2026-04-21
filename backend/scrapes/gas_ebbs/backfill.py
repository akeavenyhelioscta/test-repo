"""
Backfill historical outage data from gas EBB sources.

Two modes of operation:

1. **scrape** (default): Fetch listing pages, filter to date range, upsert,
   and enrich with detail pages. Uses the same scraper infrastructure as the
   live runs but with a higher detail_fetch_limit.

2. **enrich-existing**: Query the database for notices already in the target
   date range that are missing detail enrichment, then fetch details and
   build outage records. This is the fastest path to filling the 2025 gap
   since ~1,460 notices from 2025 are already in the DB but only ~55 have
   detail enrichment.

Usage:
    python backfill.py                                       # scrape all families, all of 2025
    python backfill.py --source williams                     # single family
    python backfill.py --source williams --pipeline transco  # single pipeline
    python backfill.py --start 2025-01-01 --end 2025-12-31  # custom range
    python backfill.py --dry-run                             # parse only, no upsert
    python backfill.py --detail-limit 500                    # override detail fetch cap
    python backfill.py --enrich-existing                     # enrich DB notices missing details
    python backfill.py --enrich-existing --source williams   # enrich specific family
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend import secrets  # noqa: F401
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.gas_ebbs.base_scraper import (
    SCHEMA,
    TABLE,
    COLUMNS,
    DATA_TYPES,
    PRIMARY_KEY,
    SNAPSHOT_TABLE,
    SNAPSHOT_COLUMNS,
    SNAPSHOT_DATA_TYPES,
    SNAPSHOT_PRIMARY_KEY,
    DETAIL_TABLE,
    DETAIL_COLUMNS,
    DETAIL_DATA_TYPES,
    DETAIL_PRIMARY_KEY,
    OUTAGE_TABLE,
    OUTAGE_COLUMNS,
    OUTAGE_DATA_TYPES,
    OUTAGE_PRIMARY_KEY,
    ACTIONABLE_CATEGORIES,
    LOG_DIR,
    create_scraper,
    discover_all_pipelines,
)
from backend.scrapes.gas_ebbs import outage_extractor

log = logging.getLogger("gas_ebbs.backfill")


# ── Date filtering ────────────────────────────────────────────────────────

POSTED_DT_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%m/%d/%Y %I:%M:%S%p",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S %Z",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%b %d, %Y %I:%M:%S %p",
    "%b %d, %Y %I:%M %p",
    "%b %d, %Y",
    "%b %d %Y %I:%M:%S %p",
    "%b %d %Y %I:%M %p",
    "%b %d %Y",
]


def _parse_posted_date(dt_str: str) -> datetime | None:
    """Best-effort parse of posted_datetime strings into a datetime."""
    if not dt_str or not dt_str.strip():
        return None

    cleaned = dt_str.strip()
    for tz_abbr in (" CST", " EST", " CDT", " EDT", " MST", " MDT", " PST", " PDT"):
        if cleaned.endswith(tz_abbr):
            cleaned = cleaned[: -len(tz_abbr)]
            break

    for fmt in POSTED_DT_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def _filter_by_date_range(
    raw_notices: list[dict],
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Filter notices whose posted_datetime falls within [start, end].

    Notices with unparseable dates are kept (err on the side of inclusion).
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59
    )

    filtered = []
    for notice in raw_notices:
        posted = _parse_posted_date(notice.get("posted_datetime", ""))
        if posted is None:
            filtered.append(notice)
        elif start <= posted <= end:
            filtered.append(notice)
    return filtered


# ── Mode 1: Scrape listing pages ────────────────────────────────────────

def backfill_scrape(
    source_family: str,
    pipeline_name: str,
    start_date: str,
    end_date: str,
    dry_run: bool = False,
    detail_limit: int = 500,
) -> dict:
    """Scrape listing pages, filter to date range, upsert + enrich."""
    result = {
        "pipeline_name": pipeline_name,
        "source_family": source_family,
        "success": False,
        "notices_parsed": 0,
        "notices_filtered": 0,
        "notices_upserted": 0,
        "details_fetched": 0,
        "outages_built": 0,
        "error": "",
    }

    api_scrape_name = f"gas_ebb_backfill_{pipeline_name}"
    logger = logging_utils.PipelineLogger(
        name=api_scrape_name,
        log_dir=LOG_DIR,
        log_to_file=True,
        delete_if_no_errors=True,
    )

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=api_scrape_name,
        source="gas_ebbs_backfill",
        target_table=f"{SCHEMA}.{TABLE}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"BACKFILL {api_scrape_name} [{start_date} .. {end_date}]")

        scraper = create_scraper(source_family, pipeline_name)
        scraper.config["detail_fetch_limit"] = detail_limit

        # ── Phase 1: Pull listing pages ──
        listing_sources = scraper._get_listing_sources()

        raw_notices = []
        for source in listing_sources:
            url = source["url"]
            context = {k: v for k, v in source.items() if k != "url"}
            label = context.get("label", url)

            logger.section(f"Pulling: {label}")
            logger.info(f"URL: {url}")

            try:
                html = scraper._pull(url)
                logger.info(f"Fetched {len(html):,} bytes")
            except Exception as e:
                logger.warning(f"Failed to fetch {label}: {e}")
                continue

            notices = scraper._parse_listing(html, **context)
            logger.info(f"Parsed {len(notices)} notices from page")
            raw_notices.extend(notices)

        result["notices_parsed"] = len(raw_notices)

        if not raw_notices:
            logger.warning("No notices found on any listing page.")
            run.success(rows_processed=0)
            result["success"] = True
            return result

        # ── Phase 2: Filter to date range ──
        logger.section(f"Filtering to {start_date} .. {end_date}")
        filtered = _filter_by_date_range(raw_notices, start_date, end_date)
        logger.info(
            f"Kept {len(filtered)} of {len(raw_notices)} notices "
            f"within date range"
        )
        result["notices_filtered"] = len(filtered)

        if not filtered:
            logger.info("No notices in target date range.")
            run.success(rows_processed=0)
            result["success"] = True
            return result

        # ── Phase 3: Format + classify ──
        logger.section("Formatting and Classifying")
        df = scraper._format(filtered)
        logger.info(f"Formatted {len(df)} notices")

        if not df.empty:
            counts = df["notice_category"].value_counts().to_dict()
            for cat, count in counts.items():
                logger.info(f"  {cat}: {count}")

        # ── Phase 4: Upsert ──
        if dry_run:
            logger.info("[DRY RUN] Skipping upsert")
        else:
            logger.section("Upserting to Database")
            scraper._upsert(df)
            logger.success(f"Upserted {len(df)} notices to {SCHEMA}.{TABLE}")
        result["notices_upserted"] = len(df)

        # ── Phase 5: Detail enrichment ──
        try:
            logger.section("Detail Enrichment")
            detail_df = scraper._fetch_details(df, logger)

            if not detail_df.empty:
                result["details_fetched"] = len(detail_df)
                if dry_run:
                    logger.info(f"[DRY RUN] Would upsert {len(detail_df)} details")
                else:
                    scraper._upsert_details(detail_df)
                    logger.info(f"Upserted {len(detail_df)} details to {SCHEMA}.{DETAIL_TABLE}")

                outage_df = scraper._build_outages(detail_df, df)
                if not outage_df.empty:
                    result["outages_built"] = len(outage_df)
                    if dry_run:
                        logger.info(f"[DRY RUN] Would upsert {len(outage_df)} outages")
                    else:
                        scraper._upsert_outages(outage_df)
                        logger.info(f"Upserted {len(outage_df)} outages to {SCHEMA}.{OUTAGE_TABLE}")
            else:
                logger.info("No detail rows to process")
        except Exception as e:
            logger.warning(f"Detail enrichment failed (non-fatal): {e}")

        run.success(rows_processed=len(df))
        result["success"] = True

    except Exception as e:
        logger.exception(f"Backfill failed: {e}")
        run.failure(error=e)
        result["error"] = str(e)
    finally:
        logger.close()

    return result


# ── Mode 2: Enrich existing DB notices ───────────────────────────────────

def _query_unenriched_notices(
    start_date: str,
    end_date: str,
    source_family: str | None = None,
    pipeline_name: str | None = None,
) -> pd.DataFrame:
    """Query notices in date range that have actionable categories but no details."""
    categories = ", ".join(f"'{c}'" for c in ACTIONABLE_CATEGORIES)

    where = f"""
        n.notice_category IN ({categories})
        AND n.posted_datetime LIKE '%{start_date[:4]}%'
        AND d.notice_identifier IS NULL
    """
    if source_family:
        where += f"\n        AND n.source_family = '{source_family}'"
    if pipeline_name:
        where += f"\n        AND n.pipeline_name = '{pipeline_name}'"

    sql = f"""
        SELECT
            n.source_family,
            n.pipeline_name,
            n.notice_identifier,
            n.notice_type,
            n.notice_subtype,
            n.subject,
            n.notice_status,
            n.posted_datetime,
            n.effective_datetime,
            n.end_datetime,
            n.response_datetime,
            n.detail_url,
            n.notice_category,
            n.severity,
            n.scraped_at
        FROM {SCHEMA}.{TABLE} n
        LEFT JOIN {SCHEMA}.{DETAIL_TABLE} d
            ON n.source_family = d.source_family
            AND n.pipeline_name = d.pipeline_name
            AND n.notice_identifier = d.notice_identifier
        WHERE {where}
        ORDER BY n.source_family, n.pipeline_name, n.posted_datetime DESC
    """

    df = azure_postgresql.pull_from_db(sql)
    if df is None or df.empty:
        return pd.DataFrame(columns=COLUMNS)

    # Ensure column order
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[COLUMNS]


def enrich_existing(
    start_date: str,
    end_date: str,
    source_family: str | None = None,
    pipeline_name: str | None = None,
    dry_run: bool = False,
    detail_limit: int = 500,
    detail_delay: float = 0.5,
) -> dict:
    """Enrich existing DB notices that are missing detail data.

    Queries the DB for actionable notices in the date range without
    entries in notice_details, then fetches detail pages, extracts
    outage data, and upserts to notice_details + planned_outages.
    """
    result = {
        "success": False,
        "total_unenriched": 0,
        "details_fetched": 0,
        "outages_built": 0,
        "errors": [],
    }

    label = source_family or "all"
    if pipeline_name:
        label = f"{label}/{pipeline_name}"

    api_name = f"gas_ebb_enrich_{label.replace('/', '_')}"
    logger = logging_utils.PipelineLogger(
        name=api_name,
        log_dir=LOG_DIR,
        log_to_file=True,
        delete_if_no_errors=True,
    )

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=api_name,
        source="gas_ebbs_backfill",
        target_table=f"{SCHEMA}.{DETAIL_TABLE}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(
            f"ENRICH EXISTING [{start_date} .. {end_date}] "
            f"source={label}"
        )

        # ── Query unenriched notices ──
        logger.section("Querying unenriched notices")
        notices_df = _query_unenriched_notices(
            start_date, end_date, source_family, pipeline_name
        )

        # Client-side date filter for accuracy (DB LIKE is approximate)
        if not notices_df.empty:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )

            def in_range(dt_str):
                parsed = _parse_posted_date(str(dt_str))
                if parsed is None:
                    return True  # Keep unparseable
                return start_dt <= parsed <= end_dt

            mask = notices_df["posted_datetime"].apply(in_range)
            notices_df = notices_df[mask].reset_index(drop=True)

        total = len(notices_df)
        result["total_unenriched"] = total
        logger.info(f"Found {total} unenriched actionable notices")

        if total == 0:
            logger.info("Nothing to enrich.")
            run.success(rows_processed=0)
            result["success"] = True
            return result

        # Show breakdown
        counts = notices_df.groupby(["source_family", "notice_category"]).size()
        for (fam, cat), cnt in counts.items():
            logger.info(f"  {fam} / {cat}: {cnt}")

        # ── Group by (source_family, pipeline_name) for scraper reuse ──
        groups = notices_df.groupby(["source_family", "pipeline_name"])
        logger.info(f"Processing {len(groups)} pipeline groups")

        all_detail_rows = []
        all_outage_rows = []

        for (fam, pipe), group_df in groups:
            group_df = group_df.head(detail_limit)
            logger.section(f"Enriching {fam}/{pipe} ({len(group_df)} notices)")

            try:
                scraper = create_scraper(fam, pipe)
            except Exception as e:
                logger.warning(f"Cannot create scraper for {fam}/{pipe}: {e}")
                result["errors"].append(f"{fam}/{pipe}: {e}")
                continue

            delay = scraper.config.get("detail_fetch_delay", detail_delay)

            for _, row in group_df.iterrows():
                notice_id = row["notice_identifier"]
                url = row["detail_url"]

                if not url or str(url).strip() == "":
                    logger.info(f"  {notice_id}: no detail_url, skipping")
                    continue

                try:
                    html = scraper._pull_detail(str(url))
                    extraction = scraper._parse_detail(html, row.to_dict())

                    extraction["source_family"] = fam
                    extraction["pipeline_name"] = pipe
                    extraction["notice_identifier"] = notice_id
                    extraction["detail_html_blob_url"] = ""
                    extraction["fetched_at"] = datetime.now(timezone.utc).isoformat()

                    all_detail_rows.append(extraction)
                    logger.info(f"  {notice_id}: OK")
                except Exception as e:
                    logger.warning(f"  {notice_id}: {e}")
                    result["errors"].append(f"{fam}/{pipe}/{notice_id}: {e}")

                if delay > 0:
                    time.sleep(delay)

        # ── Build detail DataFrame ──
        if all_detail_rows:
            detail_df = pd.DataFrame(all_detail_rows)
            for col in DETAIL_COLUMNS:
                if col not in detail_df.columns:
                    detail_df[col] = ""
            detail_df = detail_df[DETAIL_COLUMNS]

            for col in detail_df.columns:
                if col in ("capacity_value", "capacity_bcfd"):
                    detail_df[col] = pd.to_numeric(detail_df[col], errors="coerce").fillna(0.0)
                else:
                    detail_df[col] = detail_df[col].fillna("").astype(str)

            if "detail_text" in detail_df.columns:
                detail_df["detail_text"] = (
                    detail_df["detail_text"]
                    .str.replace("\x00", "", regex=False)
                    .str.replace(r"[\r\n\t]+", " ", regex=True)
                    .str.strip()
                )

            result["details_fetched"] = len(detail_df)
            logger.section(f"Upserting {len(detail_df)} detail rows")

            if dry_run:
                logger.info(f"[DRY RUN] Would upsert {len(detail_df)} details")
            else:
                azure_postgresql.upsert_to_azure_postgresql(
                    schema=SCHEMA,
                    table_name=DETAIL_TABLE,
                    df=detail_df,
                    columns=DETAIL_COLUMNS,
                    data_types=DETAIL_DATA_TYPES,
                    primary_key=DETAIL_PRIMARY_KEY,
                )
                logger.success(f"Upserted {len(detail_df)} details")

            # ── Build outages ──
            # We need the full notices_df for category/severity lookup
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            outage_rows = []

            for _, d in detail_df.iterrows():
                key_mask = (
                    (notices_df["source_family"] == d["source_family"])
                    & (notices_df["pipeline_name"] == d["pipeline_name"])
                    & (notices_df["notice_identifier"] == d["notice_identifier"])
                )
                match = notices_df[key_mask]
                if match.empty:
                    continue
                notice = match.iloc[0]

                start_d = str(d.get("gas_day_start", ""))
                end_d = str(d.get("gas_day_end", ""))

                if start_d and end_d:
                    status = "COMPLETED" if end_d < today else ("ACTIVE" if start_d <= today else "UPCOMING")
                elif start_d:
                    status = "ACTIVE" if start_d <= today else "UPCOMING"
                else:
                    status = "UPCOMING"

                cap = d.get("capacity_bcfd", 0.0)
                try:
                    cap = float(cap) if cap else 0.0
                except (ValueError, TypeError):
                    cap = 0.0

                outage_rows.append({
                    "source_family": d["source_family"],
                    "pipeline_name": d["pipeline_name"],
                    "notice_identifier": d["notice_identifier"],
                    "location": str(d.get("affected_locations", "")),
                    "sub_region": "",
                    "start_date": start_d,
                    "end_date": end_d,
                    "capacity_loss_bcfd": cap,
                    "outage_type": str(notice.get("notice_category", "")),
                    "status": status,
                    "subject": str(notice.get("subject", "")),
                    "notice_category": str(notice.get("notice_category", "")),
                    "severity": int(notice.get("severity", 0)),
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })

            if outage_rows:
                outage_df = pd.DataFrame(outage_rows)[OUTAGE_COLUMNS]
                for col in outage_df.columns:
                    if col == "severity":
                        outage_df[col] = outage_df[col].fillna(0).astype(int)
                    elif col == "capacity_loss_bcfd":
                        outage_df[col] = outage_df[col].fillna(0.0)
                    else:
                        outage_df[col] = outage_df[col].fillna("").astype(str)

                result["outages_built"] = len(outage_df)
                if dry_run:
                    logger.info(f"[DRY RUN] Would upsert {len(outage_df)} outages")
                else:
                    azure_postgresql.upsert_to_azure_postgresql(
                        schema=SCHEMA,
                        table_name=OUTAGE_TABLE,
                        df=outage_df,
                        columns=OUTAGE_COLUMNS,
                        data_types=OUTAGE_DATA_TYPES,
                        primary_key=OUTAGE_PRIMARY_KEY,
                    )
                    logger.info(f"Upserted {len(outage_df)} outages")

        run.success(rows_processed=result["details_fetched"])
        result["success"] = True

    except Exception as e:
        logger.exception(f"Enrich failed: {e}")
        run.failure(error=e)
        result["errors"].append(str(e))
    finally:
        logger.close()

    return result


# ── CLI ──────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical gas EBB outage data.",
    )
    parser.add_argument(
        "--enrich-existing",
        action="store_true",
        help=(
            "Instead of scraping listing pages, query the DB for notices "
            "in the date range that are missing detail enrichment, and "
            "fetch/enrich those."
        ),
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Source family (e.g. williams, enbridge). Default: all.",
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default=None,
        help="Single pipeline name within a source family.",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2025-01-01",
        help="Start date (YYYY-MM-DD). Default: 2025-01-01.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="2025-12-31",
        help="End date (YYYY-MM-DD). Default: 2025-12-31.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and extract only, do not upsert to database.",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=500,
        help="Max detail pages to fetch per pipeline (default: 500).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between pipeline runs in scrape mode (default: 1.0).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Validate dates
    try:
        datetime.strptime(args.start, "%Y-%m-%d")
        datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError:
        print("Error: --start and --end must be YYYY-MM-DD format.")
        sys.exit(1)

    if args.start > args.end:
        print("Error: --start must be before --end.")
        sys.exit(1)

    # ── Enrich-existing mode ──
    if args.enrich_existing:
        mode = "DRY RUN" if args.dry_run else "LIVE"
        print(
            f"\n=== Gas EBB Enrich Existing ({mode}) ===\n"
            f"  Date range:  {args.start} .. {args.end}\n"
            f"  Source:      {args.source or 'all'}\n"
            f"  Pipeline:    {args.pipeline or 'all'}\n"
            f"  Detail cap:  {args.detail_limit} per pipeline\n"
        )

        result = enrich_existing(
            start_date=args.start,
            end_date=args.end,
            source_family=args.source,
            pipeline_name=args.pipeline,
            dry_run=args.dry_run,
            detail_limit=args.detail_limit,
        )

        status = "SUCCESS" if result["success"] else "FAILED"
        print(
            f"\n=== Enrich Complete ({status}) ===\n"
            f"  Unenriched found: {result['total_unenriched']}\n"
            f"  Details fetched:  {result['details_fetched']}\n"
            f"  Outages built:    {result['outages_built']}\n"
        )
        if result["errors"]:
            print(f"  Errors ({len(result['errors'])}):")
            for err in result["errors"][:20]:
                print(f"    - {err}")

        sys.exit(0 if result["success"] else 1)

    # ── Scrape mode ──
    all_pipelines = discover_all_pipelines()

    if args.source:
        all_pipelines = [
            (name, fam, cfg)
            for name, fam, cfg in all_pipelines
            if fam == args.source
        ]
        if not all_pipelines:
            print(f"Error: no pipelines found for source family '{args.source}'.")
            sys.exit(1)

    if args.pipeline:
        all_pipelines = [
            (name, fam, cfg)
            for name, fam, cfg in all_pipelines
            if name == args.pipeline
        ]
        if not all_pipelines:
            print(f"Error: pipeline '{args.pipeline}' not found.")
            sys.exit(1)

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(
        f"\n=== Gas EBB Backfill Scrape ({mode}) ===\n"
        f"  Date range: {args.start} .. {args.end}\n"
        f"  Pipelines:  {len(all_pipelines)}\n"
        f"  Detail cap: {args.detail_limit} per pipeline\n"
    )

    results = []
    for i, (name, family, _) in enumerate(all_pipelines, 1):
        print(f"  [{i}/{len(all_pipelines)}] {name} ({family}) ...", end=" ", flush=True)

        summary = backfill_scrape(
            source_family=family,
            pipeline_name=name,
            start_date=args.start,
            end_date=args.end,
            dry_run=args.dry_run,
            detail_limit=args.detail_limit,
        )
        results.append(summary)

        status = "PASS" if summary["success"] else "FAIL"
        print(
            f"{status}  "
            f"parsed={summary['notices_parsed']}  "
            f"in_range={summary['notices_filtered']}  "
            f"upserted={summary['notices_upserted']}  "
            f"details={summary['details_fetched']}  "
            f"outages={summary['outages_built']}"
            + (f"  err={summary['error']}" if summary["error"] else "")
        )

        if i < len(all_pipelines) and args.delay > 0:
            time.sleep(args.delay)

    # Summary
    passed = sum(1 for r in results if r["success"])
    failed = len(results) - passed
    total_parsed = sum(r["notices_parsed"] for r in results)
    total_filtered = sum(r["notices_filtered"] for r in results)
    total_upserted = sum(r["notices_upserted"] for r in results)
    total_details = sum(r["details_fetched"] for r in results)
    total_outages = sum(r["outages_built"] for r in results)

    print(
        f"\n=== Backfill Complete ===\n"
        f"  Pipelines:  {passed} passed, {failed} failed\n"
        f"  Notices:    {total_parsed} parsed, {total_filtered} in range, "
        f"{total_upserted} upserted\n"
        f"  Details:    {total_details} fetched\n"
        f"  Outages:    {total_outages} built\n"
    )

    if failed:
        print("Failed pipelines:")
        for r in results:
            if not r["success"]:
                print(f"  - {r['pipeline_name']} ({r['source_family']}): {r['error']}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
