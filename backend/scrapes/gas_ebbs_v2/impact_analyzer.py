"""
Gas EBB Impact Analyzer — v2 (Azure SQL target).

Maps pipeline outages to production sub-regions and calculates
capacity/pricing impact by joining planned_outages with a
pipeline_regions reference table.

Usage:
    python impact_analyzer.py              # run full impact analysis
    python impact_analyzer.py --seed-only  # seed reference table only
    python impact_analyzer.py --dry-run    # compute impacts without upserting
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend import secrets  # noqa: F401
from backend.utils import (
    azure_sql_utils as azure_sql,
    logging_utils,
    pipeline_run_logger,
)


# ── Constants ─────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / "logs"

REFERENCE_SCHEMA = "gas_reference"
REFERENCE_TABLE = "pipeline_regions"

REFERENCE_COLUMNS = [
    "pipeline_name",
    "source_family",
    "display_name",
    "operator",
    "primary_basin",
    "secondary_basins",
    "primary_region",
    "direction",
    "design_capacity_bcfd",
    "notes",
    "updated_at",
]

REFERENCE_DATA_TYPES = [
    "VARCHAR",   # pipeline_name
    "VARCHAR",   # source_family
    "VARCHAR",   # display_name
    "VARCHAR",   # operator
    "VARCHAR",   # primary_basin
    "VARCHAR",   # secondary_basins
    "VARCHAR",   # primary_region
    "VARCHAR",   # direction
    "FLOAT",     # design_capacity_bcfd
    "VARCHAR",   # notes
    "VARCHAR",   # updated_at
]

REFERENCE_PRIMARY_KEY = ["source_family", "pipeline_name"]

IMPACT_SCHEMA = "gas_ebbs"
IMPACT_TABLE = "outage_impacts"

IMPACT_COLUMNS = [
    "source_family",
    "pipeline_name",
    "notice_identifier",
    "primary_basin",
    "primary_region",
    "direction",
    "design_capacity_bcfd",
    "capacity_loss_bcfd",
    "capacity_loss_pct",
    "price_impact",
    "impact_summary",
    "computed_at",
]

IMPACT_DATA_TYPES = [
    "VARCHAR",   # source_family
    "VARCHAR",   # pipeline_name
    "VARCHAR",   # notice_identifier
    "VARCHAR",   # primary_basin
    "VARCHAR",   # primary_region
    "VARCHAR",   # direction
    "FLOAT",     # design_capacity_bcfd
    "FLOAT",     # capacity_loss_bcfd
    "FLOAT",     # capacity_loss_pct
    "VARCHAR",   # price_impact
    "VARCHAR",   # impact_summary
    "VARCHAR",   # computed_at
]

IMPACT_PRIMARY_KEY = ["source_family", "pipeline_name", "notice_identifier"]


# ── Reference data: pipeline-to-region mappings ───────────────────────────
# Import seed data from gas_ebbs (shared reference, not DB-specific)

from backend.scrapes.gas_ebbs.impact_analyzer import PIPELINE_REGION_SEED


# ── Seeding logic ─────────────────────────────────────────────────────────


def build_reference_df() -> pd.DataFrame:
    """Build a DataFrame of pipeline region mappings from the seed data."""
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entry in PIPELINE_REGION_SEED:
        (source_family, pipeline_name, display_name, operator,
         primary_basin, secondary_basins, primary_region, direction,
         design_capacity_bcfd, notes) = entry
        rows.append({
            "pipeline_name": pipeline_name,
            "source_family": source_family,
            "display_name": display_name or "",
            "operator": operator or "",
            "primary_basin": primary_basin or "",
            "secondary_basins": secondary_basins or "",
            "primary_region": primary_region or "",
            "direction": direction or "",
            "design_capacity_bcfd": design_capacity_bcfd if design_capacity_bcfd is not None else 0.0,
            "notes": notes or "",
            "updated_at": now,
        })

    df = pd.DataFrame(rows)
    df = df[REFERENCE_COLUMNS]

    # Fill NaN
    for col in df.columns:
        if col == "design_capacity_bcfd":
            df[col] = df[col].fillna(0.0)
        else:
            df[col] = df[col].fillna("").astype(str)

    return df


def seed_reference_table() -> int:
    """Upsert pipeline region reference data to gas_reference.pipeline_regions.

    Returns the number of rows seeded.
    """
    df = build_reference_df()
    azure_sql.upsert_to_azure_sql(
        schema=REFERENCE_SCHEMA,
        table_name=REFERENCE_TABLE,
        df=df,
        columns=REFERENCE_COLUMNS,
        data_types=REFERENCE_DATA_TYPES,
        primary_key=REFERENCE_PRIMARY_KEY,
    )
    return len(df)


# ── Impact computation ────────────────────────────────────────────────────


def pull_planned_outages() -> pd.DataFrame:
    """Pull all planned outages from gas_ebbs.planned_outages."""
    return azure_sql.pull_from_db("""
        SELECT
            source_family,
            pipeline_name,
            notice_identifier,
            location,
            sub_region,
            start_date,
            end_date,
            capacity_loss_bcfd,
            outage_type,
            status,
            subject,
            notice_category,
            severity,
            scraped_at
        FROM gas_ebbs.planned_outages
    """)


def pull_pipeline_regions() -> pd.DataFrame:
    """Pull all pipeline region mappings from gas_reference.pipeline_regions."""
    return azure_sql.pull_from_db("""
        SELECT
            source_family,
            pipeline_name,
            display_name,
            operator,
            primary_basin,
            secondary_basins,
            primary_region,
            direction,
            design_capacity_bcfd,
            notes
        FROM gas_reference.pipeline_regions
    """)


def _classify_price_impact(direction: str, capacity_loss: float) -> str:
    """Determine price impact based on pipeline direction and capacity loss."""
    if not direction or capacity_loss <= 0:
        return "unknown"

    direction = str(direction).strip().lower()

    if direction == "production_area":
        return "bearish"
    elif direction == "demand_area":
        return "bullish"
    elif direction == "bidirectional":
        return "neutral"
    else:
        return "unknown"


def _generate_impact_summary(
    pipeline_name: str,
    display_name: str,
    direction: str,
    primary_basin: str,
    primary_region: str,
    capacity_loss_bcfd: float,
    design_capacity_bcfd: float,
    capacity_loss_pct: float,
    price_impact: str,
    subject: str,
) -> str:
    """Generate a human-readable impact summary string."""
    name = display_name if display_name else pipeline_name
    parts = []

    parts.append(f"{name}")

    location_parts = []
    if primary_basin:
        location_parts.append(primary_basin)
    if primary_region:
        location_parts.append(primary_region)
    if location_parts:
        parts.append(f"({', '.join(location_parts)})")

    if capacity_loss_bcfd > 0 and design_capacity_bcfd > 0:
        parts.append(
            f"— {capacity_loss_bcfd:.3f} Bcf/d loss "
            f"({capacity_loss_pct:.1f}% of {design_capacity_bcfd:.3f} Bcf/d design)"
        )
    elif capacity_loss_bcfd > 0:
        parts.append(f"— {capacity_loss_bcfd:.3f} Bcf/d loss")

    direction_labels = {
        "production_area": "production-area pipeline",
        "demand_area": "demand-area pipeline",
        "bidirectional": "bidirectional pipeline",
    }
    if direction and direction in direction_labels:
        parts.append(f"[{direction_labels[direction]}]")

    impact_labels = {
        "bullish": "BULLISH: supply constrained to demand area",
        "bearish": "BEARISH: gas trapped in production area",
        "neutral": "NEUTRAL: bidirectional flow; direction-dependent",
        "unknown": "UNKNOWN: insufficient data for classification",
    }
    parts.append(f"-> {impact_labels.get(price_impact, 'UNKNOWN')}")

    return " ".join(parts)


def compute_impacts(outages_df: pd.DataFrame, regions_df: pd.DataFrame) -> pd.DataFrame:
    """Join planned_outages with pipeline_regions to compute impacts."""
    if outages_df is None or outages_df.empty:
        return pd.DataFrame(columns=IMPACT_COLUMNS)

    merged = outages_df.merge(
        regions_df[["source_family", "pipeline_name", "display_name",
                     "primary_basin", "primary_region", "direction",
                     "design_capacity_bcfd"]],
        on=["source_family", "pipeline_name"],
        how="left",
        suffixes=("", "_ref"),
    )

    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for _, row in merged.iterrows():
        cap_loss = float(row.get("capacity_loss_bcfd", 0) or 0)
        design_cap = float(row.get("design_capacity_bcfd", 0) or 0)
        direction = str(row.get("direction", "") or "")
        primary_basin = str(row.get("primary_basin", "") or "")
        primary_region = str(row.get("primary_region", "") or "")
        display_name = str(row.get("display_name", "") or "")
        subject = str(row.get("subject", "") or "")

        if design_cap > 0 and cap_loss > 0:
            loss_pct = round((cap_loss / design_cap) * 100, 2)
        else:
            loss_pct = 0.0

        price_impact = _classify_price_impact(direction, cap_loss)

        summary = _generate_impact_summary(
            pipeline_name=row["pipeline_name"],
            display_name=display_name,
            direction=direction,
            primary_basin=primary_basin,
            primary_region=primary_region,
            capacity_loss_bcfd=cap_loss,
            design_capacity_bcfd=design_cap,
            capacity_loss_pct=loss_pct,
            price_impact=price_impact,
            subject=subject,
        )

        rows.append({
            "source_family": row["source_family"],
            "pipeline_name": row["pipeline_name"],
            "notice_identifier": row["notice_identifier"],
            "primary_basin": primary_basin,
            "primary_region": primary_region,
            "direction": direction,
            "design_capacity_bcfd": design_cap,
            "capacity_loss_bcfd": cap_loss,
            "capacity_loss_pct": loss_pct,
            "price_impact": price_impact,
            "impact_summary": summary[:2000],
            "computed_at": now,
        })

    if not rows:
        return pd.DataFrame(columns=IMPACT_COLUMNS)

    impact_df = pd.DataFrame(rows)[IMPACT_COLUMNS]

    for col in impact_df.columns:
        if col in ("design_capacity_bcfd", "capacity_loss_bcfd", "capacity_loss_pct"):
            impact_df[col] = impact_df[col].fillna(0.0)
        else:
            impact_df[col] = impact_df[col].fillna("").astype(str)

    return impact_df


def upsert_impacts(impact_df: pd.DataFrame) -> int:
    """Upsert computed impact rows to gas_ebbs.outage_impacts."""
    if impact_df.empty:
        return 0

    azure_sql.upsert_to_azure_sql(
        schema=IMPACT_SCHEMA,
        table_name=IMPACT_TABLE,
        df=impact_df,
        columns=IMPACT_COLUMNS,
        data_types=IMPACT_DATA_TYPES,
        primary_key=IMPACT_PRIMARY_KEY,
    )
    return len(impact_df)


# ── Runner ────────────────────────────────────────────────────────────────


def run_impact_analysis(dry_run: bool = False) -> dict:
    """Full impact analysis pipeline."""
    api_scrape_name = "gas_ebb_v2_impact_analysis"
    logger = logging_utils.PipelineLogger(
        name=api_scrape_name,
        log_dir=LOG_DIR,
        log_to_file=True,
        delete_if_no_errors=True,
    )

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=api_scrape_name,
        source="gas_ebbs_v2",
        target_table=f"{IMPACT_SCHEMA}.{IMPACT_TABLE}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(api_scrape_name)

        logger.section("Seeding Reference Data")
        ref_count = seed_reference_table()
        logger.info(f"Seeded {ref_count} pipeline region mappings to {REFERENCE_SCHEMA}.{REFERENCE_TABLE}")

        logger.section("Pulling Data")
        outages_df = pull_planned_outages()
        regions_df = pull_pipeline_regions()

        outage_count = len(outages_df) if outages_df is not None else 0
        region_count = len(regions_df) if regions_df is not None else 0
        logger.info(f"Pulled {outage_count} outages, {region_count} region mappings")

        if outages_df is None or outages_df.empty:
            logger.warning("No planned outages found — nothing to analyze")
            run.success(rows_processed=0)
            return {"outages": 0, "regions": region_count, "impacts": 0}

        logger.section("Computing Impacts")
        impact_df = compute_impacts(outages_df, regions_df)
        logger.info(f"Computed {len(impact_df)} impact rows")

        if not impact_df.empty:
            counts = impact_df["price_impact"].value_counts().to_dict()
            for impact_type, count in sorted(counts.items()):
                logger.info(f"  {impact_type}: {count}")

            region_counts = impact_df["primary_region"].value_counts().to_dict()
            for region, count in sorted(region_counts.items()):
                if region:
                    logger.info(f"  region={region}: {count}")

        if dry_run:
            logger.info("DRY RUN — skipping upsert")
            impact_count = 0
        else:
            logger.section("Upserting Impacts")
            impact_count = upsert_impacts(impact_df)
            logger.success(
                f"Upserted {impact_count} impact rows to "
                f"{IMPACT_SCHEMA}.{IMPACT_TABLE}"
            )

        run.success(rows_processed=impact_count)

        return {
            "outages": outage_count,
            "regions": region_count,
            "impacts": impact_count if not dry_run else len(impact_df),
        }

    except Exception as e:
        logger.exception(f"Impact analysis failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logger.close()


# ── CLI entrypoint ────────────────────────────────────────────────────────


def main():
    args = sys.argv[1:]

    if "--seed-only" in args:
        print("\n=== Seeding Pipeline Regions Reference Table (Azure SQL) ===\n")
        count = seed_reference_table()
        print(f"  Seeded {count} pipeline region mappings to {REFERENCE_SCHEMA}.{REFERENCE_TABLE}")
        print()
        return

    dry_run = "--dry-run" in args

    if dry_run:
        print("\n=== Gas EBB v2 Impact Analysis (DRY RUN) ===\n")
    else:
        print("\n=== Gas EBB v2 Impact Analysis ===\n")

    result = run_impact_analysis(dry_run=dry_run)

    print(f"\n  Outages analyzed:  {result['outages']}")
    print(f"  Region mappings:   {result['regions']}")
    print(f"  Impacts computed:  {result['impacts']}")
    if dry_run:
        print("  (dry run — no data written to DB)")
    print()


if __name__ == "__main__":
    main()
