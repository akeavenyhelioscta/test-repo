"""
Abstract base class for gas EBB scrapers — v2 (Azure SQL target).

Identical scraping logic to gas_ebbs, but writes to Azure SQL Server
(GenscapeDataFeed) instead of Azure PostgreSQL.

Each source family (PipeRiv, Kinder Morgan, Williams, etc.) implements
a concrete adapter by subclassing EBBScraper and overriding _parse_listing().
Only override _pull() when the transport differs (session, Selenium, etc.).

Usage:
    scraper = create_scraper("piperiv", "algonquin")
    scraper.main()
"""

import logging
import time
import yaml
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from backend import secrets  # noqa: F401 — ensures env vars are loaded
from backend.utils import (
    azure_sql_utils as azure_sql,
    logging_utils,
    pipeline_run_logger,
)

from backend.scrapes.gas_ebbs_v2 import notice_classifier, outage_extractor
from backend.scrapes.gas_ebbs_v2.ebb_utils import DEFAULT_HEADERS


# ── Storage constants ──────────────────────────────────────────────────────

SCHEMA = "gas_ebbs"
TABLE = "notices"
SNAPSHOT_TABLE = "notice_snapshots"

COLUMNS = [
    "source_family",
    "pipeline_name",
    "notice_identifier",
    "notice_type",
    "notice_subtype",
    "subject",
    "notice_status",
    "posted_datetime",
    "effective_datetime",
    "end_datetime",
    "response_datetime",
    "detail_url",
    "notice_category",
    "severity",
    "scraped_at",
]

DATA_TYPES = [
    "VARCHAR",   # source_family
    "VARCHAR",   # pipeline_name
    "VARCHAR",   # notice_identifier
    "VARCHAR",   # notice_type
    "VARCHAR",   # notice_subtype
    "VARCHAR",   # subject
    "VARCHAR",   # notice_status
    "VARCHAR",   # posted_datetime
    "VARCHAR",   # effective_datetime
    "VARCHAR",   # end_datetime
    "VARCHAR",   # response_datetime
    "VARCHAR",   # detail_url
    "VARCHAR",   # notice_category
    "INTEGER",   # severity
    "VARCHAR",   # scraped_at
]

PRIMARY_KEY = ["source_family", "pipeline_name", "notice_identifier"]

SNAPSHOT_COLUMNS = COLUMNS.copy()

SNAPSHOT_DATA_TYPES = DATA_TYPES.copy()

SNAPSHOT_PRIMARY_KEY = [
    "source_family",
    "pipeline_name",
    "notice_identifier",
    "scraped_at",
]

# ── notice_details table ──────────────────────────────────────────────────

DETAIL_TABLE = "notice_details"

DETAIL_COLUMNS = [
    "source_family",
    "pipeline_name",
    "notice_identifier",
    "detail_text",
    "detail_html_blob_url",
    "gas_day_start",
    "gas_day_end",
    "capacity_value",
    "capacity_bcfd",
    "affected_locations",
    "receipt_points",
    "delivery_points",
    "fetched_at",
]

DETAIL_DATA_TYPES = [
    "VARCHAR",   # source_family
    "VARCHAR",   # pipeline_name
    "VARCHAR",   # notice_identifier
    "VARCHAR",   # detail_text
    "VARCHAR",   # detail_html_blob_url
    "VARCHAR",   # gas_day_start
    "VARCHAR",   # gas_day_end
    "FLOAT",     # capacity_value
    "FLOAT",     # capacity_bcfd
    "VARCHAR",   # affected_locations
    "VARCHAR",   # receipt_points
    "VARCHAR",   # delivery_points
    "VARCHAR",   # fetched_at
]

DETAIL_PRIMARY_KEY = ["source_family", "pipeline_name", "notice_identifier"]

# ── planned_outages table ─────────────────────────────────────────────────

OUTAGE_TABLE = "planned_outages"

OUTAGE_COLUMNS = [
    "source_family",
    "pipeline_name",
    "notice_identifier",
    "location",
    "sub_region",
    "start_date",
    "end_date",
    "capacity_loss_bcfd",
    "outage_type",
    "status",
    "subject",
    "notice_category",
    "severity",
    "scraped_at",
]

OUTAGE_DATA_TYPES = [
    "VARCHAR",   # source_family
    "VARCHAR",   # pipeline_name
    "VARCHAR",   # notice_identifier
    "VARCHAR",   # location
    "VARCHAR",   # sub_region
    "VARCHAR",   # start_date
    "VARCHAR",   # end_date
    "FLOAT",     # capacity_loss_bcfd
    "VARCHAR",   # outage_type
    "VARCHAR",   # status
    "VARCHAR",   # subject
    "VARCHAR",   # notice_category
    "INTEGER",   # severity
    "VARCHAR",   # scraped_at
]

OUTAGE_PRIMARY_KEY = ["source_family", "pipeline_name", "notice_identifier"]

# Categories eligible for detail enrichment
ACTIONABLE_CATEGORIES = {
    "force_majeure",
    "capacity_reduction",
    "ofo",
    "critical_alert",
    "maintenance",
}


LOG_DIR = Path(__file__).parent / "logs"


# ── Base class ─────────────────────────────────────────────────────────────


class EBBScraper(ABC):
    """Abstract base class for all gas EBB source-family scrapers.

    Subclasses must implement ``_parse_listing()``.
    Override ``_pull()`` only when the HTTP transport differs from a simple GET
    (e.g. session bootstrap for Williams, Selenium for Tallgrass).
    """

    def __init__(
        self,
        pipeline_name: str,
        source_family: str,
        listing_url: str,
        detail_url_template: str = "",
        datetime_formats: Optional[list[str]] = None,
        config: Optional[dict] = None,
    ):
        self.pipeline_name = pipeline_name
        self.source_family = source_family
        self.listing_url = listing_url
        self.detail_url_template = detail_url_template
        self.datetime_formats = datetime_formats or []
        self.config = config or {}
        self._blob_client = None

    # ── Blob storage (lazy-init) ───────────────────────────────────────

    @property
    def blob_client(self):
        """Lazy-init Azure Blob client. Returns None if credentials unavailable."""
        if self._blob_client is None:
            if secrets.AZURE_STORAGE_CONNECTION_STRING:
                from backend.utils.azure_blob_storage_utils import AzureBlobStorageClient
                self._blob_client = AzureBlobStorageClient()
            else:
                return None
        return self._blob_client

    # ── Standard pipeline functions ────────────────────────────────────

    def _get_listing_sources(self) -> list[dict]:
        """Return list of source configs to scrape.

        Each dict must have ``url``. Additional keys are passed as
        keyword arguments to ``_parse_listing()``.

        Default: single URL from ``self.listing_url``.
        Override in adapters that need to scrape multiple pages per
        pipeline (e.g. Enbridge scrapes both CRI and NON pages).
        """
        return [{"url": self.listing_url}]

    def _pull(self, url: str = "") -> str:
        """Fetch listing page HTML via GET. Override for session/Selenium."""
        target = url or self.listing_url
        response = requests.get(
            target,
            headers=DEFAULT_HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        return response.text

    @abstractmethod
    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        """Parse listing-page HTML into a list of raw notice dicts.

        Each dict should contain the fields available from the listing page:
            notice_type, notice_subtype, subject, notice_status,
            posted_datetime, effective_datetime, end_datetime,
            response_datetime, notice_identifier, detail_url

        Missing fields should be set to empty string, not omitted.

        Keyword arguments come from ``_get_listing_sources()`` context.
        """
        ...

    def _format(self, raw_notices: list[dict]) -> pd.DataFrame:
        """Normalize raw notice dicts into a DataFrame ready for upsert.

        Adds pipeline metadata, classification, and scraped_at timestamp.
        """
        if not raw_notices:
            return pd.DataFrame(columns=COLUMNS)

        df = pd.DataFrame(raw_notices)

        # Add pipeline metadata
        df["source_family"] = self.source_family
        df["pipeline_name"] = self.pipeline_name
        df["scraped_at"] = datetime.now(timezone.utc).isoformat()

        # Classify each notice by subject
        classifications = df["subject"].apply(notice_classifier.classify)
        df["notice_category"] = classifications.apply(lambda x: x[0])
        df["severity"] = classifications.apply(lambda x: x[1])

        # Ensure all expected columns exist
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = ""

        # Reorder to canonical column order
        df = df[COLUMNS]

        # Fill NaN with empty string for VARCHAR columns, 0 for severity
        for col in df.columns:
            if col == "severity":
                df[col] = df[col].fillna(0).astype(int)
            else:
                df[col] = df[col].fillna("").astype(str)

        return df

    def _upsert(self, df: pd.DataFrame) -> None:
        """Upsert notices to gas_ebbs.notices and gas_ebbs.notice_snapshots."""
        if df.empty:
            return

        # Upsert to canonical notices table
        azure_sql.upsert_to_azure_sql(
            schema=SCHEMA,
            table_name=TABLE,
            df=df,
            columns=COLUMNS,
            data_types=DATA_TYPES,
            primary_key=PRIMARY_KEY,
        )

        # Append to snapshot table for revision history
        azure_sql.upsert_to_azure_sql(
            schema=SCHEMA,
            table_name=SNAPSHOT_TABLE,
            df=df,
            columns=SNAPSHOT_COLUMNS,
            data_types=SNAPSHOT_DATA_TYPES,
            primary_key=SNAPSHOT_PRIMARY_KEY,
        )

    # ── Detail enrichment (Phase 2) ───────────────────────────────────

    def _archive_html(self, html: str, page_type: str, notice_id: str = "") -> str:
        """Upload HTML to Azure Blob. Returns blob URL, or empty string on failure."""
        if not self.config.get("archive_html", True):
            return ""
        client = self.blob_client
        if not client:
            return ""
        try:
            return client.upload_ebb_html(
                html=html,
                source_family=self.source_family,
                pipeline_name=self.pipeline_name,
                page_type=page_type,
                notice_id=notice_id,
            )
        except Exception as e:
            logging.warning(f"HTML archive failed (non-fatal): {e}")
            return ""

    def _pull_detail(self, url: str) -> str:
        """Fetch a detail page. Adapters may override for session-based fetching."""
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()
        # Strip null bytes that break encoding
        return response.text.replace("\x00", "")

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse a detail page and extract outage fields.

        Default: extract body text via BeautifulSoup, run outage_extractor.
        Adapters should override for structured parsing of their HTML format.
        """
        soup = BeautifulSoup(html, "html.parser")
        # Remove scripts/styles for cleaner text
        for tag in soup(["script", "style"]):
            tag.decompose()
        body_text = soup.get_text(separator=" ", strip=True)
        # Collapse whitespace
        body_text = " ".join(body_text.split())

        extraction = outage_extractor.extract_outage(
            subject=notice.get("subject", ""),
            detail_text=body_text,
        )
        extraction["detail_text"] = body_text[:5000]  # cap stored text
        return extraction

    def _fetch_details(self, df: pd.DataFrame, logger) -> pd.DataFrame:
        """Fetch detail pages for actionable notices and enrich the DataFrame.

        Filters to notices with non-empty detail_url and actionable categories,
        applies a per-run cap, fetches with delay, and merges extraction results.
        """
        empty_detail = pd.DataFrame(columns=DETAIL_COLUMNS)

        if not self.config.get("detail_enabled", True):
            logger.info("Detail fetching disabled in config")
            return empty_detail

        limit = self.config.get("detail_fetch_limit", 50)
        delay = self.config.get("detail_fetch_delay", 0.5)

        # Filter to actionable notices with detail URLs
        mask = (
            (df["detail_url"].str.len() > 0)
            & (df["notice_category"].isin(ACTIONABLE_CATEGORIES))
        )
        candidates = df[mask].head(limit)

        if candidates.empty:
            logger.info("No actionable notices with detail URLs")
            return empty_detail

        logger.info(f"Fetching details for {len(candidates)} notices (limit={limit})")

        detail_rows = []
        for idx, row in candidates.iterrows():
            notice_id = row["notice_identifier"]
            url = row["detail_url"]

            try:
                html = self._pull_detail(url)
                blob_url = self._archive_html(html, "detail", notice_id)
                extraction = self._parse_detail(html, row.to_dict())
                extraction["source_family"] = row["source_family"]
                extraction["pipeline_name"] = row["pipeline_name"]
                extraction["notice_identifier"] = notice_id
                extraction["detail_html_blob_url"] = blob_url
                extraction["fetched_at"] = datetime.now(timezone.utc).isoformat()
                detail_rows.append(extraction)
                logger.info(f"  {notice_id}: OK")
            except Exception as e:
                logger.warning(f"  {notice_id}: detail fetch failed — {e}")

            if delay > 0:
                time.sleep(delay)

        if detail_rows:
            detail_df = pd.DataFrame(detail_rows)
            # Ensure all detail columns exist
            for col in DETAIL_COLUMNS:
                if col not in detail_df.columns:
                    detail_df[col] = ""
            detail_df = detail_df[DETAIL_COLUMNS]
            # Fill NaN
            for col in detail_df.columns:
                if col in ("capacity_value", "capacity_bcfd"):
                    detail_df[col] = detail_df[col].fillna(0.0)
                else:
                    detail_df[col] = detail_df[col].fillna("").astype(str)
            # Sanitize detail_text: strip null bytes, newlines, tabs
            if "detail_text" in detail_df.columns:
                detail_df["detail_text"] = (
                    detail_df["detail_text"]
                    .str.replace("\x00", "", regex=False)
                    .str.replace(r"[\r\n\t]+", " ", regex=True)
                    .str.strip()
                )
            return detail_df

        return empty_detail

    def _build_outages(self, detail_df: pd.DataFrame, notices_df: pd.DataFrame) -> pd.DataFrame:
        """Transform enriched detail rows into planned_outages records."""
        if detail_df.empty:
            return pd.DataFrame(columns=OUTAGE_COLUMNS)

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rows = []

        for _, d in detail_df.iterrows():
            # Look up the original notice for category/severity/subject
            key = (d["source_family"], d["pipeline_name"], d["notice_identifier"])
            match = notices_df[
                (notices_df["source_family"] == key[0])
                & (notices_df["pipeline_name"] == key[1])
                & (notices_df["notice_identifier"] == key[2])
            ]

            if match.empty:
                continue
            notice = match.iloc[0]

            start = str(d.get("gas_day_start", ""))
            end = str(d.get("gas_day_end", ""))

            # Compute status
            if start and end:
                if end < today:
                    status = "COMPLETED"
                elif start <= today:
                    status = "ACTIVE"
                else:
                    status = "UPCOMING"
            elif start:
                status = "ACTIVE" if start <= today else "UPCOMING"
            else:
                status = "UPCOMING"

            cap = d.get("capacity_bcfd", 0.0)
            try:
                cap = float(cap) if cap else 0.0
            except (ValueError, TypeError):
                cap = 0.0

            rows.append({
                "source_family": d["source_family"],
                "pipeline_name": d["pipeline_name"],
                "notice_identifier": d["notice_identifier"],
                "location": str(d.get("affected_locations", "")),
                "sub_region": "",
                "start_date": start,
                "end_date": end,
                "capacity_loss_bcfd": cap,
                "outage_type": str(notice.get("notice_category", "")),
                "status": status,
                "subject": str(notice.get("subject", "")),
                "notice_category": str(notice.get("notice_category", "")),
                "severity": int(notice.get("severity", 0)),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })

        if not rows:
            return pd.DataFrame(columns=OUTAGE_COLUMNS)

        out_df = pd.DataFrame(rows)[OUTAGE_COLUMNS]
        for col in out_df.columns:
            if col == "severity":
                out_df[col] = out_df[col].fillna(0).astype(int)
            elif col == "capacity_loss_bcfd":
                out_df[col] = out_df[col].fillna(0.0)
            else:
                out_df[col] = out_df[col].fillna("").astype(str)
        return out_df

    def _upsert_details(self, detail_df: pd.DataFrame) -> None:
        """Upsert enriched detail rows to gas_ebbs.notice_details."""
        if detail_df.empty:
            return
        azure_sql.upsert_to_azure_sql(
            schema=SCHEMA,
            table_name=DETAIL_TABLE,
            df=detail_df,
            columns=DETAIL_COLUMNS,
            data_types=DETAIL_DATA_TYPES,
            primary_key=DETAIL_PRIMARY_KEY,
        )

    def _upsert_outages(self, outage_df: pd.DataFrame) -> None:
        """Upsert planned outage rows to gas_ebbs.planned_outages."""
        if outage_df.empty:
            return
        azure_sql.upsert_to_azure_sql(
            schema=SCHEMA,
            table_name=OUTAGE_TABLE,
            df=outage_df,
            columns=OUTAGE_COLUMNS,
            data_types=OUTAGE_DATA_TYPES,
            primary_key=OUTAGE_PRIMARY_KEY,
        )

    # ── Main pipeline ──────────────────────────────────────────────────

    def main(self) -> list[dict]:
        """Full scrape pipeline: pull -> parse -> format -> classify -> upsert -> enrich.

        Returns the list of raw notice dicts (before formatting).
        """
        api_scrape_name = f"gas_ebb_v2_{self.pipeline_name}"
        logger = logging_utils.PipelineLogger(
            name=api_scrape_name,
            log_dir=LOG_DIR,
            log_to_file=True,
            delete_if_no_errors=True,
        )

        run = pipeline_run_logger.PipelineRunLogger(
            pipeline_name=api_scrape_name,
            source="gas_ebbs_v2",
            target_table=f"{SCHEMA}.{TABLE}",
            operation_type="upsert",
            log_file_path=logger.log_file_path,
        )
        run.start()

        try:
            logger.header(api_scrape_name)

            # ── Phase 1: Listing scrape ──
            listing_sources = self._get_listing_sources()
            raw_notices = []

            for source in listing_sources:
                url = source["url"]
                context = {k: v for k, v in source.items() if k != "url"}
                label = context.get("label", url)

                logger.section(f"Pulling: {label}")
                logger.info(f"URL: {url}")
                html = self._pull(url)
                logger.info(f"Fetched {len(html):,} bytes")

                self._archive_html(html, "listing")

                notices = self._parse_listing(html, **context)
                logger.info(f"Parsed {len(notices)} notices")
                raw_notices.extend(notices)

            if not raw_notices:
                logger.warning("No notices found. Page structure may have changed.")
                run.success(rows_processed=0)
                return []

            # ── Phase 2: Format + classify ──
            logger.section("Formatting and Classifying")
            df = self._format(raw_notices)
            logger.info(f"Formatted {len(df)} notices")

            if not df.empty:
                counts = df["notice_category"].value_counts().to_dict()
                for cat, count in counts.items():
                    logger.info(f"  {cat}: {count}")

            # ── Phase 3: Upsert notices (always runs) ──
            logger.section("Upserting to Azure SQL")
            self._upsert(df)
            logger.success(
                f"Upserted {len(df)} notices to {SCHEMA}.{TABLE}"
            )

            # ── Phase 4: Detail enrichment (non-fatal) ──
            try:
                logger.section("Detail Enrichment")
                detail_df = self._fetch_details(df, logger)

                if not detail_df.empty:
                    self._upsert_details(detail_df)
                    logger.info(
                        f"Upserted {len(detail_df)} detail rows to "
                        f"{SCHEMA}.{DETAIL_TABLE}"
                    )

                    outage_df = self._build_outages(detail_df, df)
                    if not outage_df.empty:
                        self._upsert_outages(outage_df)
                        logger.info(
                            f"Upserted {len(outage_df)} outage rows to "
                            f"{SCHEMA}.{OUTAGE_TABLE}"
                        )
                    else:
                        logger.info("No outage rows to upsert")
                else:
                    logger.info("No detail rows to upsert")
            except Exception as e:
                logger.warning(f"Detail enrichment failed (non-fatal): {e}")

            run.success(rows_processed=len(df))
            return raw_notices

        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            run.failure(error=e)
            raise

        finally:
            logger.close()


# ── Adapter registry + factory ─────────────────────────────────────────────

ADAPTER_REGISTRY: dict[str, type[EBBScraper]] = {}


def register_adapter(source_family: str):
    """Decorator to register an adapter class for a source family."""

    def decorator(cls: type[EBBScraper]):
        ADAPTER_REGISTRY[source_family] = cls
        return cls

    return decorator


def _ensure_adapters_loaded():
    """Import adapter modules so they register themselves."""
    if ADAPTER_REGISTRY:
        return
    # Import all adapter modules — each uses @register_adapter
    from backend.scrapes.gas_ebbs_v2.adapters import piperiv_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import enbridge_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import kindermorgan_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import williams_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import energytransfer_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import tce_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import tcplus_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import quorum_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import bhegts_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import northern_natural_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import dtmidstream_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import gasnom_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import tallgrass_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import cheniere_adapter  # noqa: F401
    from backend.scrapes.gas_ebbs_v2.adapters import standalone_adapter  # noqa: F401


CONFIG_DIR = Path(__file__).parent / "config"


def load_family_config(config_path: Path) -> dict:
    """Load and return a source-family YAML config."""
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _resolve_pipeline_config(family_config: dict, pipeline_name: str) -> dict:
    """Merge family defaults with pipeline-specific overrides."""
    defaults = family_config.get("defaults", {})
    pipeline_overrides = family_config["pipelines"][pipeline_name]

    config = {**defaults, **pipeline_overrides}
    config["source_family"] = family_config["source_family"]
    config["pipeline_name"] = pipeline_name

    # Render URL templates
    if "listing_url_template" in config and "listing_url" not in config:
        config["listing_url"] = config["listing_url_template"].format(**config)
    if "detail_url_template" in config:
        # Keep the template — it gets rendered per-notice at parse time
        pass

    return config


def create_scraper(source_family: str, pipeline_name: str) -> EBBScraper:
    """Load YAML config and return the correct adapter instance.

    Args:
        source_family: e.g. "piperiv", "kindermorgan", "williams"
        pipeline_name: e.g. "algonquin", "tgp", "transco"
    """
    _ensure_adapters_loaded()

    # Find the YAML config for this family
    config_path = CONFIG_DIR / f"{source_family}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"No config found: {config_path}")

    family_config = load_family_config(config_path)
    if pipeline_name not in family_config.get("pipelines", {}):
        available = list(family_config.get("pipelines", {}).keys())
        raise ValueError(
            f"Pipeline '{pipeline_name}' not found in {source_family}.yaml. "
            f"Available: {available}"
        )

    config = _resolve_pipeline_config(family_config, pipeline_name)

    adapter_cls = ADAPTER_REGISTRY.get(source_family)
    if adapter_cls is None:
        raise ValueError(
            f"No adapter registered for '{source_family}'. "
            f"Available: {list(ADAPTER_REGISTRY.keys())}"
        )

    return adapter_cls(
        pipeline_name=pipeline_name,
        source_family=source_family,
        listing_url=config["listing_url"],
        detail_url_template=config.get("detail_url_template", ""),
        datetime_formats=config.get("datetime_formats", []),
        config=config,
    )


def discover_all_pipelines() -> list[tuple[str, str, Path]]:
    """Discover all configured pipelines across all YAML configs.

    Skips pipelines with ``disabled: true`` in their config.

    Returns a sorted list of (pipeline_name, source_family, config_path) tuples.
    """
    pipelines = []
    for yaml_file in sorted(CONFIG_DIR.glob("*.yaml")):
        family_config = load_family_config(yaml_file)
        source_family = family_config["source_family"]
        for name, pconfig in sorted(family_config.get("pipelines", {}).items()):
            if isinstance(pconfig, dict) and pconfig.get("disabled"):
                continue
            pipelines.append((name, source_family, yaml_file))
    return pipelines
