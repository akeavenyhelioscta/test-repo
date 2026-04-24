"""Load forecast — latest PJM + Meteologica load by region.

Ported from helioscta-pjm-da load_forecast_vintage_combined.py with vintages
dropped. For each of the four load regions (RTO, WEST, MIDATL, SOUTH) we
render a PJM section and a Meteologica section side by side.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from da_models.common import configs
from da_models.common.data.loader import (
    load_load_forecast,
    load_meteologica_load_forecast,
)
from html_reports.fragments._forecast_utils import (
    COLORS, REGIONS, empty_html, latest_line_with_ramp, prep_hours,
)
from utils.logging_utils import get_logger

logger = get_logger()

Section = tuple[str, Any, str | None]


def build_fragments(
    schema: str | None = None,
    cache_dir: Path | None = None,
    cache_enabled: bool = True,
    cache_ttl_hours: float = 24.0,
    force_refresh: bool = False,
) -> list[Section]:
    """One PJM + one Meteologica chart per region."""
    logger.info("Building load forecast fragments...")
    cache_dir = cache_dir or configs.CACHE_DIR

    pjm_df = _safe_load(load_load_forecast, cache_dir, "PJM load forecast")
    meteo_df = _safe_load(load_meteologica_load_forecast, cache_dir, "Meteologica load forecast")

    if pjm_df.empty and meteo_df.empty:
        return [("Load Forecast — Unavailable",
                 empty_html("Neither PJM nor Meteologica load forecast data available."), None)]

    pjm_df = prep_hours(pjm_df)
    meteo_df = prep_hours(meteo_df)

    sections: list[Section] = []
    for region_key, region_label in REGIONS:
        sections.append(f"{region_label}")

        pjm_sub = pjm_df[pjm_df["region"] == region_key] if not pjm_df.empty else pd.DataFrame()
        if pjm_sub.empty:
            sections.append((f"PJM {region_label}", empty_html(f"No PJM load for {region_label}."), None))
        else:
            sections.append((
                f"PJM {region_label}",
                latest_line_with_ramp(
                    pjm_sub, value_col="forecast_load_mw",
                    title=f"PJM Load — {region_label}",
                    div_id=f"load-pjm-{region_key.lower()}",
                    color=COLORS["load"],
                ),
                None,
            ))

        meteo_sub = meteo_df[meteo_df["region"] == region_key] if not meteo_df.empty else pd.DataFrame()
        if meteo_sub.empty:
            sections.append((f"Meteologica {region_label}",
                             empty_html(f"No Meteologica load for {region_label}."), None))
        else:
            sections.append((
                f"Meteologica {region_label}",
                latest_line_with_ramp(
                    meteo_sub, value_col="forecast_load_mw",
                    title=f"Meteologica Load — {region_label}",
                    div_id=f"load-meteo-{region_key.lower()}",
                    color=COLORS["load"],
                ),
                None,
            ))

    return sections


def _safe_load(loader_fn, cache_dir: Path, label: str) -> pd.DataFrame:
    try:
        return loader_fn(cache_dir=cache_dir)
    except FileNotFoundError as exc:
        logger.warning(f"{label} parquet not found: {exc}")
        return pd.DataFrame()
