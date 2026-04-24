"""Solar forecast — latest PJM (RTO only) + Meteologica (all regions).

PJM solar is RTO-only; Meteologica provides regional solar for all 4 regions.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from da_models.common import configs
from da_models.common.data.loader import (
    load_meteologica_solar_forecast,
    load_solar_forecast,
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
    """PJM RTO + Meteologica regional solar charts."""
    logger.info("Building solar forecast fragments...")
    cache_dir = cache_dir or configs.CACHE_DIR

    pjm_df = _safe_load(load_solar_forecast, cache_dir, "PJM solar forecast")
    meteo_df = _safe_load(load_meteologica_solar_forecast, cache_dir, "Meteologica solar forecast")

    if pjm_df.empty and meteo_df.empty:
        return [("Solar Forecast — Unavailable",
                 empty_html("Neither PJM nor Meteologica solar forecast data available."), None)]

    pjm_df = prep_hours(pjm_df)
    meteo_df = prep_hours(meteo_df)

    sections: list[Section] = []

    # PJM is RTO-only
    sections.append("RTO")
    if pjm_df.empty:
        sections.append(("PJM RTO", empty_html("No PJM solar forecast."), None))
    else:
        sections.append((
            "PJM RTO",
            latest_line_with_ramp(
                pjm_df, value_col="solar_forecast",
                title="PJM Solar — RTO",
                div_id="solar-pjm-rto",
                color=COLORS["solar"],
            ),
            None,
        ))
        if "solar_forecast_btm" in pjm_df.columns and pjm_df["solar_forecast_btm"].notna().any():
            sections.append((
                "PJM Solar BTM — RTO",
                latest_line_with_ramp(
                    pjm_df, value_col="solar_forecast_btm",
                    title="PJM Solar BTM — RTO",
                    div_id="solar-btm-pjm-rto",
                    color=COLORS["solar_btm"],
                ),
                None,
            ))

    # Meteologica: one per region
    for region_key, region_label in REGIONS:
        if region_key != "RTO":
            sections.append(region_label)
        sub = meteo_df[meteo_df["region"] == region_key] if not meteo_df.empty else pd.DataFrame()
        if sub.empty:
            sections.append((f"Meteologica {region_label}",
                             empty_html(f"No Meteologica solar for {region_label}."), None))
        else:
            sections.append((
                f"Meteologica {region_label}",
                latest_line_with_ramp(
                    sub, value_col="solar_forecast",
                    title=f"Meteologica Solar — {region_label}",
                    div_id=f"solar-meteo-{region_key.lower()}",
                    color=COLORS["solar"],
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
