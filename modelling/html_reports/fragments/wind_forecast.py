"""Wind forecast — latest PJM (RTO only) + Meteologica (all regions)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from da_models.common import configs
from da_models.common.data.loader import (
    load_meteologica_wind_forecast,
    load_wind_forecast,
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
    """PJM RTO + Meteologica regional wind charts."""
    logger.info("Building wind forecast fragments...")
    cache_dir = cache_dir or configs.CACHE_DIR

    pjm_df = _safe_load(load_wind_forecast, cache_dir, "PJM wind forecast")
    meteo_df = _safe_load(load_meteologica_wind_forecast, cache_dir, "Meteologica wind forecast")

    if pjm_df.empty and meteo_df.empty:
        return [("Wind Forecast — Unavailable",
                 empty_html("Neither PJM nor Meteologica wind forecast data available."), None)]

    pjm_df = prep_hours(pjm_df)
    meteo_df = prep_hours(meteo_df)

    sections: list[Section] = []

    sections.append("RTO")
    if pjm_df.empty:
        sections.append(("PJM RTO", empty_html("No PJM wind forecast."), None))
    else:
        sections.append((
            "PJM RTO",
            latest_line_with_ramp(
                pjm_df, value_col="wind_forecast",
                title="PJM Wind — RTO",
                div_id="wind-pjm-rto",
                color=COLORS["wind"],
            ),
            None,
        ))

    for region_key, region_label in REGIONS:
        if region_key != "RTO":
            sections.append(region_label)
        sub = meteo_df[meteo_df["region"] == region_key] if not meteo_df.empty else pd.DataFrame()
        if sub.empty:
            sections.append((f"Meteologica {region_label}",
                             empty_html(f"No Meteologica wind for {region_label}."), None))
        else:
            sections.append((
                f"Meteologica {region_label}",
                latest_line_with_ramp(
                    sub, value_col="wind_forecast",
                    title=f"Meteologica Wind — {region_label}",
                    div_id=f"wind-meteo-{region_key.lower()}",
                    color=COLORS["wind"],
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
