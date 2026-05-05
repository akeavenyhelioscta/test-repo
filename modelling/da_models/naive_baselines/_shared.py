"""Thin LMP-only pool builder for the naive baselines.

The naive baselines need exactly one input: a wide DataFrame with one
row per delivery date and ``lmp_h1..lmp_h24`` columns at the configured
hub. This module wraps the shared pool primitives in
``common.data.lmp_pool`` so the naive family stays decoupled from the
like-day spec / domain stack.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from da_models.common.configs import CACHE_DIR as DEFAULT_CACHE_DIR
from da_models.common.data.lmp_pool import (
    LMP_HOUR_COLUMNS,
    build_lmp_labels,
    load_lmp_da,
)

logger = logging.getLogger(__name__)


DEFAULT_HUB: str = "WESTERN HUB"


def build_lmp_only_pool(
    hub: str = DEFAULT_HUB,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Load DA LMPs for ``hub`` and pivot to wide ``lmp_h1..lmp_h24`` by date.

    Returns one row per delivery date, sorted ascending. Columns:
    ``date`` plus the 24 hourly LMP columns. Hub defaults to PJM Western
    Hub to keep leaderboard numbers comparable with the like-day model.
    """
    resolved_cache = cache_dir if cache_dir is not None else DEFAULT_CACHE_DIR
    df_lmp_da = load_lmp_da(resolved_cache)

    pool = build_lmp_labels(df_lmp_da, hub)
    pool = pool.sort_values("date").reset_index(drop=True)

    n_with_labels = int(pool[LMP_HOUR_COLUMNS].notna().any(axis=1).sum())
    logger.info(
        "naive_baselines pool: %d rows for hub=%r (%d with labels)",
        len(pool), hub, n_with_labels,
    )
    return pool
