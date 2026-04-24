"""Calendar filtering and fallback ladder for forward-only KNN."""
from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _holiday_mask(pool: pd.DataFrame, target_is_holiday: bool) -> pd.Series:
    """Mask for holiday matching."""
    if "is_nerc_holiday" not in pool.columns:
        return pd.Series([True] * len(pool), index=pool.index)
    if target_is_holiday:
        return pool["is_nerc_holiday"] == 1
    return pool["is_nerc_holiday"] != 1


def _exact_dow_mask(pool: pd.DataFrame, target_dow: int) -> pd.Series:
    """Mask for exact day-of-week matching (Sun=0..Sat=6)."""
    if "day_of_week_number" not in pool.columns:
        return pd.Series([True] * len(pool), index=pool.index)
    return pool["day_of_week_number"] == int(target_dow)


def _dow_group_mask(pool: pd.DataFrame, target_dow_group: int) -> pd.Series:
    """Mask for day-of-week group matching (weekday/sat/sun)."""
    if "dow_group" not in pool.columns:
        return pd.Series([True] * len(pool), index=pool.index)
    return pool["dow_group"] == int(target_dow_group)


def apply_filter_ladder(
    pool: pd.DataFrame,
    target_dow: int,
    target_dow_group: int,
    target_is_holiday: bool,
    min_pool_size: int,
    same_dow_group: bool = True,
    exclude_holidays: bool = True,
) -> pd.DataFrame:
    """Apply strict-to-relaxed calendar filtering until minimum pool size is met."""
    base = pool.copy()

    holiday_mask = _holiday_mask(base, target_is_holiday) if exclude_holidays else pd.Series(
        [True] * len(base), index=base.index,
    )

    exact_dow = _exact_dow_mask(base, target_dow)
    group_dow = _dow_group_mask(base, target_dow_group) if same_dow_group else pd.Series(
        [True] * len(base), index=base.index,
    )

    candidates: list[tuple[str, pd.DataFrame]] = [
        ("exact_dow+holiday", base[exact_dow & holiday_mask]),
        ("exact_dow_only", base[exact_dow]),
        ("dow_group+holiday", base[group_dow & holiday_mask]),
        ("dow_group_only", base[group_dow]),
        ("no_calendar_filter", base),
    ]

    for stage, frame in candidates:
        if len(frame) >= min_pool_size:
            logger.info(
                "Calendar filter stage '%s' accepted (%s rows, min=%s)",
                stage,
                len(frame),
                min_pool_size,
            )
            return frame

    logger.warning(
        "Pool remains below minimum after fallback ladder (%s rows, min=%s)",
        len(base),
        min_pool_size,
    )
    return base
