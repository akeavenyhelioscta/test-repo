"""Preflight data-freshness checks for forward-only KNN.

Runs before a forecast to surface missing query-side inputs and thin pools.
Returns a structured report; does not raise. Pipeline decides how to react.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from da_models.forward_only_knn import configs

logger = logging.getLogger(__name__)


@dataclass
class PreflightReport:
    target_date: date
    pool_rows: int
    pool_ok: bool
    query_group_coverage: dict[str, float] = field(default_factory=dict)
    pool_group_coverage: dict[str, float] = field(default_factory=dict)
    missing_query_groups: list[str] = field(default_factory=list)
    low_pool_groups: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def ok(self, min_pool_size: int = configs.MIN_POOL_SIZE) -> bool:
        return self.pool_ok and self.pool_rows >= min_pool_size

    def as_dict(self) -> dict:
        return {
            "target_date": str(self.target_date),
            "pool_rows": self.pool_rows,
            "pool_ok": self.pool_ok,
            "query_group_coverage": self.query_group_coverage,
            "pool_group_coverage": self.pool_group_coverage,
            "missing_query_groups": list(self.missing_query_groups),
            "low_pool_groups": list(self.low_pool_groups),
            "warnings": list(self.warnings),
        }


def _group_coverage(query: pd.Series, feature_cols: list[str]) -> float:
    """Fraction of non-null feature values in this group on the query row."""
    if not feature_cols:
        return 1.0
    total = len(feature_cols)
    non_null = 0
    for col in feature_cols:
        val = query.get(col) if col in query.index else None
        if val is not None and not pd.isna(val):
            non_null += 1
    return non_null / total


def _pool_group_coverage(pool: pd.DataFrame, feature_cols: list[str]) -> float:
    """Fraction of non-null values for this group across pool rows and group columns."""
    if not feature_cols:
        return 1.0
    if len(pool) == 0:
        return 0.0

    present_cols = [c for c in feature_cols if c in pool.columns]
    if not present_cols:
        return 0.0

    non_null_cells = int(pool[present_cols].notna().to_numpy().sum())
    # Missing columns count as fully null for coverage purposes.
    total_cells = len(pool) * len(feature_cols)
    if total_cells <= 0:
        return 0.0
    return non_null_cells / float(total_cells)


def run_preflight(
    query: pd.Series,
    pool: pd.DataFrame,
    target_date: date,
    feature_weights: dict[str, float] | None = None,
    min_pool_size: int = configs.MIN_POOL_SIZE,
    min_group_coverage: float = 0.5,
    min_pool_group_coverage: float = 0.1,
) -> PreflightReport:
    """Build a preflight report for a forecast run.

    - Checks pool size meets min_pool_size.
    - Computes per-group coverage on the query row.
    - Flags groups with weight > 0 whose query coverage < min_group_coverage.
    - Flags groups with weight > 0 whose pool coverage < min_pool_group_coverage.
    """
    weights = feature_weights or configs.FEATURE_GROUP_WEIGHTS
    report = PreflightReport(
        target_date=target_date,
        pool_rows=len(pool),
        pool_ok=len(pool) >= min_pool_size,
    )

    if not report.pool_ok:
        report.warnings.append(
            f"Pool has {len(pool)} rows, below min_pool_size={min_pool_size}."
        )

    for group_name, group_cols in configs.FEATURE_GROUPS.items():
        weight = float(weights.get(group_name, 0.0))
        if weight <= 0:
            continue
        coverage = _group_coverage(query, group_cols)
        pool_cov = _pool_group_coverage(pool, group_cols)
        report.query_group_coverage[group_name] = coverage
        report.pool_group_coverage[group_name] = pool_cov
        if coverage < min_group_coverage:
            report.missing_query_groups.append(group_name)
            report.warnings.append(
                f"Group '{group_name}' query coverage {coverage:.0%} < "
                f"{min_group_coverage:.0%}; will contribute little to distance."
            )
        if pool_cov < min_pool_group_coverage:
            report.low_pool_groups.append(group_name)
            report.warnings.append(
                f"Group '{group_name}' pool coverage {pool_cov:.0%} < "
                f"{min_pool_group_coverage:.0%}; low-signal group will be down-weighted."
            )

    for msg in report.warnings:
        logger.warning("Preflight: %s", msg)

    logger.info(
        "Preflight: pool_rows=%s pool_ok=%s missing_query=%s low_pool=%s",
        report.pool_rows,
        report.pool_ok,
        report.missing_query_groups,
        report.low_pool_groups,
    )
    return report
