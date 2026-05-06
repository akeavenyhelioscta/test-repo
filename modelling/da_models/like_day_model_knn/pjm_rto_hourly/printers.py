"""Terminal printers for the pjm_rto_hourly forecast.

Mirrors the ``_print_*`` helpers in
helioscta-pjm-da/backend/src/like_day_forecast/pipelines/forecast.py so a
side-by-side run produces visually-comparable terminal output.

Adapted to the per-hour KNN engine's output shape: analogs are emitted
per (hour_ending, rank) pair, so the analogs table aggregates per-date
(n_hours appeared, mean distance, summed weight).
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
from tabulate import tabulate

from da_models.like_day_model_knn.configs import KnnModelConfig, ModelSpec
from utils.logging_utils import (
    Colors,
    print_divider,
    print_header,
    print_section,
    supports_color,
)

# Enable ANSI on Windows terminals once at import time. supports_color()
# calls SetConsoleMode on win32 with side-effects we want to trigger here
# so the ANSI escapes inside _HL_* render correctly without colorama.
_COLOR_ON = supports_color()
_HL_FORECAST = (Colors.BOLD + Colors.BRIGHT_RED) if _COLOR_ON else ""
_HL_QUARTILE = Colors.CYAN if _COLOR_ON else ""  # P25 / P75
_HL_INNER = Colors.YELLOW if _COLOR_ON else ""  # P37.5 / P62.5
_HL_UP = Colors.BRIGHT_GREEN if _COLOR_ON else ""  # analog feature > target
_HL_DOWN = Colors.BRIGHT_RED if _COLOR_ON else ""  # analog feature < target
_RS = Colors.RESET if _COLOR_ON else ""
_ROW_STYLES: dict[str, str] = {
    "Forecast": _HL_FORECAST,
    "P25": _HL_QUARTILE,
    "P75": _HL_QUARTILE,
    "P37.5": _HL_INNER,
    "P62.5": _HL_INNER,
}

DAY_ABBR: dict[int, str] = {
    0: "Mon",
    1: "Tue",
    2: "Wed",
    3: "Thu",
    4: "Fri",
    5: "Sat",
    6: "Sun",
}


def quantile_label(q: float) -> str:
    """Format quantile label (P25, P37.5, P90, ...)."""
    q_pct = q * 100
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")


def print_config(
    config: KnnModelConfig,
    spec: ModelSpec,
    target_date: date,
    day_type: str,
) -> None:
    """Forecast configuration block (90-char banner)."""
    target_dow = DAY_ABBR[target_date.weekday()]
    weights = spec.feature_group_weights

    window = config.season_window_days
    win_start = target_date - timedelta(days=window)
    win_end = target_date + timedelta(days=window)

    print_header("FORECAST CONFIGURATION", "=", 90)

    print(f"\n  Target        {target_date} ({target_dow})")
    print(f"  Day-type      {day_type}")
    print(f"  Hub           {config.hub}")
    print(f"  Spec          {spec.name}")
    print(f"  Description   {spec.description}")

    half_life_days = float(getattr(config, "recency_half_life_days", 0.0) or 0.0)
    if half_life_days > 0:
        weight_method = (
            "inverse_distance (analog weights) + linear pre-selection age "
            f"penalty (half-life={half_life_days:g} days)"
        )
    else:
        weight_method = "inverse_distance"

    print_section("Analog Selection")
    print(f"  N analogs          {config.n_analogs}")
    print(f"  Weight method      {weight_method}")
    print(f"  flt_radius         {spec.flt_radius}")

    print_section("Pre-Filtering")
    print(
        f"  Season window      +/-{window}d  "
        f"({win_start.strftime('%b %d')} - {win_end.strftime('%b %d')})"
    )
    print(f"  DOW group filter   {config.same_dow_group}")
    print(
        f"  Weekend filter     same_weekend_group={config.same_weekend_group}  "
        f"for_weekends={config.same_weekend_group_for_weekends}"
    )
    print(f"  Exclude holidays   {config.exclude_holidays}")
    if config.exclude_dates:
        print(f"  Exclude dates      {', '.join(config.exclude_dates)}")
    print(f"  Min pool size      {config.min_pool_size}")

    print_section("Recency")
    print(f"  Max age years      {config.max_age_years}")
    print(f"  Half-life days     {half_life_days:g}")
    print(f"  Label source       {getattr(config, 'label_source', 'hub_lmp')}")

    from da_models.like_day_model_knn import configs as _configs_module
    from da_models.like_day_model_knn.domains import feature_group_weight_locations

    raw_weights = spec.raw_feature_group_weights
    active = {k: v for k, v in sorted(weights.items()) if v > 0}
    disabled = [k for k, v in sorted(weights.items()) if v == 0]
    raw_total = sum(raw_weights.get(k, 0.0) for k in active)
    locations = feature_group_weight_locations()

    print_section("Feature Group Weights")
    print(f"  Spec: {_configs_module.__file__}")
    loc_strs = {
        k: f"{locations[k][0]}:{locations[k][1]}" for k in active if k in locations
    }
    loc_w = max((len(s) for s in loc_strs.values()), default=0)
    bar_w = max((int(w * 40) for w in active.values()), default=0)
    print(
        f"  {'group':<32} {'raw':>6} {'norm':>6}  "
        f"{'bar':<{bar_w}}  {'defined at':<{loc_w}}"
    )
    for name, w in sorted(active.items(), key=lambda x: -x[1]):
        bar = "#" * int(w * 40)
        raw = raw_weights.get(name, 0.0)
        loc_str = loc_strs.get(name, "—")
        print(f"  {name:<32} {raw:>6.3f} {w:>6.3f}  {bar:<{bar_w}}  {loc_str:<{loc_w}}")
    print(f"  {'(sum)':<32} {raw_total:>6.3f} {1.0:>6.3f}")

    if disabled:
        print_section("Disabled Groups")
        print(f"  {', '.join(disabled)}")

    print()
    print_divider("=", 90, dim=False)


def print_pool_funnel(
    funnel,
    target_date: date,
    day_type: str,
    hub: str,
) -> None:
    """Render the candidate-pool funnel before distance computation.

    Shows each filter stage that actually fired (raw → chronological →
    excludes → recency → holiday → DOW → season), with surviving and
    dropped counts. Relaxed filters (those that would have pushed the
    pool below ``min_pool_size`` and were therefore reverted) are
    flagged so the reader can see the fall-back.
    """
    target_dow = DAY_ABBR[target_date.weekday()]
    print_header("POOL FUNNEL", "=", 110)
    print(
        f"  Forecast: {target_date} ({target_dow})  |  Day-type: {day_type}  "
        f"|  Hub: {hub}"
    )
    print_divider("=", 110, dim=False)

    if funnel is None or not funnel.stages:
        print("\n  (no funnel records)")
        return

    raw = funnel.initial
    final = funnel.final

    print()
    print(
        f"  {'Stage':<5}  {'Filter':<24}  {'Detail':<46}  "
        f"{'Survives':>9}  {'Dropped':>9}  {'Cumul %':>8}"
    )
    print("  " + "─" * 108)
    for i, st in enumerate(funnel.stages):
        cum_pct = (st.survives / raw * 100.0) if raw > 0 else 0.0
        if st.relaxed:
            wd = st.would_survive if st.would_survive is not None else 0
            survives_str = (
                f"{_HL_DOWN}{st.survives:>9,}{_RS}"
                if _COLOR_ON
                else f"{st.survives:>9,}"
            )
            dropped_str = f"{'(relaxed)':>9}"
            detail_str = f"{st.detail}  → would yield {wd:,} (< min, kept full)"
        else:
            survives_str = f"{st.survives:>9,}"
            dropped_str = f"{-st.dropped:>+9,}" if st.dropped > 0 else f"{0:>9}"
            detail_str = st.detail
        print(
            f"  {i:<5}  {st.name:<24}  {detail_str[:46]:<46}  "
            f"{survives_str}  {dropped_str}  {cum_pct:>7.1f}%"
        )
    print("  " + "─" * 108)

    overall_drop = raw - final
    overall_pct = (final / raw * 100.0) if raw > 0 else 0.0
    final_color = f"{Colors.BOLD}{Colors.BRIGHT_GREEN}" if _COLOR_ON else ""
    print(
        f"  → {final_color}{final:,}{_RS} candidates feed find_twins  "
        f"|  {overall_drop:,} dropped overall ({overall_pct:.1f}% retained)"
    )

    print()
    print_divider("=", 110, dim=False)


def print_analogs(analogs: pd.DataFrame, target_date: date, hub: str) -> None:
    """Top analog days table.

    The pjm_rto_hourly engine emits one analog row per (hour_ending, rank);
    we aggregate to per-date for display: count of HEs the date appears
    in, summed weight, mean distance.
    """
    target_dow = DAY_ABBR[target_date.weekday()]
    print("\n" + "=" * 90)
    print("  LIKE-DAY ANALOG DAYS (aggregated across HEs)")
    print(f"  Forecast: {target_date} ({target_dow})  |  Hub: {hub}")
    print("=" * 90)

    if analogs is None or len(analogs) == 0:
        print("\n  (no analogs returned)")
        return

    by_date = (
        analogs.groupby("date", as_index=False)
        .agg(
            n_hours=("hour_ending", "nunique"),
            mean_distance=("distance", "mean"),
            summed_weight=("weight", "sum"),
        )
        .sort_values("summed_weight", ascending=False)
        .reset_index(drop=True)
    )

    n_show = min(len(by_date), 20)
    display = by_date.head(n_show).copy()
    display.insert(0, "rank", range(1, len(display) + 1))
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%a %b-%d %Y")
    display["mean_distance"] = display["mean_distance"].map("{:.4f}".format)
    display["summed_weight"] = display["summed_weight"].map("{:.4f}".format)

    print()
    print(tabulate(display, headers="keys", tablefmt="simple", showindex=False))
    total_w = float(by_date["summed_weight"].sum())
    top5_w = float(by_date.head(5)["summed_weight"].sum())
    print(
        f"\n  Total unique dates: {len(by_date)} | "
        f"Total weight: {total_w:.2f} | "
        f"Top-5 date weight: {top5_w / total_w:.2%} | "
        f"Distance range: {analogs['distance'].min():.4f} — {analogs['distance'].max():.4f}"
    )


def print_forecast(table: pd.DataFrame, metrics: dict | None) -> None:
    """Actual / Forecast / Error table (120-char banner) + metrics block."""
    print_header("DA LMP LIKE-DAY FORECAST — Western Hub ($/MWh)", "=", 120)

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            line += f" {val:>7.2f}" if pd.notna(val) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header))

    if metrics:
        if {"mae", "rmse", "mape"}.issubset(metrics.keys()):
            print(
                f"  MAE: ${metrics['mae']:.2f}/MWh  |  "
                f"RMSE: ${metrics['rmse']:.2f}/MWh  |  "
                f"MAPE: {metrics['mape']:.1f}%"
            )
        if "rmae" in metrics:
            verdict = "better" if metrics["rmae"] < 1 else "worse"
            print(
                f"  rMAE vs naive (last week): {metrics['rmae']:.3f} "
                f"({verdict} than naive)"
            )
        cov_parts: list[str] = []
        for label, key in (
            ("80%PI", "coverage_80pct"),
            ("90%PI", "coverage_90pct"),
            ("98%PI", "coverage_98pct"),
        ):
            if metrics.get(key) is not None:
                cov_parts.append(f"{label}={metrics[key]:.0%}")
        if cov_parts:
            print(f"  Coverage: {' | '.join(cov_parts)}")
        if metrics.get("sharpness_90pct") is not None:
            print(f"  Sharpness (90%PI width): ${metrics['sharpness_90pct']:.2f}/MWh")
        if "mean_pinball" in metrics:
            print(f"  Mean Pinball Loss: {metrics['mean_pinball']:.4f}")
        if "crps" in metrics:
            print(f"  CRPS: {metrics['crps']:.4f}")

    print()
    print_divider("=", 120, dim=False)
    print()


def print_quantiles(table: pd.DataFrame) -> None:
    """Quantile bands table (P25 / P37.5 / P50 / Forecast / P62.5 / P75)."""
    print("  Quantile Bands ($/MWh)")
    print("-" * 100)

    header = f"{'Date':<12} {'Band':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            line += f" {val:>7.2f}" if pd.notna(val) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header) + "\n")
