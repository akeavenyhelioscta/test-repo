"""Single-day forward-only KNN forecast pipeline."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.forward_only_knn import configs
from da_models.forward_only_knn.features.builder import build_pool, build_query_row
from da_models.forward_only_knn.similarity.engine import find_twins
from da_models.forward_only_knn.validation.preflight import run_preflight

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    """Weighted quantile via cumulative interpolation."""
    idx = np.argsort(values)
    v = values[idx]
    w = weights[idx]
    cdf = np.cumsum(w)
    cdf = cdf / cdf[-1]
    return float(np.interp(q, cdf, v))


def _quantile_label(q: float) -> str:
    q_pct = q * 100.0
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")


def _add_summary_cols(row_dict: dict) -> dict:
    on_vals = [row_dict.get(f"HE{h}") for h in ONPEAK_HOURS]
    off_vals = [row_dict.get(f"HE{h}") for h in OFFPEAK_HOURS]
    flat_vals = [row_dict.get(f"HE{h}") for h in configs.HOURS]

    on_vals = [v for v in on_vals if v is not None and not pd.isna(v)]
    off_vals = [v for v in off_vals if v is not None and not pd.isna(v)]
    flat_vals = [v for v in flat_vals if v is not None and not pd.isna(v)]

    row_dict["OnPeak"] = float(np.mean(on_vals)) if on_vals else np.nan
    row_dict["OffPeak"] = float(np.mean(off_vals)) if off_vals else np.nan
    row_dict["Flat"] = float(np.mean(flat_vals)) if flat_vals else np.nan
    return row_dict


def _build_output_table(
    target_date: date,
    forecast_hourly: dict[int, float],
    actual_hourly: dict[int, float] | None,
) -> pd.DataFrame:
    rows: list[dict] = []

    if actual_hourly is not None:
        actual_row = {"Date": target_date, "Type": "Actual"}
        for h in configs.HOURS:
            actual_row[f"HE{h}"] = actual_hourly.get(h)
        rows.append(_add_summary_cols(actual_row))

    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for h in configs.HOURS:
        forecast_row[f"HE{h}"] = forecast_hourly.get(h)
    rows.append(_add_summary_cols(forecast_row))

    if actual_hourly is not None:
        error_row = {"Date": target_date, "Type": "Error"}
        for h in configs.HOURS:
            a = actual_hourly.get(h)
            f = forecast_hourly.get(h)
            error_row[f"HE{h}"] = (f - a) if (a is not None and f is not None) else None
        rows.append(_add_summary_cols(error_row))

    cols = ["Date", "Type"] + [f"HE{h}" for h in configs.HOURS] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _actuals_from_pool(pool: pd.DataFrame, target_date: date) -> dict[int, float] | None:
    row = pool[pool["date"] == target_date]
    if len(row) == 0:
        return None
    rec = row.iloc[0]
    actuals: dict[int, float] = {}
    for h in configs.HOURS:
        val = rec.get(f"lmp_h{h}")
        if val is None or pd.isna(val):
            return None
        actuals[h] = float(val)
    return actuals


def _season_window_filter_for_preflight(
    pool: pd.DataFrame,
    target_date: date,
    season_window_days: int,
) -> pd.DataFrame:
    """Mirror season-window filtering used by analog selection for coverage checks."""
    hist = pool[pd.to_datetime(pool["date"]).dt.date < target_date].copy()
    if season_window_days <= 0 or len(hist) == 0:
        return hist

    target_doy = pd.Timestamp(target_date).dayofyear
    day_of_year = pd.to_datetime(hist["date"]).dt.dayofyear.to_numpy(dtype=float)
    direct = np.abs(day_of_year - float(target_doy))
    circular = np.minimum(direct, 366.0 - direct)
    return hist[circular <= float(season_window_days)].copy()


def _derive_effective_weights(
    base_weights: dict[str, float],
    missing_query_groups: list[str],
    low_pool_groups: list[str],
) -> tuple[dict[str, float], list[str]]:
    """Zero weak groups based on preflight coverage checks."""
    effective = dict(base_weights)
    disabled = sorted(set(missing_query_groups) | set(low_pool_groups))
    for group in disabled:
        if group in effective:
            effective[group] = 0.0

    if not any(float(weight) > 0 for weight in effective.values()):
        effective["calendar_dow"] = max(float(base_weights.get("calendar_dow", 0.0)), 1.0)
        disabled = sorted(set(disabled) - {"calendar_dow"})

    return effective, disabled


def _hourly_forecast_from_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    rows: list[dict] = []
    for h in configs.HOURS:
        col = f"lmp_h{h}"
        if col not in analogs.columns:
            continue
        hour = analogs[["weight", col]].dropna(subset=[col]).copy()
        if len(hour) == 0:
            continue
        values = hour[col].to_numpy(dtype=float)
        weights = hour["weight"].to_numpy(dtype=float)
        weights = weights / weights.sum()

        row = {"hour_ending": h, "point_forecast": float(np.average(values, weights=weights))}
        for q in quantiles:
            row[f"q_{q:.2f}"] = weighted_quantile(values, weights, q)
        rows.append(row)
    return pd.DataFrame(rows)


def run_forecast(
    target_date: date | None = None,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    config: configs.ForwardOnlyKNNConfig | None = None,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> dict:
    """Run D+1 forward-only KNN forecast."""
    if config is None:
        config = configs.ForwardOnlyKNNConfig(n_analogs=n_analogs)

    if target_date is None:
        target_date = config.resolved_target_date()
    else:
        target_date = pd.to_datetime(target_date).date()

    horizon_offset = max((target_date - date.today()).days, 1)
    include_gas = horizon_offset <= config.gas_feature_max_horizon_days
    include_outages = horizon_offset <= config.outage_feature_max_horizon_days
    include_renewables = horizon_offset <= config.renewable_feature_max_horizon_days
    weights = config.resolved_feature_weights(
        include_gas=include_gas,
        include_outages=include_outages,
        include_renewables=include_renewables,
    )

    pool = build_pool(
        schema=config.schema,
        hub=config.hub,
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )
    query = build_query_row(
        target_date=target_date,
        schema=config.schema,
        include_gas=include_gas,
        include_outages=include_outages,
        include_renewables=include_renewables,
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    preflight = run_preflight(
        query=query,
        pool=_season_window_filter_for_preflight(pool, target_date, config.season_window_days),
        target_date=target_date,
        feature_weights=weights,
        min_pool_size=config.min_pool_size,
    )
    effective_weights, disabled_groups = _derive_effective_weights(
        base_weights=weights,
        missing_query_groups=preflight.missing_query_groups,
        low_pool_groups=preflight.low_pool_groups,
    )

    analogs = find_twins(
        query=query,
        pool=pool,
        target_date=target_date,
        n_analogs=config.n_analogs,
        feature_weights=effective_weights,
        min_pool_size=config.min_pool_size,
        same_dow_group=config.same_dow_group,
        exclude_holidays=config.exclude_holidays,
        season_window_days=config.season_window_days,
        recency_half_life_days=config.recency_half_life_days,
        weight_method=config.weight_method,
    )

    quantiles = config.resolved_quantiles()
    df_forecast = _hourly_forecast_from_analogs(analogs, quantiles)
    forecast_hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["point_forecast"]))

    actual_hourly = _actuals_from_pool(pool, target_date)
    output_table = _build_output_table(target_date, forecast_hourly, actual_hourly)

    q_rows: list[dict] = []
    for q in quantiles:
        q_col = f"q_{q:.2f}"
        if q_col not in df_forecast.columns:
            continue
        row = {"Date": target_date, "Type": _quantile_label(q)}
        hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast[q_col]))
        for h in configs.HOURS:
            row[f"HE{h}"] = hourly.get(h)
        q_rows.append(_add_summary_cols(row))

    q_cols = ["Date", "Type"] + [f"HE{h}" for h in configs.HOURS] + ["OnPeak", "OffPeak", "Flat"]
    quantiles_table = pd.DataFrame(q_rows, columns=q_cols)

    forecast_rows = output_table[output_table["Type"] == "Forecast"].iloc[0:1].copy()
    p50_idx = quantiles_table[quantiles_table["Type"] == "P50"].index
    if len(forecast_rows) > 0 and len(p50_idx) > 0:
        pos = int(p50_idx[0]) + 1
        quantiles_table = pd.concat(
            [quantiles_table.iloc[:pos], forecast_rows, quantiles_table.iloc[pos:]],
        ).reset_index(drop=True)

    has_actuals = actual_hourly is not None
    logger.info(
        "Forward-only KNN forecast complete: target=%s analogs=%s has_actuals=%s disabled_groups=%s",
        target_date,
        len(analogs),
        has_actuals,
        disabled_groups,
    )

    preflight_dict = preflight.as_dict()
    preflight_dict["effective_feature_weights"] = effective_weights
    preflight_dict["disabled_feature_groups"] = disabled_groups

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs,
        "metrics": None,
        "forecast_date": str(target_date),
        "reference_date": str(target_date - timedelta(days=1)),
        "has_actuals": has_actuals,
        "n_analogs_used": len(analogs),
        "df_forecast": df_forecast,
        "scenario": "forward_only_knn",
        "preflight": preflight_dict,
    }


def run(*args, **kwargs) -> dict:
    """Backward-compatible alias."""
    return run_forecast(*args, **kwargs)


if __name__ == "__main__":
    result = run_forecast()
    if "error" in result:
        print(result["error"])
    else:
        print(result["output_table"])
