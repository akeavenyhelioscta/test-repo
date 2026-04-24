"""Multi-day strip forecast for forward-only KNN."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from da_models.forward_only_knn import configs
from da_models.forward_only_knn.features.builder import build_pool, build_query_row
from da_models.forward_only_knn.pipelines.forecast import (
    _add_summary_cols,
    _derive_effective_weights,
    _actuals_from_pool,
    _hourly_forecast_from_analogs,
    _quantile_label,
    _season_window_filter_for_preflight,
)
from da_models.forward_only_knn.similarity.engine import find_twins
from da_models.forward_only_knn.validation.preflight import run_preflight

logger = logging.getLogger(__name__)


def run_strip_forecast(
    horizon: int = 3,
    start_date: date | None = None,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    config: configs.ForwardOnlyKNNConfig | None = None,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> dict:
    """Run D+1..D+N strip with independent query rows per day."""
    if config is None:
        config = configs.ForwardOnlyKNNConfig(n_analogs=n_analogs)

    base = pd.to_datetime(start_date).date() if start_date is not None else date.today()
    forecast_dates = [base + timedelta(days=d) for d in range(1, horizon + 1)]
    quantiles = config.resolved_quantiles()

    pool = build_pool(
        schema=config.schema,
        hub=config.hub,
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    strip_rows: list[dict] = []
    quantile_rows: list[dict] = []
    per_day: dict[str, dict] = {}
    per_day_analogs: dict[str, pd.DataFrame] = {}

    for target_date in forecast_dates:
        offset = (target_date - base).days
        include_gas = offset <= config.gas_feature_max_horizon_days
        include_outages = offset <= config.outage_feature_max_horizon_days
        include_renewables = offset <= config.renewable_feature_max_horizon_days
        weights = config.resolved_feature_weights(
            include_gas=include_gas,
            include_outages=include_outages,
            include_renewables=include_renewables,
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
        per_day_analogs[str(target_date)] = analogs

        df_forecast = _hourly_forecast_from_analogs(analogs, quantiles)
        fc_hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["point_forecast"]))
        act_hourly = _actuals_from_pool(pool, target_date)

        fc_row = {"Date": target_date, "Type": "Forecast"}
        for h in configs.HOURS:
            fc_row[f"HE{h}"] = fc_hourly.get(h)
        strip_rows.append(_add_summary_cols(fc_row))

        if act_hourly is not None:
            act_row = {"Date": target_date, "Type": "Actual"}
            for h in configs.HOURS:
                act_row[f"HE{h}"] = act_hourly.get(h)
            strip_rows.append(_add_summary_cols(act_row))

        for q in quantiles:
            q_col = f"q_{q:.2f}"
            if q_col not in df_forecast.columns:
                continue
            q_row = {"Date": target_date, "Type": _quantile_label(q)}
            q_hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast[q_col]))
            for h in configs.HOURS:
                q_row[f"HE{h}"] = q_hourly.get(h)
            quantile_rows.append(_add_summary_cols(q_row))

        per_day[str(target_date)] = {
            "df_forecast": df_forecast,
            "offset": offset,
            "has_actuals": act_hourly is not None,
            "n_analogs_used": len(analogs),
            "analogs": analogs,
            "preflight": {
                **preflight.as_dict(),
                "effective_feature_weights": effective_weights,
                "disabled_feature_groups": disabled_groups,
            },
        }

    cols = ["Date", "Type"] + [f"HE{h}" for h in configs.HOURS] + ["OnPeak", "OffPeak", "Flat"]
    strip_table = pd.DataFrame(strip_rows, columns=cols)
    quantiles_table = pd.DataFrame(quantile_rows, columns=cols)

    first_day = str(forecast_dates[0]) if forecast_dates else None
    top_level_analogs = per_day_analogs.get(first_day, pd.DataFrame())

    logger.info("Forward-only KNN strip complete: horizon=%s days", horizon)
    return {
        "strip_table": strip_table,
        "quantiles_table": quantiles_table,
        "analogs": top_level_analogs,
        "per_day_analogs": per_day_analogs,
        "reference_date": str(base),
        "forecast_dates": [str(d) for d in forecast_dates],
        "per_day": per_day,
        "output_table": strip_table,
        "forecast_date": str(forecast_dates[0]) if forecast_dates else str(base),
        "n_analogs_used": int(len(top_level_analogs)) if len(top_level_analogs) > 0 else 0,
        "preflight": None,
    }


def run_strip(*args, **kwargs) -> dict:
    """Backward-compatible alias."""
    return run_strip_forecast(*args, **kwargs)


if __name__ == "__main__":
    result = run_strip_forecast(horizon=3)
    print(result["strip_table"])
