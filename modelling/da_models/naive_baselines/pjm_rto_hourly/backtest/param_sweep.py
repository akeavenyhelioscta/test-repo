"""Multi-day backtest for the two naive baselines.

Iterates over a configurable date list, runs both EPF and d-7 against
each date, writes per-row parquet to ``backtest/output/<sweep_id>.parquet``
and prints a leaderboard at the end.

The naive baselines are deterministic, so the only loop dimension is
the date axis (no scenario sweep).

Usage::

    python -m da_models.naive_baselines.pjm_rto_hourly.backtest.param_sweep
    python modelling/da_models/naive_baselines/pjm_rto_hourly/backtest/param_sweep.py
"""
from __future__ import annotations

import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from da_models.like_day_model_knn import configs as like_day_configs  # noqa: E402
from da_models.naive_baselines._shared import build_lmp_only_pool  # noqa: E402
from da_models.naive_baselines.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as naive_run,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None              # None -> tomorrow (date.today() + 1d)
BACKTEST_WINDOW_DAYS: int = 14               # walk back N calendar days from
                                             # TARGET_DATE - 1; filter to weekday
                                             # targets that have full actuals
SWEEP_OUTPUT_DIR: Path = Path(__file__).resolve().parent / "output"
HUB: str = like_day_configs.HUB
BASELINES: tuple[str, ...] = ("epf", "d7")
HIT_RATE_DOLLARS: float = 5.0                # |error| < N => "hit"


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _backtest_dates(
    pool: pd.DataFrame, anchor_date: date, lookback_days: int,
) -> list[date]:
    """Last N weekday targets ending at ``anchor_date - 1``, restricted to
    dates with full actuals AND with both lag-1 and lag-7 source rows
    available in the pool."""
    out: list[date] = []
    pool_dates = set(pool["date"].tolist())
    for k in range(1, lookback_days + 1):
        d = anchor_date - timedelta(days=k)
        if d.weekday() >= 5:
            continue
        if actuals_from_pool(pool, d) is None:
            continue
        if (d - timedelta(days=1)) not in pool_dates:
            continue
        if (d - timedelta(days=7)) not in pool_dates:
            continue
        out.append(d)
    return out


def _sweep_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    return f"naive_{stamp}_{uuid.uuid4().hex[:6]}"


def _execute(
    *, baseline: str, target_date: date, pool: pd.DataFrame,
) -> dict:
    started = time.perf_counter()
    base: dict = {
        "target_date": target_date,
        "baseline": baseline,
        "status": "ok",
        "error_message": None,
    }
    try:
        result = naive_run(target_date=target_date, baseline=baseline, pool=pool, quiet=True)
    except Exception as exc:
        base.update({
            "status": "failed",
            "error_message": f"{type(exc).__name__}: {exc}",
            "duration_s": round(time.perf_counter() - started, 3),
        })
        return base

    metrics = result.get("metrics") or {}
    output_table = result.get("output_table")
    df_forecast = result.get("df_forecast")

    forecast_row = (
        output_table[output_table["Type"] == "Forecast"].iloc[0]
        if output_table is not None and len(output_table) else None
    )
    actual_row = (
        output_table[output_table["Type"] == "Actual"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Actual").any() else None
    )
    error_row = (
        output_table[output_table["Type"] == "Error"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Error").any() else None
    )

    n_hours_actual = 0
    n_hits = 0
    if error_row is not None:
        for h in range(1, 25):
            v = error_row.get(f"HE{h}")
            if pd.notna(v):
                n_hours_actual += 1
                if abs(float(v)) < HIT_RATE_DOLLARS:
                    n_hits += 1
    hit_rate = (n_hits / n_hours_actual) if n_hours_actual else None

    base.update({
        "baseline_name": result.get("baseline_name"),
        "n_pool": result.get("n_pool"),
        "has_actuals": result.get("has_actuals"),
        "n_hours_actual": n_hours_actual,
        "n_hours_forecast": int(len(df_forecast)) if df_forecast is not None else 0,
        "n_hits": n_hits,
        "hit_rate": hit_rate,
        "mae": metrics.get("mae"),
        "rmse": metrics.get("rmse"),
        "mape": metrics.get("mape"),
        "forecast_onpeak": float(forecast_row["OnPeak"]) if forecast_row is not None else None,
        "forecast_offpeak": float(forecast_row["OffPeak"]) if forecast_row is not None else None,
        "forecast_flat": float(forecast_row["Flat"]) if forecast_row is not None else None,
        "actual_onpeak": float(actual_row["OnPeak"]) if actual_row is not None else None,
        "actual_offpeak": float(actual_row["OffPeak"]) if actual_row is not None else None,
        "actual_flat": float(actual_row["Flat"]) if actual_row is not None else None,
        "duration_s": round(time.perf_counter() - started, 3),
    })
    return base


def _attach_rmae_self(rows: list[dict]) -> list[dict]:
    """For each (target_date, baseline) row, compute MAE divided by the
    OTHER baseline's MAE on the same date. Diagnostic only, comparing
    the two naives directly without a third reference.
    """
    df = pd.DataFrame(rows)
    if df.empty or "mae" not in df.columns:
        for r in rows:
            r["rmae_self"] = None
        return rows

    mae_lookup: dict[tuple[date, str], float] = {}
    for _, r in df.iterrows():
        if r.get("status") != "ok" or pd.isna(r.get("mae")):
            continue
        mae_lookup[(r["target_date"], r["baseline"])] = float(r["mae"])

    for r in rows:
        own = mae_lookup.get((r["target_date"], r["baseline"]))
        others = [
            mae for (td, b), mae in mae_lookup.items()
            if td == r["target_date"] and b != r["baseline"]
        ]
        if own is None or not others:
            r["rmae_self"] = None
            continue
        denom = others[0]
        r["rmae_self"] = (own / denom) if denom > 0 else float("inf")
    return rows


def _print_leaderboard(rows: list[dict], sweep_id: str, target_dates: list[date]) -> None:
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"]
    failed = df[df["status"] == "failed"]

    print("\n" + "=" * 100)
    print(f"  NAIVE BASELINE SWEEP — {sweep_id}")
    print(f"  Window: {len(target_dates)} weekday target(s)")
    if target_dates:
        print(f"  Range:  {target_dates[-1]} -> {target_dates[0]}")
    print(f"  Cells:  {len(df)} ({len(ok)} ok, {len(failed)} failed)")
    print("=" * 100)

    if len(ok) == 0:
        print("\n  No successful cells; nothing to summarize.\n")
    else:
        agg = (
            ok.groupby("baseline_name")
            .agg(
                n_dates=("target_date", "count"),
                mean_mae=("mae", "mean"),
                mean_rmse=("rmse", "mean"),
                mean_rmae_self=("rmae_self", "mean"),
                mean_hit_rate=("hit_rate", "mean"),
            )
            .reset_index()
            .sort_values("mean_mae", ascending=True, na_position="last")
            .reset_index(drop=True)
        )
        cols = [
            "baseline_name", "n_dates", "mean_mae", "mean_rmse",
            "mean_rmae_self", "mean_hit_rate",
        ]
        formatters = {
            "mean_mae":       lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "mean_rmse":      lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "mean_rmae_self": lambda v: f"{v:>7.3f}" if pd.notna(v) else "    n/a",
            "mean_hit_rate":  lambda v: f"{v:>6.1%}" if pd.notna(v) else "   n/a",
        }
        with pd.option_context("display.max_rows", None, "display.width", None):
            print()
            print(agg[cols].to_string(index=False, formatters=formatters))
        print(f"\n  hit-rate threshold: |error| < ${HIT_RATE_DOLLARS:.0f}/MWh")

    if len(failed) > 0:
        print("\n  Failed cells:")
        for _, r in failed.iterrows():
            print(f"    {r['baseline']} @ {r['target_date']}: {r['error_message']}")
    print()


def _persist(rows: list[dict], sweep_id: str) -> Path:
    SWEEP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = SWEEP_OUTPUT_DIR / f"{sweep_id}.parquet"
    df = pd.DataFrame(rows)
    df["sweep_id"] = sweep_id
    df["sweep_started_at_utc"] = datetime.now(timezone.utc).isoformat()
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def run(
    target_date: date | None = TARGET_DATE,
    backtest_window_days: int = BACKTEST_WINDOW_DAYS,
    baselines: tuple[str, ...] = BASELINES,
    output_dir: Path | None = None,
    hub: str = HUB,
) -> Path:
    """Run both naive baselines across the backtest window, persist results,
    and print a leaderboard. Returns the parquet path.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    global SWEEP_OUTPUT_DIR
    if output_dir is not None:
        SWEEP_OUTPUT_DIR = Path(output_dir)

    sweep_id = _sweep_id()
    anchor = _resolve_target_date(target_date)

    print(f"\n[naive_sweep {sweep_id}] building LMP-only pool...")
    t0 = time.perf_counter()
    pool = build_lmp_only_pool(hub=hub)
    print(f"[naive_sweep {sweep_id}] pool built in {time.perf_counter() - t0:.1f}s "
          f"({len(pool)} rows)")

    target_dates = _backtest_dates(pool, anchor, backtest_window_days)
    if not target_dates:
        raise RuntimeError(
            f"No weekday target dates with actuals + lag rows found in the "
            f"{backtest_window_days}d window ending {anchor}. "
            "Try a longer window or check pool coverage."
        )
    print(f"[naive_sweep {sweep_id}] {len(target_dates)} weekday targets: "
          f"{target_dates[-1]} -> {target_dates[0]}")

    rows: list[dict] = []
    for td in target_dates:
        for baseline in baselines:
            row = _execute(baseline=baseline, target_date=td, pool=pool)
            tag = "OK" if row["status"] == "ok" else "FAIL"
            mae = row.get("mae")
            mae_str = f"MAE={mae:.2f}" if isinstance(mae, (int, float)) and pd.notna(mae) else "MAE=n/a"
            print(f"[naive_sweep {sweep_id}]   {td} {baseline:<6} {tag:<4} "
                  f"{mae_str}  ({row['duration_s']:.2f}s)")
            rows.append(row)

    rows = _attach_rmae_self(rows)
    parquet_path = _persist(rows, sweep_id)
    _print_leaderboard(rows, sweep_id, target_dates)
    print(f"  Wrote: {parquet_path}\n")
    return parquet_path


if __name__ == "__main__":
    run()
