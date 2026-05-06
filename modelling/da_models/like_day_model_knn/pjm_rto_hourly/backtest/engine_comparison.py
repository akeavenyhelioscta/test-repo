"""Cross-family engine comparison + flt_radius sweep.

Runs four engine configurations on the same backtest window of weekday
target dates and compares mean MAE / RMSE / rMAE / CRPS / coverage.

Configurations:

  knn_flt0   -- like_day_model_knn engine, flt_radius=0
                (scalar match at target HE; sunny-like window scope)
  knn_flt1   -- like_day_model_knn engine, flt_radius=1
                (HE-1, HE, HE+1 window — current spec default)
  knn_flt3   -- like_day_model_knn engine, flt_radius=3
                (HE-3 .. HE+3 window — broader local context)
  sunny      -- like_day_model_knn_sunny engine on its native long pool

Both families consume the same parquet sources (load / solar / wind /
net_load / outages / gas / weather), use byte-identical metric
definitions in their respective ``metrics.evaluate_forecast``, and
default to last-week-same-day persistence as the rMAE naive baseline,
so the reported numbers are apples-to-apples comparable.

Per-family pools are built ONCE and reused across all target dates and
flt_radius variants. Per-target-date the knn ``query`` row is built
once and reused across the three flt_radius variants. Sunny builds its
query inside its pipeline because sunny's query is a 24-row frame that
the pipeline composes itself.

Cross-family import is intentional and bounded to this script — the
project's "forward-only cross-family imports" rule is for production
model code; a comparison harness is sideways by nature.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.engine_comparison
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/engine_comparison.py
"""

from __future__ import annotations

import sys
import time
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from da_models.like_day_model_knn import _shared as _knn_shared  # noqa: E402
from da_models.like_day_model_knn import configs as knn_configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool as knn_build_pool,
    build_query_row as knn_build_query_row,
)
from da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as knn_run,
)
from da_models.like_day_model_knn_sunny import configs as sunny_configs  # noqa: E402
from da_models.like_day_model_knn_sunny.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool as sunny_build_pool,
)
from da_models.like_day_model_knn_sunny.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as sunny_run,
)
from utils.logging_utils import (  # noqa: E402
    Colors,
    print_divider,
    print_header,
    supports_color,
)

_COLOR_ON: bool = supports_color()
_HL_LEADER: str = Colors.BOLD if _COLOR_ON else ""
_HL_WIN: str = Colors.BRIGHT_GREEN if _COLOR_ON else ""
_HL_LOSS: str = Colors.BRIGHT_RED if _COLOR_ON else ""
_RS: str = Colors.RESET if _COLOR_ON else ""


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = (
    None  # None -> tomorrow (anchor); we walk back to find weekday targets
)
BACKTEST_WINDOW_DAYS: int = 14
KNN_FLT_RADII: tuple[int, ...] = (0, 1, 3)

# knn spec: the sunny-aligned spec (load + ramps + solar + wind + net_load
# + temp + outages + gas + calendar). Matches what sunny exercises so the
# comparison isolates engine behavior, not feature scope.
KNN_MODEL_NAME: str = knn_configs.PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC.name
SUNNY_MODEL_NAME: str = sunny_configs.PJM_RTO_HOURLY_SUNNY_SPEC.name

_DOW_ABBR: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _resolve_anchor(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _backtest_dates(pool: pd.DataFrame, anchor: date, lookback_days: int) -> list[date]:
    """Last N weekday targets ending at anchor, with full LMP actuals in pool.

    Uses the knn (wide) pool for the actuals check; sunny shares the
    same LMP source so any date with knn actuals also has sunny actuals.
    """
    out: list[date] = []
    for k in range(1, lookback_days + 1):
        d = anchor - timedelta(days=k)
        if d.weekday() >= 5:
            continue
        if actuals_from_pool(pool, d) is None:
            continue
        out.append(d)
    return out


def _execute_knn(
    target_date: date,
    flt_radius: int,
    pool: pd.DataFrame,
    query: pd.Series,
    dates_meta: pd.DataFrame,
) -> dict:
    """One knn forecast run, captured to a flat metric row."""
    started = time.perf_counter()
    base = {
        "engine": f"knn_flt{flt_radius}",
        "target_date": target_date,
        "status": "ok",
        "error_message": None,
    }
    try:
        result = knn_run(
            target_date=target_date,
            model_name=KNN_MODEL_NAME,
            flt_radius=flt_radius,
            pool=pool,
            query=query,
            dates_meta=dates_meta,
            quiet=True,
            write_analog_store=False,
        )
    except Exception as exc:
        base.update(
            {
                "status": "failed",
                "error_message": f"{type(exc).__name__}: {exc}",
                "duration_s": round(time.perf_counter() - started, 3),
            }
        )
        return base

    metrics = result.get("metrics") or {}
    base.update(
        {
            "n_pool": result.get("n_pool"),
            "n_analogs_used": result.get("n_analogs_used"),
            "mae": metrics.get("mae"),
            "rmse": metrics.get("rmse"),
            "rmae": metrics.get("rmae"),
            "crps": metrics.get("crps"),
            "mean_pinball": metrics.get("mean_pinball"),
            "coverage_90pct": metrics.get("coverage_90pct"),
            "sharpness_90pct": metrics.get("sharpness_90pct"),
            "duration_s": round(time.perf_counter() - started, 3),
        }
    )
    return base


def _execute_sunny(target_date: date, pool: pd.DataFrame) -> dict:
    """One sunny forecast run, captured to a flat metric row."""
    started = time.perf_counter()
    base = {
        "engine": "sunny",
        "target_date": target_date,
        "status": "ok",
        "error_message": None,
    }
    try:
        result = sunny_run(
            target_date=target_date,
            model_name=SUNNY_MODEL_NAME,
            pool=pool,
            quiet=True,
        )
    except Exception as exc:
        base.update(
            {
                "status": "failed",
                "error_message": f"{type(exc).__name__}: {exc}",
                "duration_s": round(time.perf_counter() - started, 3),
            }
        )
        return base

    metrics = result.get("metrics") or {}
    base.update(
        {
            "n_pool": result.get("n_pool"),
            "n_analogs_used": result.get("n_analogs_used"),
            "mae": metrics.get("mae"),
            "rmse": metrics.get("rmse"),
            "rmae": metrics.get("rmae"),
            "crps": metrics.get("crps"),
            "mean_pinball": metrics.get("mean_pinball"),
            "coverage_90pct": metrics.get("coverage_90pct"),
            "sharpness_90pct": metrics.get("sharpness_90pct"),
            "duration_s": round(time.perf_counter() - started, 3),
        }
    )
    return base


# ── Output ─────────────────────────────────────────────────────────────────


def _format_metric(v, width: int, fmt: str = ".2f") -> str:
    if v is None or pd.isna(v):
        return f"{'n/a':>{width}}"
    return f"{v:>{width}{fmt}}"


def _format_pct(v, width: int) -> str:
    if v is None or pd.isna(v):
        return f"{'n/a':>{width}}"
    return f"{v * 100:>{width - 1}.1f}%"


def _print_per_date_table(rows: list[dict], width: int = 110) -> None:
    """Per-date MAE for each engine — useful for spotting days where the
    verdict flips between engines."""
    print()
    print_header("PER-TARGET-DATE MAE", "=", width)
    print()
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"]
    if ok.empty:
        print("  (no successful runs)")
        return
    pivot = ok.pivot_table(
        index="target_date", columns="engine", values="mae"
    ).sort_index()
    engines = sorted(ok["engine"].unique())
    name_w = max(10, max(len(e) for e in engines) + 1)

    head = f"  {'date':<12} {'dow':<4}"
    for e in engines:
        head += f" {e:>{name_w}}"
    head += f"  {'best':>10}"
    print(head)
    print_divider("-", len(head), dim=False)

    for d, row in pivot.iterrows():
        d_obj = d if isinstance(d, date) else pd.Timestamp(d).date()
        dow = _DOW_ABBR[d_obj.weekday()]
        line = f"  {str(d_obj):<12} {dow:<4}"
        best_engine = row.idxmin() if row.notna().any() else None
        for e in engines:
            v = row.get(e)
            cell = _format_metric(v, name_w, ".2f")
            if e == best_engine:
                cell = f"{_HL_WIN}{cell}{_RS}"
            line += f" {cell}"
        line += f"  {best_engine or 'n/a':>10}"
        print(line)
    print_divider("-", len(head), dim=False)


def _print_leaderboard(rows: list[dict], width: int = 120) -> None:
    """Per-engine mean metrics across the backtest window, sorted by mean MAE."""
    print()
    print_header("ENGINE COMPARISON LEADERBOARD  (mean across window)", "=", width)
    print()
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"]
    failed = df[df["status"] == "failed"]
    if ok.empty:
        print("  No successful runs to summarize.")
        if not failed.empty:
            print("\n  Failed runs:")
            for _, r in failed.iterrows():
                print(f"    {r['engine']:<12} {r['target_date']}  {r['error_message']}")
        return

    agg = (
        ok.groupby("engine")
        .agg(
            n_dates=("target_date", "count"),
            mean_mae=("mae", "mean"),
            mean_rmse=("rmse", "mean"),
            mean_rmae=("rmae", "mean"),
            mean_crps=("crps", "mean"),
            mean_cov_90=("coverage_90pct", "mean"),
            mean_sharp_90=("sharpness_90pct", "mean"),
            mean_n_analogs=("n_analogs_used", "mean"),
            mean_dur_s=("duration_s", "mean"),
        )
        .reset_index()
    )
    agg = agg.sort_values("mean_mae", ascending=True, na_position="last").reset_index(
        drop=True
    )

    name_w = max(8, max(len(e) for e in agg["engine"]) + 1)
    head = (
        f"  {'engine':<{name_w}} {'n_dates':>8} {'mae':>8} {'rmse':>8} "
        f"{'rmae':>7} {'crps':>8} {'cov_90':>7} {'sharp_90':>9} "
        f"{'analogs':>8} {'sec':>5}"
    )
    print(head)
    print_divider("-", len(head), dim=False)
    best_mae = agg["mean_mae"].min() if not agg["mean_mae"].isna().all() else None
    for _, r in agg.iterrows():
        line = (
            f"  {str(r['engine']):<{name_w}} "
            f"{int(r['n_dates']):>8d} "
            f"{_format_metric(r['mean_mae'], 8)} "
            f"{_format_metric(r['mean_rmse'], 8)} "
            f"{_format_metric(r['mean_rmae'], 7, '.3f')} "
            f"{_format_metric(r['mean_crps'], 8, '.3f')} "
            f"{_format_pct(r['mean_cov_90'], 7)} "
            f"{_format_metric(r['mean_sharp_90'], 9)} "
            f"{_format_metric(r['mean_n_analogs'], 8, '.0f')} "
            f"{_format_metric(r['mean_dur_s'], 5, '.1f')}"
        )
        if (
            best_mae is not None
            and pd.notna(r["mean_mae"])
            and r["mean_mae"] == best_mae
        ):
            line = f"{_HL_LEADER}{line}{_RS}"
        print(line)
    print_divider("-", len(head), dim=False)
    print()
    if best_mae is not None:
        winner = agg.loc[agg["mean_mae"] == best_mae, "engine"].iloc[0]
        print(f"  Best mean MAE: {winner}  ({best_mae:.2f} $/MWh)")
    print(
        "  Read: lower mean_mae / mean_rmse / mean_rmae = better point forecast."
        " Higher mean_cov_90 (closer to 90%) and lower mean_sharp_90 = better"
        " calibrated probabilistic forecast."
    )

    if not failed.empty:
        print()
        print("  Failed runs:")
        for _, r in failed.iterrows():
            print(f"    {r['engine']:<12} {r['target_date']}  {r['error_message']}")


# ── main ───────────────────────────────────────────────────────────────────


def run(
    target_date: date | None = TARGET_DATE,
    backtest_window_days: int = BACKTEST_WINDOW_DAYS,
    knn_flt_radii: tuple[int, ...] = KNN_FLT_RADII,
) -> dict:
    """Execute the cross-family engine comparison and print the
    per-date + leaderboard tables. Returns the full row list for
    notebook consumption."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    anchor = _resolve_anchor(target_date)

    print_header("ENGINE COMPARISON  --  knn (wide) vs sunny (long)", "=", 110)
    print()
    print(f"  Anchor date         {anchor}  (walks back {backtest_window_days} days)")
    print(f"  knn spec            {KNN_MODEL_NAME}")
    print(f"  knn flt_radius      {knn_flt_radii}")
    print(f"  sunny spec          {SUNNY_MODEL_NAME}")
    print()

    # Build pools and dates_meta ONCE.
    print("[engine-cmp] building knn pool...")
    t0 = time.perf_counter()
    knn_base_spec = knn_configs.MODEL_REGISTRY[KNN_MODEL_NAME]
    knn_spec_for_build = replace(
        knn_base_spec, flt_radius=int(knn_base_spec.flt_radius)
    )
    knn_pool = knn_build_pool(spec=knn_spec_for_build, cache_dir=knn_configs.CACHE_DIR)
    knn_dates_meta = _knn_shared.load_dates_daily(knn_configs.CACHE_DIR)
    print(
        f"[engine-cmp] knn pool: {len(knn_pool)} rows in"
        f" {time.perf_counter() - t0:.1f}s"
    )

    print("[engine-cmp] building sunny pool...")
    t0 = time.perf_counter()
    sunny_spec = sunny_configs.MODEL_REGISTRY[SUNNY_MODEL_NAME]
    sunny_pool = sunny_build_pool(spec=sunny_spec, cache_dir=sunny_configs.CACHE_DIR)
    print(
        f"[engine-cmp] sunny pool: {len(sunny_pool)} rows in"
        f" {time.perf_counter() - t0:.1f}s"
    )

    target_dates = _backtest_dates(knn_pool, anchor, backtest_window_days)
    if not target_dates:
        raise RuntimeError(
            f"No weekday target dates with actuals in the {backtest_window_days}d "
            f"window ending {anchor}."
        )
    print(
        f"[engine-cmp] {len(target_dates)} weekday target(s):"
        f" {target_dates[-1]} -> {target_dates[0]}"
    )
    print(
        f"[engine-cmp] {len(target_dates)} dates x"
        f" ({len(knn_flt_radii)} knn flt + 1 sunny) ="
        f" {len(target_dates) * (len(knn_flt_radii) + 1)} cells"
    )
    print()

    rows: list[dict] = []
    for td in target_dates:
        try:
            knn_query = knn_build_query_row(
                target_date=td,
                cache_dir=knn_configs.CACHE_DIR,
                spec=knn_spec_for_build,
            )
        except Exception as exc:
            print(
                f"[engine-cmp]   skip {td}: knn build_query_row failed"
                f" ({type(exc).__name__}: {exc})"
            )
            continue

        for flt in knn_flt_radii:
            row = _execute_knn(td, flt, knn_pool, knn_query, knn_dates_meta)
            tag = "OK" if row["status"] == "ok" else "FAIL"
            mae_str = (
                f"MAE={row['mae']:.2f}"
                if row.get("mae") is not None and pd.notna(row.get("mae"))
                else "MAE=n/a"
            )
            print(
                f"[engine-cmp]   {td} knn_flt{flt}    {tag:<4}"
                f" {mae_str}  ({row['duration_s']:.2f}s)"
            )
            rows.append(row)

        srow = _execute_sunny(td, sunny_pool)
        tag = "OK" if srow["status"] == "ok" else "FAIL"
        mae_str = (
            f"MAE={srow['mae']:.2f}"
            if srow.get("mae") is not None and pd.notna(srow.get("mae"))
            else "MAE=n/a"
        )
        print(
            f"[engine-cmp]   {td} sunny         {tag:<4}"
            f" {mae_str}  ({srow['duration_s']:.2f}s)"
        )
        rows.append(srow)

    _print_per_date_table(rows)
    _print_leaderboard(rows)
    print()

    return {
        "rows": rows,
        "target_dates": target_dates,
        "anchor": anchor,
    }


if __name__ == "__main__":
    run()
