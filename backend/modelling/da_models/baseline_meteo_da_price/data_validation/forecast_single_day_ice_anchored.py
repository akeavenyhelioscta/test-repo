"""Data-validation preflight for ``pipelines/forecast_single_day_ice_anchored.py``.

The ICE-anchored pipeline consumes the *identical* Meteologica lead-1 +
settled-DA-LMP inputs as the plain single-day pipeline, so this preflight runs
the same check list, then adds ICE-ticker checks. Anchoring degrades gracefully
to the unanchored layout when ICE is unavailable, so every ICE-related finding
is a WARN, never an ERROR -- the forecast still publishes, just unscaled.

ICE checks (all WARN):
  - ICE ticker trades reachable for the target date & symbol;
  - an ICE VWAP is computable (>= 1 eligible non-Spread / non-Leg trade);
  - the ICE VWAP is in a sane $/MWh range;
  - the Det OnPeak HE8-23 mean -- the anchor denominator -- is usable
    (non-NaN, > 0); otherwise the pipeline falls back to ENS Avg, then skips.

Writes nothing; never imported by the forecast pipeline.

Usage::

    python -m backend.modelling.da_models.baseline_meteo_da_price.data_validation.forecast_single_day_ice_anchored

Exit code is 0 when inputs are healthy, non-zero (DataValidationError) otherwise.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.baseline_meteo_da_price import ice_anchor  # noqa: E402
from backend.modelling.da_models.baseline_meteo_da_price.data_validation import (  # noqa: E402
    _shared,
)
from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.modelling.da_models.common.validation import (  # noqa: E402
    CheckResult,
    CheckStatus,
    ValidationReport,
    print_report,
    run_checks,
)
from backend.modelling.da_models.common.validation.checks import (  # noqa: E402
    DA_LMP_MAX_USD,
    DA_LMP_MIN_USD,
)
from backend.utils.logging_utils import init_logging, print_header  # noqa: E402

# ── Defaults (mirror pipelines/forecast_single_day_ice_anchored.py) ────────
TARGET_DATE: date | None = None  # None -> tomorrow
HUB: str = _shared.HUB
LEAD_DAYS: int | None = _shared.LEAD_DAYS
CACHE_DIR: Path | None = _shared.CACHE_DIR
ICE_SYMBOL: str = ice_anchor.DEFAULT_SYMBOL  # "PDA D1-IUS"
ICE_VWAP_CUTOFF: pd.Timestamp | None = None  # None -> all trades to date
LOG_DIR: Path = _shared.LOG_DIR

_ICE_NAME = "ice_anchor"


def _det_onpeak_hourly(meteo: pd.DataFrame, resolved_date: date) -> dict[int, float]:
    rows = meteo[
        pd.to_datetime(meteo["date"], errors="coerce").dt.date == resolved_date
    ]
    out: dict[int, float] = {}
    for _, r in rows.iterrows():
        v = r.get("da_price_deterministic")
        if pd.notna(v):
            out[int(r["hour_ending"])] = float(v)
    return out


def _ice_results(
    meteo: pd.DataFrame,
    resolved_date: date,
    *,
    symbol: str,
    cutoff: pd.Timestamp | None,
) -> list[CheckResult]:
    """Build the ICE-ticker WARN checks. All non-fatal -- anchoring degrades."""
    try:
        trades = ice_anchor.fetch_ice_ticker_trades(
            delivery_date=resolved_date, symbol=symbol, cutoff_local=cutoff
        )
    except Exception as exc:  # noqa: BLE001 - ICE feed I/O failure must not abort
        return [
            CheckResult(
                f"{_ICE_NAME}: ICE ticker feed reachable",
                CheckStatus.WARN,
                f"ICE feed unreachable ({type(exc).__name__}: {exc}); "
                f"anchoring will be skipped",
            )
        ]

    results: list[CheckResult] = []
    if trades.empty:
        results.append(
            CheckResult(
                f"{_ICE_NAME}: trades present for target date",
                CheckStatus.WARN,
                f"no {symbol} trades for delivery {resolved_date}; "
                f"anchoring will be skipped",
            )
        )
    else:
        results.append(
            CheckResult(
                f"{_ICE_NAME}: trades present for target date",
                CheckStatus.PASS,
                f"{len(trades)} {symbol} trade row(s) for {resolved_date}",
            )
        )

    vwap = ice_anchor.compute_vwap(trades)
    if vwap.vwap is None:
        results.append(
            CheckResult(
                f"{_ICE_NAME}: VWAP computable",
                CheckStatus.WARN,
                f"no eligible trades (rows={len(trades)}, excluded={vwap.n_excluded}); "
                f"anchoring will be skipped",
            )
        )
    else:
        in_range = DA_LMP_MIN_USD <= vwap.vwap <= DA_LMP_MAX_USD
        results.append(
            CheckResult(
                f"{_ICE_NAME}: VWAP in sane $/MWh range",
                CheckStatus.PASS if in_range else CheckStatus.WARN,
                f"VWAP {vwap.vwap:,.2f} $/MWh over {vwap.volume:,.0f} MWh "
                f"({vwap.n_trades} trades)"
                + (
                    ""
                    if in_range
                    else f" -- outside [{DA_LMP_MIN_USD}, {DA_LMP_MAX_USD}]"
                ),
            )
        )

    det_onpk = ice_anchor.onpeak_mean(_det_onpeak_hourly(meteo, resolved_date))
    if det_onpk is None or det_onpk <= 0:
        results.append(
            CheckResult(
                f"{_ICE_NAME}: Det OnPeak usable as anchor denominator",
                CheckStatus.WARN,
                f"Det OnPeak HE8-23 mean is {det_onpk}; pipeline falls back to "
                f"ENS Avg, then skips anchoring",
            )
        )
    else:
        results.append(
            CheckResult(
                f"{_ICE_NAME}: Det OnPeak usable as anchor denominator",
                CheckStatus.PASS,
                f"Det OnPeak HE8-23 mean = {det_onpk:,.2f} $/MWh",
            )
        )
    return results


def validate(
    target_date: date | None = TARGET_DATE,
    *,
    hub: str = HUB,
    lead_days: int | None = LEAD_DAYS,
    cache_dir: Path | None = CACHE_DIR,
    ice_symbol: str = ICE_SYMBOL,
    ice_vwap_cutoff: pd.Timestamp | None = ICE_VWAP_CUTOFF,
) -> ValidationReport:
    """Run the single-day check list plus the ICE-ticker WARN checks."""
    resolved_date = _shared.resolve_target_date(target_date)
    meteo = loader.load_meteologica_da_price_forecast(
        cache_dir=cache_dir, lead_days=lead_days
    )
    lmps = loader.load_lmps_da(cache_dir=cache_dir)
    lmps_at_hub = lmps[lmps["region"].astype(str) == hub] if not lmps.empty else lmps

    specs = list(
        _shared.meteo_single_day_specs(
            meteo, lmps_at_hub, resolved_date, lead_days=lead_days, hub=hub
        )
    )
    # ICE checks are computed eagerly (the fetch is DB I/O); wrap each result
    # in a thunk so run_checks treats them uniformly.
    ice_res = _ice_results(
        meteo, resolved_date, symbol=ice_symbol, cutoff=ice_vwap_cutoff
    )
    specs.extend(lambda r=r: r for r in ice_res)
    return run_checks(specs)


def run(
    target_date: date | None = TARGET_DATE,
    *,
    hub: str = HUB,
    lead_days: int | None = LEAD_DAYS,
    cache_dir: Path | None = CACHE_DIR,
    ice_symbol: str = ICE_SYMBOL,
    ice_vwap_cutoff: pd.Timestamp | None = ICE_VWAP_CUTOFF,
    quiet: bool = False,
) -> ValidationReport:
    """Preflight entrypoint: validate, print the report, raise if it failed."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(
        name="preflight_baseline_single_day_ice_anchored", log_dir=LOG_DIR
    )
    try:
        resolved_date = _shared.resolve_target_date(target_date)
        if not quiet:
            print_header(
                f"PREFLIGHT - forecast_single_day_ice_anchored | {hub} | "
                f"target {resolved_date} | ICE {ice_symbol}",
                "=",
                100,
            )
        report = validate(
            target_date=target_date,
            hub=hub,
            lead_days=lead_days,
            cache_dir=cache_dir,
            ice_symbol=ice_symbol,
            ice_vwap_cutoff=ice_vwap_cutoff,
        )
        if not quiet:
            print_report(report, logger=pl)
        report.raise_if_failed()
        return report
    finally:
        pl.close()


if __name__ == "__main__":
    run()
