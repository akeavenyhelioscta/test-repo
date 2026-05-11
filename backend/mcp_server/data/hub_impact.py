"""Data-access layer for the hub-impact MCP view.

Reads the cached multi-hub DC shift factors from
`network/hub_branch_weights.parquet` and provides per-(hub, branch)
lookup. Handles parallel circuits by reporting the maximum-|ISF|
circuit (the worst-case interpretation, which is the trader-relevant
one when a constraint is binding on the corridor).

Cache is built by `backend/mcp_server/data/shift_factors.py` for all
HUB-typed aggregates in the dbt mart (currently 12 PJM hubs). Re-run
that module when the PSS/E .raw model or hub membership changes.
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from backend.mcp_server.data.shift_factors import load_hub_branch_weights

logger = logging.getLogger(__name__)


# In-process cache: parquet read once, then per-hub slices indexed
# for fast lookup. Loaded lazily on first request.
_FULL_CACHE: Optional[pd.DataFrame] = None
_HUB_INDEX: dict[str, pd.DataFrame] = {}


def _get_hub_slice(hub_name: str) -> Optional[pd.DataFrame]:
    """Return the per-branch DataFrame for one hub, indexing on demand."""
    global _FULL_CACHE
    if _FULL_CACHE is None:
        _FULL_CACHE = load_hub_branch_weights()

    if hub_name not in _HUB_INDEX:
        slice_df = _FULL_CACHE[_FULL_CACHE["agg_pnode_name"] == hub_name].copy()
        if slice_df.empty:
            return None
        _HUB_INDEX[hub_name] = slice_df.reset_index(drop=True)
    return _HUB_INDEX[hub_name]


def lookup_hub_isf(
    hub_name: str,
    from_bus: int,
    to_bus: int,
) -> Optional[dict]:
    """Look up the hub-weighted shift factor for a branch.

    Tries both directional orderings (from→to and to→from) since the
    .raw stores branches in one specific order but constraints may
    arrive in either. Returns the row with maximum |hub_isf| if
    multiple parallel circuits exist at the corridor (worst-case).

    Returns None if the hub isn't in the cache OR the branch isn't
    matched (post-2021 facility / model gap).
    """
    slice_df = _get_hub_slice(hub_name)
    if slice_df is None:
        return None

    matches = slice_df[
        ((slice_df["from_bus"] == from_bus) & (slice_df["to_bus"] == to_bus))
        | ((slice_df["from_bus"] == to_bus) & (slice_df["to_bus"] == from_bus))
    ]
    if matches.empty:
        return None

    picked = matches.iloc[matches["abs_hub_isf"].argmax()]

    # Sign correction: if we matched on the reversed orientation, flip
    # the sign so the caller's (from_bus, to_bus) direction is what's
    # reported.
    matched_forward = (picked["from_bus"] == from_bus) and (picked["to_bus"] == to_bus)
    hub_isf = float(picked["hub_isf"]) if matched_forward else -float(picked["hub_isf"])

    return {
        "hub_name": hub_name,
        "from_bus": int(picked["from_bus"] if matched_forward else picked["to_bus"]),
        "to_bus": int(picked["to_bus"] if matched_forward else picked["from_bus"]),
        "ckt_id": str(picked["ckt_id"]),
        "equipment_type": str(picked["equipment_type"]),
        "x_pu": float(picked["x_pu"]),
        "hub_isf": hub_isf,
        "abs_hub_isf": float(picked["abs_hub_isf"]),
        "n_parallel_circuits": int(len(matches)),
    }


def list_hubs() -> list[str]:
    """List the hub names available in the cache."""
    global _FULL_CACHE
    if _FULL_CACHE is None:
        _FULL_CACHE = load_hub_branch_weights()
    return sorted(_FULL_CACHE["agg_pnode_name"].unique().tolist())


def cache_summary() -> dict:
    """Quick stats about the loaded cache."""
    global _FULL_CACHE
    if _FULL_CACHE is None:
        _FULL_CACHE = load_hub_branch_weights()
    return {
        "n_rows": len(_FULL_CACHE),
        "n_hubs": int(_FULL_CACHE["agg_pnode_name"].nunique()),
        "hub_names": sorted(_FULL_CACHE["agg_pnode_name"].unique().tolist()),
    }
