"""View-model builders for the hub-buses MCP endpoint.

One endpoint, two builders depending on whether the caller passes a
``hub_name`` filter:

  ``GET /views/hub_buses?hub_name=WESTERN HUB``
      → ``build_hub_buses_detail_view_model``  — bus list for one aggregate

  ``GET /views/hub_buses``
      → ``build_hub_buses_summary_view_model`` — discovery list of
        aggregates (defaults to type=HUB)
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def _sf(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _si(val) -> Optional[int]:
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


def build_hub_buses_detail_view_model(
    df: pd.DataFrame,
    hub_name: str,
) -> dict:
    """Detail view for a single aggregate.

    Empty df → ``{"hub_name": <requested>, "found": False, ...}`` so the
    caller can detect a typo without sniffing list lengths.
    """
    if df.empty:
        return {
            "hub_name": hub_name,
            "found": False,
            "agg_pnode_id": None,
            "agg_pnode_type": None,
            "bus_count": 0,
            "factor_sum": 0.0,
            "buses": [],
        }

    # All rows in df share the same agg_pnode_id / type (single aggregate).
    first = df.iloc[0]
    buses = [
        {
            "bus_pnode_id": _si(r["bus_pnode_id"]),
            "bus_pnode_name": str(r["bus_pnode_name"]).strip(),
            "bus_pnode_factor": _sf(r["bus_pnode_factor"]),
        }
        for _, r in df.iterrows()
    ]
    return {
        "hub_name": str(first["agg_pnode_name"]).strip(),
        "found": True,
        "agg_pnode_id": _si(first["agg_pnode_id"]),
        "agg_pnode_type": str(first["agg_pnode_type"]),
        "bus_count": len(buses),
        "factor_sum": _sf(df["bus_pnode_factor"].sum()),
        "buses": buses,
    }


def build_hub_buses_summary_view_model(
    df: pd.DataFrame,
    agg_pnode_type: Optional[str],
) -> dict:
    """Discovery summary across all aggregates.

    One row per aggregate with bus_count and factor_sum (sanity invariant
    is factor_sum ≈ 1.0 for properly-defined aggregates).
    """
    aggregates = [
        {
            "agg_pnode_id": _si(r["agg_pnode_id"]),
            "agg_pnode_name": str(r["agg_pnode_name"]).strip(),
            "agg_pnode_type": str(r["agg_pnode_type"]),
            "bus_count": _si(r["bus_count"]),
            "factor_sum": _sf(r["factor_sum"]),
        }
        for _, r in df.iterrows()
    ]
    return {
        "agg_pnode_type_filter": agg_pnode_type,
        "aggregate_count": len(aggregates),
        "aggregates": aggregates,
    }
