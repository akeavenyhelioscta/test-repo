"""View-model builder for the hub-impact MCP endpoint.

Single endpoint shape:

    GET /views/hub_impact?hub_name=X&from_bus=Y&to_bus=Z&shadow_price=W

Returns the hub-weighted shift factor for the branch (Y, Z) under hub
X, and if shadow_price is provided, the estimated hub LMP impact in
$/MWh:

    hub_LMP_impact = shadow_price × hub_isf

Sign convention: positive hub_isf means injection at the hub increases
flow in the from→to direction; for a binding constraint that limits
flow in that direction, the LMP at the hub increases by
`shadow × hub_isf`. Negative hub_isf means the hub injection RELIEVES
that constraint — binding actually pushes the hub's LMP down.

The endpoint is designed for brief subagents to call once per top-N
binding constraint (typically 5-10 calls per brief run). Cache read
is in-memory after first call.
"""

from __future__ import annotations

from typing import Optional


def build_hub_impact_view_model(
    hub_name: str,
    from_bus: int,
    to_bus: int,
    shadow_price: Optional[float],
    isf_record: Optional[dict],
) -> dict:
    """Compose the response dict.

    isf_record is the dict returned by data.hub_impact.lookup_hub_isf,
    or None when the hub isn't cached or the branch isn't matched.
    """
    if isf_record is None:
        return {
            "hub_name": hub_name,
            "from_bus": from_bus,
            "to_bus": to_bus,
            "matched": False,
            "hub_isf": None,
            "shadow_price": shadow_price,
            "hub_lmp_impact_per_dollar": None,
            "hub_lmp_impact_dollars_per_mwh": None,
            "note": (
                "Hub not in cache, or branch not found in PSS/E network model. "
                "Common causes: hub_name typo (call /views/hub_buses to list "
                "available hubs), or post-2021 substation outside the .raw "
                "model scope (e.g. MARS2, DUMONT2). Hub LMP impact cannot be "
                "estimated."
            ),
        }

    hub_isf = isf_record["hub_isf"]
    impact = (shadow_price * hub_isf) if shadow_price is not None else None

    return {
        "hub_name": isf_record["hub_name"],
        "from_bus": isf_record["from_bus"],
        "to_bus": isf_record["to_bus"],
        "matched": True,
        "ckt_id": isf_record["ckt_id"],
        "equipment_type": isf_record["equipment_type"],
        "n_parallel_circuits": isf_record["n_parallel_circuits"],
        "hub_isf": hub_isf,
        "abs_hub_isf": isf_record["abs_hub_isf"],
        "shadow_price": shadow_price,
        "hub_lmp_impact_per_dollar": hub_isf,
        "hub_lmp_impact_dollars_per_mwh": impact,
        "magnitude_class": (
            "HIGH"
            if isf_record["abs_hub_isf"] >= 0.05
            else "MED"
            if isf_record["abs_hub_isf"] >= 0.01
            else "LOW"
        ),
    }
