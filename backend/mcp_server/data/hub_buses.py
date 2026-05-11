"""Data-access layer for the hub-buses MCP view.

Pulls from ``pjm_da_modelling_cleaned.pjm_agg_definitions_active`` — the
currently-effective subset of PJM's aggregate-pnode → bus-pnode mappings
(SCD2 with `terminate_date_ept = '9999-12-31'` sentinel for active rows).

Use cases:
  - Forward lookup: "which buses compose Western Hub?"
  - Discovery summary: list all HUB / ZONE / etc. aggregates with bus counts
"""

from __future__ import annotations

import logging

import pandas as pd

from backend.settings import DBT_SCHEMA
from backend.utils.azure_postgresql_utils import pull_from_db

logger = logging.getLogger(__name__)

_HUB_BUSES_TABLE = f"{DBT_SCHEMA}.pjm_agg_definitions_active"


def pull_hub_buses(
    hub_name: str,
) -> pd.DataFrame:
    """Buses constituting a single aggregate (case-insensitive name match).

    Returns one row per bus_pnode in the aggregate, with:
      ``agg_pnode_id``, ``agg_pnode_name``, ``agg_pnode_type``,
      ``bus_pnode_id``, ``bus_pnode_name``, ``bus_pnode_factor``,
      ``effective_date_ept``, ``terminate_date_ept``.

    Sorted by ``bus_pnode_factor`` desc — heaviest contributors first.
    Empty DataFrame if the name doesn't match any aggregate.
    """
    query = f"""
        SELECT
            agg_pnode_id
            ,agg_pnode_name
            ,agg_pnode_type
            ,bus_pnode_id
            ,bus_pnode_name
            ,bus_pnode_factor
            ,effective_date_ept
            ,terminate_date_ept
        FROM {_HUB_BUSES_TABLE}
        WHERE UPPER(agg_pnode_name) = UPPER('{hub_name}')
        ORDER BY bus_pnode_factor DESC
    """
    return pull_from_db(query)


def pull_aggregates_summary(
    agg_pnode_type: str | None = None,
) -> pd.DataFrame:
    """Summary of all aggregates with bus counts and factor sums.

    Returns one row per aggregate:
      ``agg_pnode_id``, ``agg_pnode_name``, ``agg_pnode_type``,
      ``bus_count``, ``factor_sum``.

    When ``agg_pnode_type`` is provided (e.g. ``"HUB"``), filters to that
    type — the default for the brief use case is HUB, since the OTHER
    bucket has ~1,200 individual generator/load pnodes that aren't
    aggregation points the trader cares about.

    Sorted by bus_count desc.
    """
    type_filter = ""
    if agg_pnode_type:
        type_filter = f"WHERE UPPER(agg_pnode_type) = UPPER('{agg_pnode_type}')"

    query = f"""
        SELECT
            agg_pnode_id
            ,agg_pnode_name
            ,agg_pnode_type
            ,COUNT(*)::INT                          AS bus_count
            ,ROUND(SUM(bus_pnode_factor)::numeric, 4)::FLOAT AS factor_sum
        FROM {_HUB_BUSES_TABLE}
        {type_filter}
        GROUP BY agg_pnode_id, agg_pnode_name, agg_pnode_type
        ORDER BY bus_count DESC
    """
    return pull_from_db(query)
