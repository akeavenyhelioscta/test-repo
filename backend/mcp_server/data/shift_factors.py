"""DC shift-factor computation for hub-LMP attribution.

Computes per-branch Western-Hub-weighted Injection Shift Factors (ISFs)
from PJM's PSS/E .raw network model. The output is a single scalar per
branch — the partial derivative of branch flow with respect to a unit
of energy injected at the WH aggregate (and withdrawn at slack).

Use case: given a binding DA constraint with shadow price $S/MWh on
branch (i, j), the WH-LMP impact is approximately:

    WH_impact_$/MWh ~= S × wh_isf[(i, j)]

where `wh_isf[(i, j)]` is the WH-weighted shift factor stored in the
cache table this module produces.

## Pipeline

```
.raw file
    ↓ parse_branch_reactances()  — X values (lines + transformers)
    ↓ + parse_psse_raw.parse()   — bus + branch metadata
build_b_matrix()                 — sparse N×N susceptance
    ↓ scipy.sparse.linalg.splu()
factored B
    ↓ + WH bus list + factors    — pulled from agg_definitions mart
    ↓ name-fuzzy match
WH injection vector              — Σ_b factor_b × e_b (sums to 1)
    ↓ B θ = e   (one solve)
phase angles
    ↓ for each branch: (θ[i] - θ[j]) / x_ij
WH-weighted ISF per branch
    ↓ saved to network/wh_branch_weights.parquet
```

## Approximations (first iteration)

- DC power flow (linear, lossless, ignores reactive power). Standard
  approximation for shift factor work — accurate within a few % for
  most network states.
- 3-winding transformers reduced to single branch using winding 1-2
  reactance only (tertiary ignored). Affects a small minority of
  facilities, mostly small auto-transformers.
- Phase shifters and HVDC ties treated as standard AC branches with
  their nominal reactance. Refinable later if needed.
- Out-of-service branches (status != 1) excluded from B.
- Pnode → PSS/E bus matching is fuzzy (substring + voltage-class
  match). Coverage typically 80-90% on WH's 89 generator buses;
  unmatched buses are excluded from the WH weight, which slightly
  underestimates impact for branches near them.

## Cache

Output: `backend/mcp_server/data/network/wh_branch_weights.parquet`
Schema:
    from_bus       int
    to_bus         int
    ckt_id         str
    equipment_type str       LINE / XFMR
    x_pu           float     reactance used
    wh_isf         float     WH-weighted shift factor
    abs_wh_isf     float     |wh_isf| — sort by this for ranking

Re-run when the .raw model changes or WH membership shifts (rare —
once a year typically).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp
from scipy.sparse.linalg import splu

from backend.mcp_server.data.parse_psse_raw import (
    RAW_FILE,
    _section_bounds,
    _split_record,
    parse,
)
from backend.settings import DBT_SCHEMA
from backend.utils.azure_postgresql_utils import pull_from_db

logger = logging.getLogger(__name__)


CACHE_PATH = Path(__file__).parent / "network" / "hub_branch_weights.parquet"


# ─── Reactance extraction (.raw second pass) ─────────────────────────────────


def parse_branch_reactances(
    raw_file: Path = RAW_FILE,
) -> pd.DataFrame:
    """Re-parse the .raw to extract per-branch reactance + status.

    Returns one row per (from_bus, to_bus, ckt_id, equipment_type) with:
        from_bus, to_bus, ckt_id, equipment_type, x_pu, in_service.

    Lines: x_pu = field 4. Status = field 13.
    Transformers (2-winding): x_pu from L2 (impedance line) field 1.
    Transformers (3-winding): x_pu from L2 field 1 (winding 1-2 leg only).
    """
    if not raw_file.exists():
        raise FileNotFoundError(f"PSS/E .raw not found: {raw_file}")

    lines = raw_file.read_text(encoding="latin-1").splitlines()
    bounds = _section_bounds(lines)

    rows: list[dict] = []

    # ── LINE branches ─────────────────────────────────────────────────────
    br_start, br_end = bounds["Branch"]
    for i in range(br_start, br_end):
        try:
            f = _split_record(lines[i])
            if len(f) < 14:
                continue
            from_bus = abs(int(f[0].strip()))
            to_bus = abs(int(f[1].strip()))
            ckt = f[2].strip()
            x_pu = float(f[4])
            status = int(f[13].strip()) if f[13].strip() else 1
            if x_pu == 0:
                continue  # zero-impedance branch (bus tie); skip
            rows.append(
                {
                    "from_bus": from_bus,
                    "to_bus": to_bus,
                    "ckt_id": ckt,
                    "equipment_type": "LINE",
                    "x_pu": x_pu,
                    "in_service": status == 1,
                }
            )
        except (ValueError, IndexError) as e:
            logger.debug(f"branch X parse skip line {i}: {e!r}")

    # ── Transformers ──────────────────────────────────────────────────────
    xf_start, xf_end = bounds["Transformer"]
    i = xf_start
    while i < xf_end:
        try:
            header = _split_record(lines[i])
            if len(header) < 12:
                i += 1
                continue
            from_bus = abs(int(header[0].strip()))
            to_bus = abs(int(header[1].strip()))
            third = abs(int(header[2].strip()))
            ckt = header[3].strip()
            stat = int(header[11].strip()) if header[11].strip() else 1

            impedance_line = _split_record(lines[i + 1])
            x_pu = float(impedance_line[1])

            if x_pu != 0:
                rows.append(
                    {
                        "from_bus": from_bus,
                        "to_bus": to_bus,
                        "ckt_id": ckt,
                        "equipment_type": "XFMR",
                        "x_pu": x_pu,
                        "in_service": stat in (1,),
                    }
                )
            i += 4 if third == 0 else 5
        except (ValueError, IndexError):
            i += 4

    df = pd.DataFrame(rows)
    logger.info(
        f"parsed reactances: {len(df):,} branches "
        f"({(df['equipment_type'] == 'LINE').sum():,} LINE, "
        f"{(df['equipment_type'] == 'XFMR').sum():,} XFMR), "
        f"{df['in_service'].sum():,} in-service"
    )
    return df


# ─── Load extraction (for distributed slack) ─────────────────────────────────


def parse_bus_loads(raw_file: Path = RAW_FILE) -> pd.DataFrame:
    """Re-parse the .raw to extract per-bus active power load (PL).

    Returns one row per bus with total in-service constant-power load
    in MW (sums multiple LOAD records at the same bus).

    PSS/E v30 LOAD record fields:
        I, ID, STATUS, AREA, ZONE, PL, QL, IP, IQ, YP, YQ, OWNER, ...
        Field 5 (0-indexed) = PL (active constant-power load, MW).
    """
    lines = raw_file.read_text(encoding="latin-1").splitlines()
    bounds = _section_bounds(lines)
    if "Load" not in bounds:
        raise RuntimeError("no Load section found in .raw")
    start, end = bounds["Load"]

    rows: list[dict] = []
    for i in range(start, end):
        try:
            f = _split_record(lines[i])
            if len(f) < 6:
                continue
            bus = abs(int(f[0].strip()))
            status = int(f[2].strip()) if f[2].strip() else 1
            pl = float(f[5])
            if status == 1 and pl > 0:
                rows.append({"bus_id": bus, "load_mw": pl})
        except (ValueError, IndexError):
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Sum multiple loads at same bus
    df = df.groupby("bus_id", as_index=False)["load_mw"].sum()
    logger.info(
        f"parsed loads: {len(df):,} buses with active load, "
        f"total = {df['load_mw'].sum():,.0f} MW"
    )
    return df


# ─── B matrix + factorization ─────────────────────────────────────────────────


def build_b_matrix(
    buses_df: pd.DataFrame,
    reactances_df: pd.DataFrame,
) -> tuple[sp.csc_matrix, dict[int, int], np.ndarray, np.ndarray]:
    """Build the bus susceptance matrix B (DC power flow), reduced by
    one slack per connected component plus isolated buses.

    The PSS/E .raw includes the main PJM grid plus external/boundary
    sub-networks. With multiple connected components, B has one
    nullspace dimension per component — singular until we drop one
    slack per component.

    Returns (B_reduced, bus_id_to_idx, kept_mask, components):
      - B_reduced: sparse CSC, square, invertible
      - bus_id_to_idx: PSS/E bus_id → original matrix row index
      - kept_mask: bool array of size N; True = bus kept in reduced B
      - components: array of size N; component label per bus
    """
    from scipy.sparse.csgraph import connected_components

    bus_ids = sorted(buses_df["bus_id"].astype(int).unique().tolist())
    bus_id_to_idx = {b: i for i, b in enumerate(bus_ids)}
    n = len(bus_ids)

    in_service = reactances_df[reactances_df["in_service"]].copy()
    in_service = in_service[
        in_service["from_bus"].isin(bus_id_to_idx)
        & in_service["to_bus"].isin(bus_id_to_idx)
    ].reset_index(drop=True)
    in_service = in_service[in_service["from_bus"] != in_service["to_bus"]].reset_index(
        drop=True
    )

    in_service["b_pu"] = 1.0 / in_service["x_pu"]

    rows = in_service["from_bus"].map(bus_id_to_idx).to_numpy()
    cols = in_service["to_bus"].map(bus_id_to_idx).to_numpy()
    data = in_service["b_pu"].to_numpy()

    off_i = np.concatenate([rows, cols])
    off_j = np.concatenate([cols, rows])
    off_d = np.concatenate([-data, -data])

    diag = np.zeros(n)
    np.add.at(diag, rows, data)
    np.add.at(diag, cols, data)

    B = sp.coo_matrix((off_d, (off_i, off_j)), shape=(n, n)).tocsr()
    B = B + sp.diags(diag, format="csr")

    # Adjacency for component detection (any non-zero off-diagonal counts)
    adj = sp.coo_matrix((np.ones_like(off_d), (off_i, off_j)), shape=(n, n)).tocsr()
    n_components, labels = connected_components(adj, directed=False)

    # Pick the lowest-index connected bus in each component as that
    # component's slack. Isolated buses (degree 0) need no slack.
    diag_arr = np.asarray(diag)
    slack_idxs: list[int] = []
    for c in range(n_components):
        in_c = np.where(labels == c)[0]
        connected = in_c[diag_arr[in_c] > 0]
        if len(connected) == 0:
            continue
        slack_idxs.append(int(connected[0]))

    keep = np.ones(n, dtype=bool)
    for s in slack_idxs:
        keep[s] = False
    isolated = np.where(diag_arr == 0)[0]
    for i in isolated:
        keep[i] = False

    B_reduced = B[keep][:, keep].tocsc()

    logger.info(
        f"B matrix: N={n} buses, {in_service.shape[0]:,} in-service branches; "
        f"{n_components} connected components; "
        f"{len(slack_idxs)} slacks dropped; {len(isolated)} isolated buses dropped; "
        f"reduced shape={B_reduced.shape}"
    )
    return B_reduced, bus_id_to_idx, keep, labels


# ─── Pnode → PSS/E bus name matching ─────────────────────────────────────────


# Voltage regex: NO leading \b (pnode names embed voltage without space,
# e.g. 'DICKERSH13 KV'). Allows decimal voltages (13.8, 34.5).
_VOLTAGE_KV_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*KV", re.IGNORECASE)

# Trailing tokens that PJM appends to pnode names but PSS/E doesn't
# carry on the bus_name: 'EHV', 'LMP', 'GEN', 'LOAD', 'PSE', etc.
# Strip these after voltage strip.
_TRAILING_SUFFIX_RE = re.compile(r"(EHV|LMP|GEN|LOAD|PSE|HUB|RES)\s*$")


def _normalize_bus_name(name: str) -> str:
    """Extract the substation token from a name, comparable across
    PSS/E bus_names and PJM pnode_names.

    PSS/E bus_name format: 'STATION  ' (≤8 chars + padding).
    PJM pnode_name format: 'STATION<KV><space>KV<space><UNIT>',
        e.g. 'DICKERSH13 KV   HCT1', 'KEYSTONEEHV     LMP'.

    Steps:
      1. Uppercase, strip whitespace.
      2. Drop everything from the voltage match onward.
      3. Strip trailing letter clusters like 'EHV', 'LMP'.
      4. Strip trailing digits (e.g., 'KEYSTONE2' → 'KEYSTONE').
      5. Truncate to first 8 chars (matches PSS/E .raw NAME width).
    """
    if not name:
        return ""
    upper = name.upper().strip()
    m = _VOLTAGE_KV_RE.search(upper)
    if m:
        upper = upper[: m.start()]
    # Strip trailing EHV/LMP/etc. suffix
    upper = _TRAILING_SUFFIX_RE.sub("", upper).strip()
    # Strip trailing digits
    upper = re.sub(r"\d+$", "", upper).strip()
    # PSS/E bus_name is ≤8 chars, so truncate match key
    return upper[:8].strip()


def _extract_voltage_kv(name: str) -> float | None:
    """Return the voltage embedded in a name as float kV, or None."""
    if not name:
        return None
    m = _VOLTAGE_KV_RE.search(name.upper())
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def match_pnode_to_psse(
    wh_buses_df: pd.DataFrame,
    psse_buses_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict]]:
    """Match WH bus_pnode_names → PSS/E bus_ids by normalized substation
    + voltage class (within ±1 kV bucket).

    Returns (matched_df, unmatched_list). matched_df has one row per
    matched WH bus_pnode_id with the assigned PSS/E bus_id and
    bus_pnode_factor.
    """
    psse = psse_buses_df.copy()
    psse["norm"] = psse["bus_name"].apply(_normalize_bus_name)
    psse["kv"] = psse["voltage_kv"].astype(float)

    matched: list[dict] = []
    unmatched: list[dict] = []

    def _kv_close(a: float, b: float) -> bool:
        # ±2 kV absolute OR within 5% — handles 13/13.8, 23/22, 34.5/35
        return abs(a - b) <= 2.0 or abs(a - b) / max(a, b, 1) <= 0.05

    for _, r in wh_buses_df.iterrows():
        pname = str(r["bus_pnode_name"])
        norm = _normalize_bus_name(pname)
        kv = _extract_voltage_kv(pname)

        if not norm:
            unmatched.append(
                {
                    "bus_pnode_id": r["bus_pnode_id"],
                    "name": pname,
                    "reason": "blank-after-normalize",
                }
            )
            continue

        # Stage 1: full normalized name + voltage match
        if kv is not None:
            cand = psse[
                (psse["norm"] == norm)
                & psse["kv"].apply(lambda b, k=kv: _kv_close(b, k))
            ]
        else:
            cand = psse[psse["norm"] == norm]
        if len(cand) >= 1:
            picked = cand.sort_values("bus_id").iloc[0]
            matched.append(
                {
                    "bus_pnode_id": int(r["bus_pnode_id"]),
                    "bus_pnode_name": pname,
                    "bus_pnode_factor": float(r["bus_pnode_factor"]),
                    "psse_bus_id": int(picked["bus_id"]),
                    "psse_bus_name": picked["bus_name"],
                    "psse_kv": float(picked["voltage_kv"]),
                    "match_stage": "exact_kv" if kv else "exact",
                }
            )
            continue

        # Stage 2: prefix (first 6 chars) + voltage
        if len(norm) >= 6 and kv is not None:
            prefix = norm[:6]
            cand = psse[
                psse["norm"].str.startswith(prefix)
                & psse["kv"].apply(lambda b, k=kv: _kv_close(b, k))
            ]
            if len(cand) >= 1:
                picked = cand.sort_values("bus_id").iloc[0]
                matched.append(
                    {
                        "bus_pnode_id": int(r["bus_pnode_id"]),
                        "bus_pnode_name": pname,
                        "bus_pnode_factor": float(r["bus_pnode_factor"]),
                        "psse_bus_id": int(picked["bus_id"]),
                        "psse_bus_name": picked["bus_name"],
                        "psse_kv": float(picked["voltage_kv"]),
                        "match_stage": "prefix_kv",
                    }
                )
                continue

        # Stage 3: name-only match (no kv) — last resort, pick highest-kV
        # candidate (the substation's hub bus is usually the high-side)
        if len(norm) >= 6:
            prefix = norm[:6]
            cand = psse[psse["norm"].str.startswith(prefix)]
            if len(cand) >= 1:
                picked = cand.sort_values("kv", ascending=False).iloc[0]
                matched.append(
                    {
                        "bus_pnode_id": int(r["bus_pnode_id"]),
                        "bus_pnode_name": pname,
                        "bus_pnode_factor": float(r["bus_pnode_factor"]),
                        "psse_bus_id": int(picked["bus_id"]),
                        "psse_bus_name": picked["bus_name"],
                        "psse_kv": float(picked["voltage_kv"]),
                        "match_stage": "prefix_only",
                    }
                )
                continue

        unmatched.append(
            {
                "bus_pnode_id": int(r["bus_pnode_id"]),
                "name": pname,
                "reason": "no-match",
            }
        )

    matched_df = pd.DataFrame(matched)
    logger.info(
        f"WH match: {len(matched_df)}/{len(wh_buses_df)} matched "
        f"({100 * len(matched_df) / max(1, len(wh_buses_df)):.1f}%); "
        f"unmatched: {len(unmatched)}"
    )
    return matched_df, unmatched


# ─── Hub branch weights — main entry point ────────────────────────────────────


_DEFAULT_HUBS_QUERY = f"""
    SELECT DISTINCT agg_pnode_name
    FROM {DBT_SCHEMA}.pjm_agg_definitions_active
    WHERE agg_pnode_type = 'HUB'
    ORDER BY agg_pnode_name
"""


def _pull_hub_buses(hub_name: str) -> pd.DataFrame:
    """Read a single aggregate's bus membership from the dbt mart."""
    q = f"""
        SELECT agg_pnode_id, bus_pnode_id, bus_pnode_name, bus_pnode_factor
        FROM {DBT_SCHEMA}.pjm_agg_definitions_active
        WHERE agg_pnode_name = '{hub_name}'
        ORDER BY bus_pnode_factor DESC
    """
    return pull_from_db(q)


def _solve_one_hub(
    hub_name: str,
    buses_df: pd.DataFrame,
    in_service: pd.DataFrame,
    bus_id_to_idx: dict[int, int],
    n: int,
    keep_mask: np.ndarray,
    components: np.ndarray,
    B_lu,
    load_vec_full: np.ndarray,
) -> tuple[pd.DataFrame, dict]:
    """Compute per-branch ISFs for a single hub.

    Reuses the pre-computed B_lu factorization and the full-network
    load vector (clipped per-component below). Returns
    (per_branch_df, per_hub_summary).
    """
    summary: dict = {"hub_name": hub_name}

    hub_buses = _pull_hub_buses(hub_name)
    if hub_buses is None or hub_buses.empty:
        raise RuntimeError(
            f"Could not pull buses for hub '{hub_name}' from "
            f"{DBT_SCHEMA}.pjm_agg_definitions_active."
        )
    matched_df, unmatched = match_pnode_to_psse(hub_buses, buses_df)
    summary["buses_total"] = len(hub_buses)
    summary["buses_matched"] = len(matched_df)
    summary["buses_unmatched"] = len(unmatched)
    summary["factor_matched"] = (
        float(matched_df["bus_pnode_factor"].sum()) if not matched_df.empty else 0.0
    )

    if matched_df.empty:
        raise RuntimeError(f"hub '{hub_name}': no buses matched in PSS/E")

    agg_pnode_id_val = (
        int(hub_buses["agg_pnode_id"].iloc[0])
        if "agg_pnode_id" in hub_buses.columns and not hub_buses.empty
        else None
    )

    # Hub injection vector at the FULL bus count
    hub_inj = np.zeros(n)
    for _, r in matched_df.iterrows():
        idx = bus_id_to_idx.get(int(r["psse_bus_id"]))
        if idx is None:
            continue
        hub_inj[idx] += float(r["bus_pnode_factor"])

    # Identify the main connected component spanned by this hub
    hub_components_set = set(
        components[idx]
        for idx in [
            bus_id_to_idx.get(int(r["psse_bus_id"])) for _, r in matched_df.iterrows()
        ]
        if idx is not None
    )
    summary["components_spanned"] = len(hub_components_set)
    main_component = (
        max(hub_components_set, key=lambda c: int(np.sum(components == c)))
        if hub_components_set
        else 0
    )

    # Distributed slack restricted to load buses in the same component
    load_vec = load_vec_full.copy()
    load_vec[components != main_component] = 0
    total_in_component = float(load_vec.sum())
    if total_in_component <= 0:
        raise RuntimeError(
            f"hub '{hub_name}': no in-component load for distributed slack"
        )
    load_dist = load_vec / total_in_component
    summary["distributed_slack_mw_total"] = total_in_component

    # Solve B θ = (hub_inj - load_dist)
    net_inj = hub_inj - load_dist
    inj_reduced = net_inj[keep_mask]
    theta_reduced = B_lu.solve(inj_reduced)

    theta = np.zeros(n)
    theta[keep_mask] = theta_reduced

    # Per-branch ISF
    from_idx = in_service["from_bus"].map(bus_id_to_idx).to_numpy()
    to_idx = in_service["to_bus"].map(bus_id_to_idx).to_numpy()
    hub_isf = (theta[from_idx] - theta[to_idx]) / in_service["x_pu"].to_numpy()

    df = pd.DataFrame(
        {
            "agg_pnode_name": hub_name,
            "agg_pnode_id": agg_pnode_id_val,
            "from_bus": in_service["from_bus"].values,
            "to_bus": in_service["to_bus"].values,
            "ckt_id": in_service["ckt_id"].values,
            "equipment_type": in_service["equipment_type"].values,
            "x_pu": in_service["x_pu"].values,
            "hub_isf": hub_isf,
            "abs_hub_isf": np.abs(hub_isf),
        }
    )

    summary["max_abs_hub_isf"] = float(df["abs_hub_isf"].max())
    summary["mean_abs_hub_isf"] = float(df["abs_hub_isf"].mean())

    return df, summary


def build_hub_branch_weights(
    hub_names: list[str] | None = None,
    raw_file: Path = RAW_FILE,
    save: bool = True,
) -> tuple[pd.DataFrame, dict]:
    """End-to-end build: parse, factor B once, solve per-hub, save cache.

    By default, computes all HUB-typed aggregates from the dbt mart
    (currently 12 PJM hubs). Pass `hub_names=['WESTERN HUB']` to
    restrict for testing.

    Returns (weights_df, summary_dict). weights_df has one row per
    (hub × in-service branch) with hub_isf.
    """
    overall: dict = {"per_hub": {}}

    # 1. Parse network + reactances
    buses_df, _ = parse(raw_file)
    reactances_df = parse_branch_reactances(raw_file)

    # 2. Build + factor B (one factor reused for all hubs)
    B, bus_id_to_idx, keep_mask, components = build_b_matrix(buses_df, reactances_df)
    bus_ids = sorted(bus_id_to_idx.keys())
    n = len(bus_ids)
    overall["n_buses"] = n
    overall["n_branches_in_service"] = int(reactances_df["in_service"].sum())
    overall["n_connected_components"] = int(components.max() + 1)
    overall["n_buses_kept_in_reduced_B"] = int(keep_mask.sum())

    logger.info("factoring B (sparse LU)...")
    B_lu = splu(B)

    # 3. Pre-compute full-network load vector
    loads_df = parse_bus_loads(raw_file)
    load_vec_full = np.zeros(n)
    if not loads_df.empty:
        for _, r in loads_df.iterrows():
            idx = bus_id_to_idx.get(int(r["bus_id"]))
            if idx is None:
                continue
            load_vec_full[idx] = float(r["load_mw"])

    in_service = reactances_df[reactances_df["in_service"]].copy()

    # 4. Determine which hubs to process
    if hub_names is None:
        hubs_df = pull_from_db(_DEFAULT_HUBS_QUERY)
        if hubs_df is None or hubs_df.empty:
            raise RuntimeError(
                "No HUB-typed aggregates found in the dbt mart. "
                "Did the agg_definitions scrape + dbt build run?"
            )
        hub_names = hubs_df["agg_pnode_name"].tolist()
    overall["n_hubs"] = len(hub_names)
    overall["hub_names"] = list(hub_names)
    logger.info(f"computing shift factors for {len(hub_names)} hubs")

    # 5. Solve per-hub (each is one cheap sparse solve against the same B_lu)
    per_hub_dfs: list[pd.DataFrame] = []
    for h in hub_names:
        logger.info(f"  hub={h}")
        df, summary = _solve_one_hub(
            hub_name=h,
            buses_df=buses_df,
            in_service=in_service,
            bus_id_to_idx=bus_id_to_idx,
            n=n,
            keep_mask=keep_mask,
            components=components,
            B_lu=B_lu,
            load_vec_full=load_vec_full,
        )
        per_hub_dfs.append(df)
        overall["per_hub"][h] = summary

    weights = pd.concat(per_hub_dfs, ignore_index=True)
    overall["n_branch_weights_total"] = len(weights)

    if save:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        weights.to_parquet(CACHE_PATH, index=False)
        logger.info(
            f"saved hub branch weights to {CACHE_PATH} "
            f"({len(weights):,} rows across {len(hub_names)} hubs)"
        )

    return weights, overall


def load_hub_branch_weights() -> pd.DataFrame:
    """Quick read of the cached multi-hub branch weights table."""
    if not CACHE_PATH.exists():
        raise FileNotFoundError(
            f"Hub branch weights cache not found at {CACHE_PATH}. "
            "Run `python -m backend.mcp_server.data.shift_factors` to build it."
        )
    return pd.read_parquet(CACHE_PATH)


# ─── CLI ─────────────────────────────────────────────────────────────────────


def run() -> None:
    """Build the cache and print per-hub sanity-check leaderboards."""
    import sys

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )

    weights, overall = build_hub_branch_weights()

    print()
    print("=== Build summary ===")
    print(f"  n_buses:                {overall['n_buses']}")
    print(f"  n_branches_in_service:  {overall['n_branches_in_service']}")
    print(f"  n_connected_components: {overall['n_connected_components']}")
    print(f"  n_buses_kept_in_B:      {overall['n_buses_kept_in_reduced_B']}")
    print(f"  n_hubs:                 {overall['n_hubs']}")
    print(f"  n_branch_weights_total: {overall['n_branch_weights_total']}")
    print()
    print("=== Per-hub coverage + |hub_isf| stats ===")
    print(f"  {'hub':<22s} {'matched':>9s} {'factor':>8s} {'max':>8s} {'mean':>9s}")
    for hub, s in overall["per_hub"].items():
        cov = f"{s['buses_matched']}/{s['buses_total']}"
        print(
            f"  {hub:<22s} {cov:>9s} {s['factor_matched']:>8.4f} "
            f"{s['max_abs_hub_isf']:>8.4f} {s['mean_abs_hub_isf']:>9.5f}"
        )
    print()
    print("=== Top 5 branches by |hub_isf| per hub ===")
    for hub in overall["per_hub"].keys():
        top = (
            weights[weights["agg_pnode_name"] == hub]
            .sort_values("abs_hub_isf", ascending=False)
            .head(5)
        )
        print(f"\n--- {hub} ---")
        print(
            top[
                ["from_bus", "to_bus", "ckt_id", "equipment_type", "hub_isf"]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    run()
