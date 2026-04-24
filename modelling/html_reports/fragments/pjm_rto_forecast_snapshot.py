"""PJM RTO + zonal forecast snapshot — latest forecast only.

Ported from helioscta-pjm-da pjm_rto_forecast_snapshot.py, with vintages dropped.

Entry points (registered separately in generate_report.py):
    build_fragments          — RTO (load + solar + wind + net load)
    build_fragments_west     — Western (load only)
    build_fragments_midatl   — Mid-Atlantic (load only)
    build_fragments_south    — Southern (load only)

For every region we produce:
  1. All-dates overview chart (stacked area + ramp for RTO, load + ramp otherwise).
  2. One section per forecast date:
       * snapshot table with HE1–HE24 + OnPeak/OffPeak/Flat summaries
         and an outright/ramp JS toggle
       * chart row: net load stacked area + net load ramp (RTO)
                    or load profile + load ramp (zones)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from da_models.common import configs
from da_models.common.data.loader import (
    load_load_forecast,
    load_solar_forecast,
    load_wind_forecast,
)
from html_reports.fragments._forecast_utils import (
    HE_COLS, OFFPEAK_HOURS, ONPEAK_HOURS, PLOTLY_LOCKED_CONFIG, PLOTLY_TEMPLATE,
    SNAPSHOT_STYLE, SUMMARY_COLS,
    cell_class, date_key, day_series, empty_html, fmt_cell, prep_hours,
    render_ramp_toggle,
)
from utils.logging_utils import get_logger

logger = get_logger()

ET = ZoneInfo("America/New_York")

Section = tuple[str, Any, str | None]


# ══════════════════════════════════════════════════════════════════════
# Public entry points
# ══════════════════════════════════════════════════════════════════════


def build_fragments(
    schema: str | None = None,
    cache_dir: Path | None = None,
    cache_enabled: bool = True,
    cache_ttl_hours: float = 24.0,
    force_refresh: bool = False,
) -> list[Section]:
    """RTO snapshot: load + solar + wind + net load."""
    return _build_for_region("RTO", "RTO", cache_dir=cache_dir or configs.CACHE_DIR)


def build_fragments_west(**kwargs) -> list[Section]:
    return _build_for_region("WEST", "Western", cache_dir=kwargs.get("cache_dir") or configs.CACHE_DIR)


def build_fragments_midatl(**kwargs) -> list[Section]:
    return _build_for_region("MIDATL", "Mid-Atlantic", cache_dir=kwargs.get("cache_dir") or configs.CACHE_DIR)


def build_fragments_south(**kwargs) -> list[Section]:
    return _build_for_region("SOUTH", "Southern", cache_dir=kwargs.get("cache_dir") or configs.CACHE_DIR)


# ══════════════════════════════════════════════════════════════════════
# Orchestration
# ══════════════════════════════════════════════════════════════════════


def _build_for_region(region_key: str, region_label: str, *, cache_dir: Path) -> list[Section]:
    is_rto = region_key == "RTO"
    logger.info(f"Building PJM {region_label} forecast snapshot...")

    try:
        load_all = load_load_forecast(cache_dir=cache_dir)
    except FileNotFoundError as exc:
        logger.warning(f"Load forecast parquet not found: {exc}")
        return [(f"PJM {region_label} — Unavailable", empty_html(str(exc)), None)]

    load_df = prep_hours(load_all[load_all["region"] == region_key].copy())
    if load_df.empty:
        return [(f"PJM {region_label} — No Data",
                 empty_html(f"No load rows for {region_label}."), None)]

    if is_rto:
        solar_df = _maybe_load(load_solar_forecast, cache_dir, "solar")
        wind_df = _maybe_load(load_wind_forecast, cache_dir, "wind")
    else:
        solar_df = pd.DataFrame()
        wind_df = pd.DataFrame()

    available_dates = sorted(load_df["date"].unique())
    sections: list[Section] = []

    # Overview across all dates
    if is_rto:
        overview = _rto_overview(load_df, solar_df, wind_df, available_dates)
    else:
        overview = _zone_overview(load_df, available_dates, region_label)
    sections.append((f"All Forecast Dates — {region_label}", overview, None))

    # Per-date sections
    today = pd.Timestamp.now(ET).date()
    for dt in available_dates:
        day_offset = (dt - today).days
        dt_label = pd.Timestamp(dt).strftime("%a %b %d")
        suffix = f"(Day +{day_offset})" if day_offset >= 0 else f"(Day {day_offset})"
        section_name = f"{dt_label} {suffix}"
        dt_key = f"{region_key.lower()}-{date_key(dt)}"

        day_df = _build_day_frame(
            load_df=load_df, solar_df=solar_df, wind_df=wind_df,
            target_date=dt, include_renewables=is_rto,
        )

        content = SNAPSHOT_STYLE
        content += _render_snapshot_table(day_df, dt_key, is_rto=is_rto)
        if is_rto:
            content += _render_net_load_row(day_df, dt_key)
        else:
            content += _render_load_only_row(day_df, dt_key, region_label)

        sections.append((section_name, content, None))

    return sections


def _maybe_load(loader_fn, cache_dir: Path, label: str) -> pd.DataFrame:
    try:
        return prep_hours(loader_fn(cache_dir=cache_dir))
    except FileNotFoundError as exc:
        logger.warning(f"{label} forecast parquet not found: {exc}")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════
# RTO overview — stacked area + grouped ramp bars across all dates
# ══════════════════════════════════════════════════════════════════════


def _rto_overview(
    load_df: pd.DataFrame, solar_df: pd.DataFrame, wind_df: pd.DataFrame, dates: list,
) -> str:
    rows = []
    for dt in dates:
        load_s = day_series(load_df, dt, "forecast_load_mw")
        solar_s = day_series(solar_df, dt, "solar_forecast")
        wind_s = day_series(wind_df, dt, "wind_forecast")
        if load_s.empty:
            continue
        for h in range(1, 25):
            ld = load_s.get(h)
            sl = solar_s.get(h)
            wn = wind_s.get(h)
            if pd.isna(ld):
                continue
            sl = 0.0 if pd.isna(sl) else sl
            wn = 0.0 if pd.isna(wn) else wn
            rows.append({
                "datetime": pd.Timestamp(dt) + pd.Timedelta(hours=h),
                "date_label": pd.Timestamp(dt).strftime("%a %b-%d"),
                "he": h, "load": ld, "solar": sl, "wind": wn,
                "net_load": ld - sl - wn,
            })
    if not rows:
        return empty_html("No net load data.")

    df = pd.DataFrame(rows).sort_values("datetime")
    for col in ["load", "solar", "wind", "net_load"]:
        df[f"{col}_ramp"] = df[col].diff()
    day_boundaries = df["date_label"] != df["date_label"].shift(1)
    for col in ["load_ramp", "solar_ramp", "wind_ramp", "net_load_ramp"]:
        df.loc[day_boundaries, col] = None

    fig = go.Figure()
    cd = df[["date_label", "he"]].values

    outright_indices: list[int] = []
    ramp_indices: list[int] = []

    stack_traces = [
        ("Net Load", "net_load", "#60a5fa", "rgba(96, 165, 250, 0.50)"),
        ("Solar",    "solar",    "#fbbf24", "rgba(251, 191, 36, 0.40)"),
        ("Wind",     "wind",     "#34d399", "rgba(52, 211, 153, 0.35)"),
    ]
    for name, col, line_c, fill_c in stack_traces:
        outright_indices.append(len(fig.data))
        fig.add_trace(go.Scatter(
            x=df["datetime"], y=df[col], mode="lines", name=name,
            stackgroup="stack",
            line=dict(color=line_c, width=1), fillcolor=fill_c,
            customdata=cd,
            hovertemplate=f"<b>%{{customdata[0]}}</b> HE %{{customdata[1]}}<br>{name}: %{{y:,.0f}} MW<extra></extra>",
        ))

    outright_indices.append(len(fig.data))
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["load"], mode="lines", name="Load",
        line=dict(color="#f8fafc", width=2),
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Load: %{y:,.0f} MW<extra></extra>",
    ))

    ramp_traces = [
        ("Load Ramp",     "load_ramp",     "#f8fafc"),
        ("Net Load Ramp", "net_load_ramp", "#60a5fa"),
        ("Solar Ramp",    "solar_ramp",    "#fbbf24"),
        ("Wind Ramp",     "wind_ramp",     "#34d399"),
    ]
    for name, col, color in ramp_traces:
        ramp_indices.append(len(fig.data))
        fig.add_trace(go.Bar(
            x=df["datetime"], y=df[col],
            name=name, marker_color=color, opacity=0.8,
            customdata=cd, visible=False,
            hovertemplate=f"<b>%{{customdata[0]}}</b> HE %{{customdata[1]}}<br>{name}: %{{y:+,.0f}} MW/hr<extra></extra>",
        ))

    fig.update_layout(
        title="Net Load Breakdown",
        height=500, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=60, b=60),
        hovermode="x unified", barmode="group", bargap=0.15,
    )
    fig.update_xaxes(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)")
    fig.update_yaxes(title_text="MW", tickformat=".1s", gridcolor="rgba(99,110,250,0.1)")

    return render_ramp_toggle(
        fig, div_id="pjm-rto-allnetload",
        outright_indices=outright_indices,
        ramp_indices=ramp_indices,
    )


# ══════════════════════════════════════════════════════════════════════
# Zone overview — load + load ramp across all dates
# ══════════════════════════════════════════════════════════════════════


def _zone_overview(load_df: pd.DataFrame, dates: list, region_label: str) -> str:
    rows = []
    for dt in dates:
        load_s = day_series(load_df, dt, "forecast_load_mw")
        if load_s.empty:
            continue
        for h in range(1, 25):
            ld = load_s.get(h)
            if pd.isna(ld):
                continue
            rows.append({
                "datetime": pd.Timestamp(dt) + pd.Timedelta(hours=h),
                "date_label": pd.Timestamp(dt).strftime("%a %b-%d"),
                "he": h, "load": ld,
            })
    if not rows:
        return empty_html(f"No load data for {region_label}.")

    df = pd.DataFrame(rows).sort_values("datetime")
    df["load_ramp"] = df["load"].diff()
    day_boundaries = df["date_label"] != df["date_label"].shift(1)
    df.loc[day_boundaries, "load_ramp"] = None

    fig = go.Figure()
    cd = df[["date_label", "he"]].values

    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["load"], mode="lines", name="Load",
        line=dict(color="#60a5fa", width=2),
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Load: %{y:,.0f} MW<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=df["datetime"], y=df["load_ramp"],
        name="Load Ramp", marker_color="#60a5fa", opacity=0.85,
        customdata=cd, visible=False,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Ramp: %{y:+,.0f} MW/hr<extra></extra>",
    ))

    fig.update_layout(
        title=f"Load — {region_label}",
        height=460, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=60, b=60),
        hovermode="x unified",
    )
    fig.update_xaxes(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)")
    fig.update_yaxes(title_text="MW", tickformat=".1s", gridcolor="rgba(99,110,250,0.1)")

    div_id = f"pjm-allload-{region_label.lower().replace(' ', '').replace('-', '')}"
    return render_ramp_toggle(
        fig, div_id=div_id,
        outright_indices=[0], ramp_indices=[1],
    )


# ══════════════════════════════════════════════════════════════════════
# Per-date helpers
# ══════════════════════════════════════════════════════════════════════


def _build_day_frame(
    *, load_df: pd.DataFrame, solar_df: pd.DataFrame, wind_df: pd.DataFrame,
    target_date, include_renewables: bool,
) -> pd.DataFrame:
    load_s = day_series(load_df, target_date, "forecast_load_mw")
    out = pd.DataFrame({"hour_ending": list(range(1, 25))})
    out["load_mw"] = out["hour_ending"].map(load_s)
    if include_renewables:
        solar_s = day_series(solar_df, target_date, "solar_forecast")
        wind_s = day_series(wind_df, target_date, "wind_forecast")
        out["solar_mw"] = out["hour_ending"].map(solar_s)
        out["wind_mw"] = out["hour_ending"].map(wind_s)
        renew = out[["solar_mw", "wind_mw"]].fillna(0).sum(axis=1)
        out["net_load_mw"] = out["load_mw"] - renew
    return out


def _render_snapshot_table(day_df: pd.DataFrame, dt_key: str, *, is_rto: bool) -> str:
    if day_df.empty:
        return empty_html("No rows.")

    tid = f"rs-tbl-{dt_key}"

    if is_rto:
        outright_rows = [
            _tbl_row("Load", "MW", day_df["load_mw"]),
            _tbl_row("Wind", "MW", day_df["wind_mw"]),
            _tbl_row("Solar", "MW", day_df["solar_mw"]),
        ]
        outright_net = [_tbl_row("Net Load", "MW", day_df["net_load_mw"])]
        ramp_rows = [
            _tbl_row("Load Ramp", "MW/hr", day_df["load_mw"].diff(), signed=True, sign_colors=True),
            _tbl_row("Wind Ramp", "MW/hr", day_df["wind_mw"].diff(), signed=True, sign_colors=True),
            _tbl_row("Solar Ramp", "MW/hr", day_df["solar_mw"].diff(), signed=True, sign_colors=True),
        ]
        ramp_net = [_tbl_row("Net Load Ramp", "MW/hr", day_df["net_load_mw"].diff(), signed=True, sign_colors=True)]
        outright_body = _tbl_body(outright_rows, outright_net)
        ramp_body = _tbl_body(ramp_rows, ramp_net)
    else:
        outright_body = _tbl_row("Load", "MW", day_df["load_mw"])
        ramp_body = _tbl_row("Load Ramp", "MW/hr", day_df["load_mw"].diff(), signed=True, sign_colors=True)

    cols = ["Metric", "Unit"] + HE_COLS + SUMMARY_COLS
    header = '<thead><tr>'
    for col in cols:
        cls = ' class="metric"' if col == "Metric" else ' class="unit"' if col == "Unit" else ""
        header += f'<th{cls}>{col}</th>'
    header += '</tr></thead>'

    toggle_btn = (
        f'<div class="rs-toggle-bar">'
        f'<button class="rs-toggle" onclick="rsToggle(\'{tid}\')" id="{tid}-btn">SHOW RAMP</button>'
        f'</div>'
    )

    return (
        f'<div class="rs-wrap">{toggle_btn}'
        f'<div class="rs-tw"><table class="rs-t" id="{tid}">{header}'
        f'<tbody id="{tid}-outright">{outright_body}</tbody>'
        f'<tbody style="display:none;" id="{tid}-ramp">{ramp_body}</tbody>'
        f'</table></div></div>'
        f'''<script>
function rsToggle(tid) {{
  var o = document.getElementById(tid + '-outright');
  var r = document.getElementById(tid + '-ramp');
  var b = document.getElementById(tid + '-btn');
  if (o.style.display === 'none') {{
    o.style.display = ''; r.style.display = 'none'; b.textContent = 'SHOW RAMP';
  }} else {{
    o.style.display = 'none'; r.style.display = ''; b.textContent = 'SHOW OUTRIGHT';
  }}
}}
</script>'''
    )


def _tbl_row(metric: str, unit: str, values: pd.Series,
             signed: bool = False, sign_colors: bool = False) -> str:
    s = values.copy()
    s.index = range(1, len(s) + 1)
    html = f'<tr><td class="metric">{metric}</td><td class="unit">{unit}</td>'
    for h in range(1, 25):
        v = s.get(h, pd.NA)
        cls = cell_class(v, sign_colors)
        html += f'<td class="{cls}">{fmt_cell(v, signed)}</td>'
    for hours in [ONPEAK_HOURS, OFFPEAK_HOURS, list(range(1, 25))]:
        vals = pd.to_numeric(s.reindex(hours), errors="coerce").dropna()
        v = float(vals.mean()) if not vals.empty else pd.NA
        cls = cell_class(v, sign_colors)
        html += f'<td class="{cls}">{fmt_cell(v, signed)}</td>'
    html += '</tr>'
    return html


def _tbl_body(component_rows: list[str], net_rows: list[str]) -> str:
    divider = (
        f'<tr><td colspan="{2 + 24 + 3}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a,#4a6a8a60);border:none;"></td></tr>'
    )
    return "".join(component_rows) + divider + "".join(net_rows)


def _render_net_load_row(day_df: pd.DataFrame, dt_key: str) -> str:
    fig = go.Figure()
    hours = day_df["hour_ending"]
    net = day_df["net_load_mw"]
    solar = day_df["solar_mw"]
    wind = day_df["wind_mw"]
    load = day_df["load_mw"]

    outright_indices: list[int] = []
    ramp_indices: list[int] = []

    # ── Outright stack ────────────────────────────────────────────
    outright_indices.append(len(fig.data))
    fig.add_trace(go.Scatter(
        x=hours, y=net, mode="lines", name="Net Load", stackgroup="stack",
        line=dict(color="#60a5fa", width=1),
        fillcolor="rgba(96, 165, 250, 0.50)",
        hovertemplate="HE %{x}<br>Net Load: %{y:,.0f} MW<extra></extra>",
    ))
    outright_indices.append(len(fig.data))
    fig.add_trace(go.Scatter(
        x=hours, y=wind, mode="lines", name="Wind", stackgroup="stack",
        line=dict(color="#34d399", width=1),
        fillcolor="rgba(52, 211, 153, 0.35)",
        hovertemplate="HE %{x}<br>Wind: %{y:,.0f} MW<extra></extra>",
    ))
    outright_indices.append(len(fig.data))
    fig.add_trace(go.Scatter(
        x=hours, y=solar, mode="lines", name="Solar", stackgroup="stack",
        line=dict(color="#fbbf24", width=1),
        fillcolor="rgba(251, 191, 36, 0.40)",
        hovertemplate="HE %{x}<br>Solar: %{y:,.0f} MW<extra></extra>",
    ))
    outright_indices.append(len(fig.data))
    fig.add_trace(go.Scatter(
        x=hours, y=load, mode="lines", name="Load",
        line=dict(color="#f8fafc", width=2),
        hovertemplate="HE %{x}<br>Load: %{y:,.0f} MW<extra></extra>",
    ))

    # ── Ramp bars (hidden until toggle) ──────────────────────────
    ramp_specs = [
        ("Load Ramp",     load.diff(),  "#f8fafc"),
        ("Net Load Ramp", net.diff(),   "#60a5fa"),
        ("Solar Ramp",    solar.diff(), "#fbbf24"),
        ("Wind Ramp",     wind.diff(),  "#34d399"),
    ]
    for name, series, color in ramp_specs:
        ramp_indices.append(len(fig.data))
        fig.add_trace(go.Bar(
            x=hours, y=series, name=name,
            marker_color=color, opacity=0.85,
            visible=False,
            hovertemplate=f"HE %{{x}}<br>{name}: %{{y:+,.0f}} MW/hr<extra></extra>",
        ))

    fig.update_layout(
        title="Net Load Breakdown",
        template=PLOTLY_TEMPLATE, height=470,
        margin=dict(l=60, r=40, t=60, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.12, x=0),
        hovermode="x unified", barmode="group", bargap=0.15,
    )
    fig.update_xaxes(
        title_text="Hour Ending",
        dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True,
    )
    fig.update_yaxes(title_text="MW", gridcolor="rgba(99,110,250,0.1)")

    return render_ramp_toggle(
        fig, div_id=f"rs-netload-{dt_key}",
        outright_indices=outright_indices,
        ramp_indices=ramp_indices,
    )


def _render_load_only_row(day_df: pd.DataFrame, dt_key: str, region_label: str) -> str:
    fig = go.Figure()
    hours = day_df["hour_ending"]
    load = day_df["load_mw"]
    ramp = load.diff()

    fig.add_trace(go.Scatter(
        x=hours, y=load, mode="lines+markers", name="Load",
        line=dict(color="#60a5fa", width=2), marker=dict(size=5),
        hovertemplate="HE %{x}<br>Load: %{y:,.0f} MW<extra></extra>",
    ))
    bar_colors = ["#34d399" if (pd.notna(v) and v >= 0) else "#f87171" for v in ramp]
    fig.add_trace(go.Bar(
        x=hours, y=ramp, name="Load Ramp",
        marker_color=bar_colors, opacity=0.85,
        visible=False,
        hovertemplate="HE %{x}<br>Ramp: %{y:+,.0f} MW/hr<extra></extra>",
    ))

    fig.update_layout(
        title=f"Load — {region_label}",
        template=PLOTLY_TEMPLATE, height=420,
        margin=dict(l=60, r=40, t=60, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.12, x=0),
        hovermode="x unified",
    )
    fig.update_xaxes(title_text="Hour Ending",
                     dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True)
    fig.update_yaxes(title_text="MW", gridcolor="rgba(99,110,250,0.1)")

    return render_ramp_toggle(
        fig, div_id=f"rs-load-{dt_key}",
        outright_indices=[0], ramp_indices=[1],
    )
