{% docs nyiso_overview %}

# NYISO Power Market Models

This dbt module transforms raw NYISO (New York Independent System Operator) data
into analysis-ready mart views for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **GridStatus** | NYISO day-ahead and real-time LMPs by zone | `backend/scrapes/power/gridstatus_open_source/nyiso/` |

## NYISO Zones

NYISO reports LMPs across 15 load zones:

| Zone | Column Suffix | Description |
|------|---------------|-------------|
| CAPITL | `_capitl` | Capital (Albany area) |
| CENTRL | `_centrl` | Central (Syracuse area) |
| DUNWOD | `_dunwod` | Dunwoodie (lower Hudson Valley) |
| GENESE | `_genese` | Genesee (Rochester area) |
| H Q | `_hq` | Hydro-Quebec import interface |
| HUD VL | `_hud_vl` | Hudson Valley |
| LONGIL | `_longil` | Long Island |
| MHK VL | `_mhk_vl` | Mohawk Valley |
| MILLWD | `_millwd` | Millwood (Westchester area) |
| NORTH | `_north` | North (Adirondacks/St. Lawrence) |
| NPX | `_npx` | New England Power Exchange interface |
| N.Y.C. | `_nyc` | New York City |
| O H | `_oh` | Ontario/Hydro import interface |
| PJM | `_pjm` | PJM Interconnection interface |
| WEST | `_west` | Western New York (Buffalo area) |

## Pipeline Architecture

```
source/          Raw GridStatus table normalization (ephemeral)
  |
  v
staging/         Core hourly transformation layer (ephemeral)
  |
  v
marts/           Consumer-facing outputs (views)
```

## Wide-Format Layout

Unlike PJM's long-format LMP models (one row per hub), NYISO models use a
**wide format** with one row per `date x hour_ending`. Each zone's DA, RT, and
DART prices are represented as separate columns, yielding 180 LMP columns
(15 zones x 4 components x 3 markets).

{% enddocs %}
