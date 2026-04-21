{% docs spp_overview %}

# SPP Power Market Models

This dbt module transforms raw Southwest Power Pool (SPP) data into analysis-ready
mart views for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **GridStatus** | SPP day-ahead hourly LMPs and real-time 5-minute LMPs by location | `backend/scrapes/power/gridstatus_open_source/spp/` or `backend/scrapes/power/gridstatusio_api_key/spp/` |

## Hub Breakdown

SPP LMP data is tracked across 15 locations, with 2 primary hubs pivoted into wide format:

- **SPPNORTH_HUB** (column suffix: `north_hub`)
- **SPPSOUTH_HUB** (column suffix: `south_hub`)

All 15 source locations: AECI, EES, ERCOTE, ERCOTN, KCPLHUB, MISO, NSP, OKGE_OKGE,
PJM, SECI_SECI, SOCO, SPA, SPPNORTH_HUB, SPPSOUTH_HUB, TVA.

## Pipeline Architecture

```
source/          Raw API table normalization (ephemeral)
  |
  v
staging/         Core hourly transformation layer (ephemeral)
  |
  v
marts/           Consumer-facing outputs (views)
```

## Update Cadences

| Data | Lag | Frequency |
|------|-----|-----------|
| DA LMPs | Next-day morning | Daily |
| RT LMPs (5-min) | ~5 min | Every 5 min |

{% enddocs %}
