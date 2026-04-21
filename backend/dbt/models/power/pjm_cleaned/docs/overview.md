{% docs pjm_overview %}

# PJM Power Market Models

This dbt module transforms raw PJM Interconnection data into analysis-ready
mart views for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **PJM Data Miner 2** | Official PJM API — LMPs (hourly + 5-min), load (metered, prelim, DA bids), reserves (operational, dispatched), ancillary services prices, outage forecasts, tie flows, 7-day load forecast | Direct API scrapes (`backend/scrapes/power/pjm/`) |
| **GridStatus (open-source)** | Community library wrapping PJM public data — fuel mix, solar/wind forecasts, load forecast | `backend/scrapes/power/gridstatus_open_source/pjm/` |
| **GridStatus.io (paid API)** | Commercial API with enriched PJM data — LMPs, fuel mix, load | `backend/scrapes/power/gridstatusio_api_key/pjm/` |

## Regional Breakdown

PJM load data is tracked across four regions:

- **RTO** — the full PJM footprint (13 states + DC)
- **MIDATL** — Mid-Atlantic zones (PECO, PPL, JCPL, PSEG, etc.)
- **WEST** — Western zones (COMED, AEP, DAY, DUQ, etc.)
- **SOUTH** — Southern zones (DOMINION, etc.) — *computed as* `RTO - MIDATL - WEST`

## Pipeline Architecture

```
source/          Raw API table normalization (ephemeral)
  |
  v
staging/         Core hourly/daily transformation layer (ephemeral)
  |
  v
marts/           Consumer-facing outputs (views)
  |
  v
queries/         Ad-hoc analytical models (ephemeral)
  |
  v
utils/           Utility models (ephemeral)
```

## Update Cadences

| Data | Lag | Frequency |
|------|-----|-----------|
| RT instantaneous load | ~5 min | Every 5 min |
| RT preliminary load | ~1 hour | Hourly |
| DA LMPs / load bids | Next-day morning | Daily |
| RT metered (actual) load | ~2 days | Daily |
| RT verified LMPs | ~60 days | Daily |
| 7-day load forecast | Multiple per day | ~4x daily |
| Solar/wind forecast | Multiple per day | ~4x daily |
| Outage forecast | Daily | Daily |
| Fuel mix | ~1 hour | Hourly |
| Ancillary services prices | ~1 hour | Hourly |
| Dispatched reserves | ~5 min | Every 5 min |
| Operational reserves | ~15 sec | Every 15 sec |
| RT dispatched reserves | Next business day | Daily |
| 5-min RT LMPs (unverified) | ~5 min | Every 5 min |

{% enddocs %}

