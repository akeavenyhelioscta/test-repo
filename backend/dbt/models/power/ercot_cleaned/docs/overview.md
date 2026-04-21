{% docs ercot_overview %}

# ERCOT Power Market Models

This dbt module transforms raw ERCOT (Electric Reliability Council of Texas) data
into analysis-ready mart views for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **GridStatus (open-source)** | Community library wrapping ERCOT public data -- LMPs, SPPs, fuel mix, load, load forecasts, solar/wind forecasts, outages, energy storage | `backend/scrapes/power/gridstatus_open_source/ercot/` |
| **GridStatus.io (paid API)** | Commercial API with enriched ERCOT data | `backend/scrapes/power/gridstatusio_api_key/ercot/` |

## Trading Hubs

ERCOT pricing data is tracked across four trading hubs:

- **HB_HOUSTON** (Houston hub)
- **HB_NORTH** (North hub)
- **HB_SOUTH** (South hub)
- **HB_WEST** (West hub)

## Load Forecast Zones

ERCOT load is broken down by forecast zone:

- **NORTH** -- North Texas
- **SOUTH** -- South Texas
- **WEST** -- West Texas
- **HOUSTON** -- Greater Houston area
- **TOTAL** -- Sum of all zones

## On-Peak Definition

ERCOT on-peak hours: **HE07--HE22** (7 AM to 10 PM Central Prevailing Time).

## Timezone

All ERCOT timestamps are in **Central Prevailing Time (CPT)**.

## Pipeline Architecture

```
source/          Raw API table normalization (ephemeral)
  |
  v
staging/         Core hourly/daily transformation layer (ephemeral)
  |
  v
marts/           Consumer-facing outputs (views)
```

## Key Differences from PJM

- ERCOT uses **SPP** (Settlement Point Price) instead of LMP for its DA prices
- Only **lmp_total** is available (no system_energy/congestion/loss breakdown)
- Load zones are geographic (NORTH, SOUTH, WEST, HOUSTON) rather than regional (RTO, MIDATL, etc.)
- On-peak hours are **HE07--HE22** (vs PJM's HE08--HE23)

{% enddocs %}
