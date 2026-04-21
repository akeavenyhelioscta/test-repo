{% docs ice_python_overview %}

# ICE Natural Gas Derivatives

This dbt module transforms raw ICE (Intercontinental Exchange) natural gas
data into analysis-ready mart views for the HeliosCTA trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **ICE Next-Day Gas** | Hourly VWAP close prices for firm physical next-day gas at 15 hubs | Python scrape (`backend/scrapes/ice_python/next_day_gas/`) into `ice_python` schema |
| **ICE BALMO** | Balance-of-month gas swap settle prices at 15 hubs | Python scrape (`backend/scrapes/ice_python/balmo/`) into `ice_python` schema |

## Hub Coverage

15 natural gas pricing hubs across 5 U.S. regions:

- **Henry Hub** — National benchmark (HH)
- **Southeast** — Transco Station 85, Pine Prairie
- **East Texas** — Waha, Houston Ship Channel, NGPL TX/OK
- **Northeast** — Transco Zone 5 South, Tetco M3, AGT, Iroquois Zone 2
- **West** — SoCal Citygate, PG&E Citygate
- **Rockies/Northwest** — CIG
- **Midwest** — NGPL Midcontinent, MichCon

## Pipeline Architecture

```
utils/           Gas day dates (ephemeral)
  |
  v
source/          Raw ICE table extraction and pivot (ephemeral)
  |
  v
staging/         Forward-fill, daily aggregation (ephemeral)
  |
  v
marts/           BALMO, hourly/daily gas views (views)
```

## Key Transformations

- **Forward fill** — Weekend/holiday gaps filled with last known settle price
  using cumulative-sum grouping and FIRST_VALUE window functions
- **Symbol pivot** — Raw long-format ICE data (symbol, value) pivoted into
  one column per hub

{% enddocs %}
