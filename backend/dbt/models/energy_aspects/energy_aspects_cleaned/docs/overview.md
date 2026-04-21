{% docs ea_overview %}

# Energy Aspects Models

This dbt module transforms raw Energy Aspects (EA) API timeseries data into
analysis-ready mart views for the HeliosCTA power trading desk.

## Data Source

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **Energy Aspects API** | Subscription-based energy market analytics — generation forecasts, dispatch costs, power prices, heat rates, spark spreads, installed capacity, and load models | Scheduled Python scripts in `backend/scrapes/energy_aspects/timeseries/` |

## Coverage

EA data covers all major US ISOs/regions:

- **PJM** (initial implementation)
- ERCOT, MISO, CAISO, NYISO, ISONE, SPP
- Non-ISO regions: Northwest, Southwest, Southeast, West, US48

## Raw Table Structure

All raw tables in the `energy_aspects` schema are **wide format**: one `date`
column (monthly) and many metric columns (one per EA dataset ID). Column names
are auto-generated from EA API metadata descriptions via
`energy_aspects_api_utils.build_column_map()`.

## Pipeline Architecture

```
source/          Wide-format raw table pass-through (ephemeral)
  |
  v
staging/         ISO-specific column extraction and renaming (ephemeral)
  |
  v
marts/           Consumer-facing PJM outputs (views)
```

## Update Cadence

EA data is **monthly**. Pipelines pull from the EA API and upsert into the
`energy_aspects` schema. All tables use `date` as the primary key.

{% enddocs %}
