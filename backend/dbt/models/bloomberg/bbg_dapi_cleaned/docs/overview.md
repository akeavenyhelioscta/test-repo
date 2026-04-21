{% docs bbg_dapi_overview %}

# Bloomberg DAPI — Cleaned Domain

This dbt module transforms raw Bloomberg Data API (DAPI) tables into
analysis-ready mart views for the HeliosCTA trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **bbg_tickers** | Bloomberg security ticker registry with human-readable descriptions | Python scrape into `bbg_dapi` schema |
| **bbg_historical** | Historical data points by security, date, snapshot, and data type | Python scrape into `bbg_dapi` schema |

## Pipeline Architecture

```
source/          Raw table extraction with explicit casts (ephemeral)
  |
  v
staging/         Join historical to tickers, deduplicate at grain (ephemeral)
  |
  v
marts/           Business-ready historical view (view)
```

## Grain

The final output grain is **one row per (`security`, `date`, `snapshot_at`, `data_type`)**.

## Key Concepts

- **security** — Bloomberg ticker identifier (e.g., `CLA Comdty`)
- **description** — Human-readable name from the tickers table
- **data_type** — Bloomberg field type (e.g., `PX_LAST`, `PX_OPEN`, `PX_HIGH`)
- **snapshot_at** — Timestamp of the data snapshot
- **value** — The observed data point value

{% enddocs %}
