{% docs isone_overview %}

# ISO-NE Power Market Models

This dbt module transforms raw ISO New England (ISO-NE) data into analysis-ready
mart views for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **ISO-NE** | Day-ahead hourly LMPs, real-time hourly LMPs (final + preliminary) by location | `backend/scrapes/power/` |

## Hub Breakdown

ISO-NE LMP data is tracked for a single hub:

- **.H.INTERNAL_HUB** (column suffix: `internal_hub`)

## Pipeline Architecture

```
source/          Raw table normalization (ephemeral)
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
| RT LMPs (final) | ~1 day | Daily |
| RT LMPs (prelim) | ~1 hour | Hourly |

{% enddocs %}
