{% docs caiso_overview %}

# CAISO Power Market Models

This dbt module transforms raw CAISO (California ISO) data into analysis-ready
mart views for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **GridStatus** | CAISO LMP data (day-ahead hourly and real-time 15-minute) | `backend/scrapes/power/gridstatus_open_source/caiso/` |

## Pricing Hubs

CAISO LMP data is tracked across two trading hubs:

- **NP15** — Northern California (North Path 15)
- **SP15** — Southern California (South Path 15)

## Pipeline Architecture

```
source/          Raw GridStatus table normalization (ephemeral)
  |
  v
staging/         Core hourly/daily transformation layer (ephemeral)
  |
  v
marts/           Consumer-facing outputs (views)
```

## Update Cadences

| Data | Lag | Frequency |
|------|-----|-----------|
| DA LMPs | Next-day morning | Daily |
| RT LMPs (15-min) | ~15 min | Every 15 min |

{% enddocs %}
