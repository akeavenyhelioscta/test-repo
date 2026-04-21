{% docs miso_overview %}

# MISO Power Market Models

This dbt module transforms raw MISO (Midcontinent Independent System Operator)
data into analysis-ready mart views for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **GridStatus** | Community library wrapping MISO public data — day-ahead and real-time LMPs | `backend/scrapes/power/gridstatus_open_source/miso/` |

## Hub Breakdown

MISO LMP data is tracked across eight commercial pricing hubs spanning 15 states:

- **ARKANSAS.HUB** — Arkansas region
- **ILLINOIS.HUB** — Illinois region (ComEd, Ameren Illinois)
- **INDIANA.HUB** — Indiana region (Duke Indiana, IPL, AES Indiana)
- **LOUISIANA.HUB** — Louisiana region (Entergy Louisiana, Cleco)
- **MICHIGAN.HUB** — Michigan region (Consumers Energy, DTE)
- **MINN.HUB** — Minnesota region (Xcel Energy, Minnesota Power)
- **MS.HUB** — Mississippi region (Entergy Mississippi)
- **TEXAS.HUB** — Texas panhandle region (within MISO South)

## Pipeline Architecture

```
source/          Raw API table normalization and hub pivot (ephemeral)
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
| RT LMPs (5-min) | ~5 min | Every 5 min |

{% enddocs %}
