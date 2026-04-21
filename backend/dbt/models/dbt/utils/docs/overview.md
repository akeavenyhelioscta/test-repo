{% docs utils_overview %}

# Shared Utility Models

Cross-domain date dimension and reference models used throughout the dbt project.
These utilities provide consistent date attributes, EIA storage week alignment,
and NERC holiday lookups for downstream domains.

## Models

| Model | Grain | Description |
|-------|-------|-------------|
| **utils_v1_dates_daily** | One row per calendar day | Daily date spine from 2010 through current year + 7, with year, month, season, EIA week, day-of-week, weekend, and NERC holiday flags. Excludes Feb 29. |
| **utils_v1_dates_weekly** | One row per EIA storage week | Weekly aggregation of the daily spine aligned to EIA natural gas storage report weeks (Friday-to-Thursday), with holiday counts per week. |
| **utils_v1_nerc_holidays** | One row per NERC holiday | Static lookup of NERC holidays (2014–2028) used for on-peak/off-peak classification. |

## Key Concepts

### EIA Storage Weeks

EIA natural gas storage reports follow a Friday-to-Thursday week. The `eia_storage_week`
column snaps each date to the Friday that ends its reporting week. This aligns daily and
weekly models for storage analysis.

### Summer / Winter Seasons

- **SUMMER** (Apr–Oct): contract code `JV-YY`
- **WINTER** (Nov–Mar): contract code `XH-YY` (using the year the winter ends)

### NERC Holidays

Six holidays per year: New Year's Day, Memorial Day, Independence Day, Labor Day,
Thanksgiving, and Christmas. Used to flag off-peak days in power market models.

{% enddocs %}
