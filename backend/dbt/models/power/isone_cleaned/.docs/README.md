# isone_cleaned marts

## Purpose

Consumer-facing ISO-NE power market views exposing LMPs (day-ahead, real-time, and DA-RT spread) for the Internal Hub. Each mart is a thin view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `isone_lmps_hourly` | `date x hour_ending x hub x market` |
| `isone_lmps_daily` | `date` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| ISO-NE DA LMPs | `staging_v1_isone_lmps_hourly`, `staging_v1_isone_lmps_daily` |
| ISO-NE RT LMPs (final + prelim) | `staging_v1_isone_lmps_hourly`, `staging_v1_isone_lmps_daily` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime` | Eastern Prevailing Time timestamp |
| `date` | Calendar date (Eastern Prevailing Time) |
| `hour_ending` | Hour ending 1-24 (EPT) |
| `hub` | Pricing hub name (.H.INTERNAL_HUB) |
| `market` | Market type: `da`, `rt`, or `dart` |
| `lmp_total` | Total LMP ($/MWh) |
| `lmp_system_energy_price` | System energy component ($/MWh) |
| `lmp_congestion_price` | Congestion component ($/MWh) |
| `lmp_marginal_loss_price` | Marginal loss component ($/MWh) |

## Transformation Notes

- All marts are materialized as **views** (`SELECT * FROM staging`).
- Hourly LMPs are normalized to long format: one row per hub x market.
- RT combines final (verified) and preliminary data; final takes precedence.
- Business logic (DA/RT join, DART spread, final/prelim dedup) lives entirely in the staging layer.
- On-peak hours: HE08-HE23.
- Daily LMP model remains in wide format for flat/onpeak/offpeak aggregation.
