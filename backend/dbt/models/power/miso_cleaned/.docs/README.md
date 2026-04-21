# miso_cleaned marts

## Purpose

Consumer-facing MISO power market views exposing LMPs (day-ahead, real-time, and DA-RT spread) across 8 commercial pricing hubs. Each mart is a thin view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `miso_lmps_hourly` | `date x hour_ending x hub x market` |
| `miso_lmps_daily` | `date x period` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| GridStatus (open-source) | `staging_v1_miso_lmps_hourly`, `staging_v1_miso_lmps_daily` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime` | Central Prevailing Time timestamp |
| `date` | Calendar date (Central Prevailing Time) |
| `hour_ending` | Hour ending 1-24 |
| `hub` | Pricing hub name (ARKANSAS.HUB, ILLINOIS.HUB, etc.) |
| `market` | Market type: `da`, `rt`, or `dart` |
| `lmp_total` | Total LMP ($/MWh) |
| `lmp_system_energy_price` | System energy component ($/MWh) |
| `lmp_congestion_price` | Congestion component ($/MWh) |
| `lmp_marginal_loss_price` | Marginal loss component ($/MWh) |

## Transformation Notes

- All marts are materialized as **views** (`SELECT * FROM staging`).
- Hourly LMPs are normalized to long format: one row per hub x market.
- Business logic (DA/RT spread calculation, 5-min to hourly averaging, hub pivoting) lives entirely in the source and staging layers.
- On-peak hours: HE08-HE23.
- Daily LMP model remains in wide format for flat/onpeak/offpeak aggregation.
