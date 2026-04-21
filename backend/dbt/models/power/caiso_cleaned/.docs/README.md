# caiso_cleaned marts

## Purpose

Consumer-facing CAISO power market views exposing LMPs with DA, RT, and DART pricing for NP15 and SP15 hubs. Each mart is a thin view wrapper over vetted staging logic.

## Grain

| Model | Grain |
|-------|-------|
| `caiso_lmps_hourly` | `date x hour_ending x hub x market` |
| `caiso_lmps_daily` | `date` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| GridStatus | `staging_v1_caiso_lmps_hourly`, `staging_v1_caiso_lmps_daily` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime` | Local timestamp |
| `date` | Calendar date |
| `hour_ending` | Hour ending 1-24 |
| `hub` | Pricing hub name (NP15, SP15) |
| `market` | Market type: `da`, `rt`, or `dart` |
| `lmp_total` | Total LMP ($/MWh) |
| `lmp_system_energy_price` | System energy component ($/MWh) |
| `lmp_congestion_price` | Congestion component ($/MWh) |
| `lmp_marginal_loss_price` | Marginal loss component ($/MWh) |

## Transformation Notes

- All marts are materialized as **views** (`SELECT * FROM staging`).
- Hourly LMPs are normalized to long format: one row per hub × market.
- Business logic (DA/RT join, DART spread) lives in the staging layer.
- Daily LMP model remains in wide format for flat/onpeak/offpeak aggregation.
