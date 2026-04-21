# nyiso_cleaned marts

## Purpose

Consumer-facing NYISO power market views exposing LMPs (day-ahead, real-time, and DART spreads) across all 15 NYISO load zones. Each mart is a thin view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `nyiso_lmps_hourly` | `date x hour_ending x hub x market` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| GridStatus (open-source) | `staging_v1_nyiso_lmps_hourly` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime` | EPT timestamp |
| `date` | Calendar date (EPT) |
| `hour_ending` | Hour ending 1-24 (EPT) |
| `hub` | NYISO zone name (CAPITL, CENTRL, DUNWOD, GENESE, H Q, HUD VL, LONGIL, MHK VL, MILLWD, NORTH, NPX, N.Y.C., O H, PJM, WEST) |
| `market` | Market type: `da`, `rt`, or `dart` |
| `lmp_total` | Total LMP ($/MWh) |
| `lmp_system_energy_price` | System energy component ($/MWh) |
| `lmp_congestion_price` | Congestion component ($/MWh) |
| `lmp_marginal_loss_price` | Marginal loss component ($/MWh) |

## Transformation Notes

- All marts are materialized as **views** (`SELECT * FROM staging`).
- Hourly LMPs are normalized to long format: one row per hub x market.
- Business logic (DA/RT averaging, DART spread calculation, zone pivoting) lives entirely in the staging layer.
- RT LMPs are averaged from 5-minute to hourly granularity.
- DART = DA - RT for each zone and price component.
