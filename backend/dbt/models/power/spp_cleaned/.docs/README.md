# spp_cleaned marts

## Purpose

Consumer-facing SPP LMP views exposing hourly day-ahead, real-time, and DA-RT spread pricing. Each mart is a view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `spp_lmps_hourly` | `date x hour_ending x hub x market` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| GridStatus SPP DA Hourly LMPs | `staging_v1_spp_lmps_hourly` |
| GridStatus SPP RT 5-min LMPs | `staging_v1_spp_lmps_hourly` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime` | Timestamp computed as date + hour_ending interval |
| `date` | Operating date in Central Prevailing Time |
| `hour_ending` | Hour ending 1-24 (CPT) |
| `hub` | Pricing hub name (SPPNORTH_HUB, SPPSOUTH_HUB) |
| `market` | Market type: `da`, `rt`, or `dart` |
| `lmp_total` | Total LMP ($/MWh) |
| `lmp_system_energy_price` | System energy component ($/MWh) |
| `lmp_congestion_price` | Congestion component ($/MWh) |
| `lmp_marginal_loss_price` | Marginal loss component ($/MWh) |

## Transformation Notes

- All marts are materialized as **views**.
- Hourly LMPs are normalized to long format: one row per hub x market.
- Business logic (5-min to hourly averaging, DART computation) lives entirely in the staging layer.
- Source models filter to 15 SPP locations but only pivot SPPNORTH_HUB and SPPSOUTH_HUB.
- RT 5-minute LMPs are averaged to hourly granularity via `AVG()` grouping.
- DART spread = DA LMP - RT LMP, computed per hour per hub per component.
