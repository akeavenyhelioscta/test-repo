{% docs spp_lmps %}

## Locational Marginal Prices (LMPs)

LMP models provide day-ahead, real-time, and DA-RT spread pricing for SPP North and
South hubs in wide format.

### Data Sources
- **DA LMPs** — GridStatus `spp_lmp_day_ahead_hourly`
- **RT LMPs** — GridStatus `spp_lmp_real_time_5_min` (5-min averaged to hourly)

### Key Transformations
- Source models filter to 15 SPP locations and pivot North/South hubs into wide-format columns
- RT 5-minute data is averaged to hourly via `AVG()` grouping
- DA and RT are joined via `FULL OUTER JOIN` on `date` and `hour_ending`
- **DART spread** = DA LMP - RT LMP, computed per hour per hub
- `datetime` column is computed as `date + hour_ending` interval

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_spp_lmps_hourly_wide` | date x hour_ending | ephemeral |
| `staging_v1_spp_lmps_hourly` | date x hour_ending x hub x market | ephemeral |

### Price Components
Each hub has 4 LMP components across 3 markets (DA, RT, DART):
- **total** — Total LMP ($/MWh)
- **system_energy_price** — System energy price component ($/MWh)
- **congestion_price** — Congestion price component ($/MWh)
- **marginal_loss_price** — Marginal loss price component ($/MWh)

{% enddocs %}
