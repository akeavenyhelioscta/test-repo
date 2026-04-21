{% docs nyiso_lmps %}

## Locational Marginal Prices (LMPs)

NYISO LMP models provide day-ahead, real-time, and DA-RT spread pricing across
all 15 NYISO load zones in wide format.

### Data Sources
- **DA LMPs** — GridStatus `nyiso_lmp_day_ahead_hourly`
- **RT LMPs** — GridStatus `nyiso_lmp_real_time_5_min` (averaged to hourly)

### Key Transformations
- Source models filter to the 15 standard NYISO zones and pivot from long to wide format
- RT 5-minute data is averaged to hourly via `AVG()` in the pivot
- **DART spread** = DA LMP - RT LMP, computed per hour/zone/component
- DA, RT, and DART columns are joined into a single wide-format row per `date x hour_ending`
- `datetime` column added as `date + hour_ending`

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_nyiso_lmps_hourly_wide` | date x hour_ending | ephemeral |
| `staging_v1_nyiso_lmps_hourly` | date x hour_ending x hub x market | ephemeral |

### Price Components (per zone)
Each zone has four price components across three markets (DA, RT, DART):
- **lmp_total** — total locational marginal price ($/MWh)
- **lmp_system_energy_price** — system energy component ($/MWh)
- **lmp_congestion_price** — congestion component ($/MWh)
- **lmp_marginal_loss_price** — marginal loss component ($/MWh)

### Column Naming Convention
`{market}_lmp_{component}_{zone}` — e.g., `da_lmp_total_nyc`, `rt_lmp_congestion_price_capitl`, `dart_lmp_total_west`

{% enddocs %}
