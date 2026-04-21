{% docs isone_lmps %}

## Locational Marginal Prices (LMPs)

LMP models provide day-ahead, real-time, and DA-RT spread pricing for the ISO-NE
Internal Hub.

### Data Sources
- **DA LMPs** — ISO-NE `da_hrl_lmps`
- **RT LMPs (final)** — ISO-NE `rt_hrl_lmps_final`
- **RT LMPs (prelim)** — ISO-NE `rt_hrl_lmps_prelim` (last 10 days, fills gaps before final is available)

### Key Transformations
- Source models filter to `.H.INTERNAL_HUB` location
- RT final and prelim are combined; final takes precedence via `NOT IN` dedup
- DA and RT are joined via `FULL OUTER JOIN` on `date` and `hour_ending`
- **DART spread** = DA LMP - RT LMP, computed per hour per component
- `datetime` column is computed as `date + hour_ending` interval

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_isone_rt_hrl_lmps` | date x hour_ending | ephemeral |
| `staging_v1_isone_lmps_hourly_wide` | date x hour_ending | ephemeral |
| `staging_v1_isone_lmps_hourly` | date x hour_ending x hub x market | ephemeral |
| `staging_v1_isone_lmps_daily` | date | ephemeral |

### Price Components
Each hub has 4 LMP components across 3 markets (DA, RT, DART):
- **total** — Total LMP ($/MWh)
- **system_energy_price** — System energy price component ($/MWh)
- **congestion_price** — Congestion price component ($/MWh)
- **marginal_loss_price** — Marginal loss price component ($/MWh)

{% enddocs %}
