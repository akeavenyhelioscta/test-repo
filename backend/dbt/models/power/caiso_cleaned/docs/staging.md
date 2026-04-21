{% docs caiso_lmps %}

## Locational Marginal Prices (LMPs)

LMP models provide day-ahead, real-time, and DA-RT spread pricing by hub.

### Data Sources
- **DA LMPs** — GridStatus `caiso_lmp_day_ahead_hourly`
- **RT LMPs** — GridStatus `caiso_lmp_real_time_15_min` (15-min averaged to hourly)

### Key Transformations
- 15-minute RT data is averaged to hourly via `AVG() GROUP BY date, hour_ending`
- **DART spread** = DA LMP - RT LMP, computed per hour/hub/component
- Normalized long format: one row per `date x hour_ending x hub x market`

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_caiso_lmps_hourly_wide` | date x hour_ending | ephemeral |
| `staging_v1_caiso_lmps_hourly` | date x hour_ending x hub x market | ephemeral |
| `staging_v1_caiso_lmps_daily` | date | ephemeral |

### Price Components
Each LMP is decomposed into: **total**, **system energy price**, **congestion price**, **marginal loss price**.

### On-Peak Definition
- **onpeak** — HE07-HE22
- **offpeak** — HE01-HE06 and HE23-HE24

{% enddocs %}
