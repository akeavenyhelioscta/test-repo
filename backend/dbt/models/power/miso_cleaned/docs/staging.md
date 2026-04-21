{% docs miso_lmps %}

## Locational Marginal Prices (LMPs)

MISO LMP models provide day-ahead, real-time, and DA-RT spread pricing across
eight commercial pricing hubs in wide (pivoted) format.

### Data Sources
- **DA LMPs** — GridStatus `miso_lmp_day_ahead_hourly`
- **RT LMPs** — GridStatus `miso_lmp_real_time_5_min` (averaged to hourly)

### Key Transformations
- Source models filter to 8 MISO hubs and pivot from long to wide format using
  `AVG(CASE WHEN location = 'X.HUB' THEN ... END)` pattern
- RT 5-minute data is averaged to hourly during the pivot
- **DART spread** = DA LMP - RT LMP, computed per hour for each hub and component
- All three markets (DA, RT, DART) are joined into a single wide row per date/hour
- RT and DART columns are filtered to complete days only
  (`date < CURRENT_TIMESTAMP AT TIME ZONE 'US/Central'`) in the daily model

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_miso_lmps_hourly_wide` | date x hour_ending | ephemeral |
| `staging_v1_miso_lmps_hourly` | date x hour_ending x hub x market | ephemeral |
| `staging_v1_miso_lmps_daily` | date x period | ephemeral |

### Price Components
Each hub has four LMP components across three markets (DA, RT, DART):
**total**, **system energy price**, **congestion price**, **marginal loss price**.

### On-Peak Definition
- **On-peak:** HE08-HE23 (hours ending 8 through 23)
- **Off-peak:** HE01-HE07 and HE24

{% enddocs %}
