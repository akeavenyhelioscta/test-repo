{% docs bbg_dapi_staging_historical_with_tickers %}

Historical Bloomberg DAPI data joined with ticker descriptions,
deduplicated at the business grain, with revision tracking.

**Key transformations:**

1. **Ticker join** — Left joins `bbg_historical` to `bbg_tickers` on `security`
   to enrich each data point with the ticker `description`.

2. **Deduplication** — If multiple rows exist at the grain
   (`security`, `date`, `snapshot_at`, `data_type`), the row with the latest
   `updated_at` is kept. Ties are broken by `created_at DESC` for determinism.
   This handles potential upsert race conditions in the ingestion layer.

3. **Revision tracking** — Within each (`date`, `security`, `description`,
   `data_type`) group, snapshots are numbered by `snapshot_at` ascending.
   `revision = 1` is the oldest snapshot; `revision = max_revision` is the
   latest. Use `WHERE revision = max_revision` to get only the most recent
   snapshot per key.

**Grain:** One row per `security` × `date` × `snapshot_at` × `data_type`.

{% enddocs %}


{% docs bbg_dapi_staging_gas_supply_demand %}

Pivoted gas supply & demand view built from the long-form DAPI historical data.
Latest revision only, filtered to `data_type = 'PX_LAST'`.

**Key transformations:**

1. **Pivot** — Converts long-form (one row per security per date) into wide-form
   (one row per date, one column per security). Uses `MAX(CASE WHEN security = ... THEN value END)`
   grouped by date.

2. **LNG absolute values** — Bloomberg reports LNG as negative; `ABS()` applied.

3. **Computed aggregates** — Follows the legacy `bloomberg.home` gas supply/demand model:
   - `total_supply = production + cad_imports`
   - `lower_48_demand = power_burn + industrial + rescom + plant_fuel + pipe_loss`
   - `total_demand = lower_48_demand + mexico_exports + lng`
   - `implied_storage = total_supply - total_demand`
   - `balancing_item = implied_storage - storage`

4. **NULL safety** — `COALESCE(..., 0)` on all composite sums.

**Grain:** One row per `date`.

**Ticker → column mapping:** 59 Bloomberg DAPI tickers mapped to named columns.
See `marts/schema.yml` for the full ticker-to-column reference.

{% enddocs %}
