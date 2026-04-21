{% docs bbg_dapi_mart_historical %}

Business-ready Bloomberg DAPI historical data, materialized as a view in
the `bbg_dapi_cleaned` schema.

**Grain:** One row per `security` × `date` × `snapshot_at` × `data_type`.

**Key columns:**
- `date` — Observation date
- `snapshot_at` — Data snapshot timestamp
- `revision` — Snapshot revision number (1 = oldest, ascending by `snapshot_at`)
- `max_revision` — Total revisions for this (`date`, `security`, `description`, `data_type`)
- `security` — Bloomberg ticker identifier
- `description` — Human-readable ticker name (from `bbg_tickers`)
- `data_type` — Bloomberg field name (e.g., `PX_LAST`)
- `value` — Observed value

**Revision filtering:**
- Latest snapshot only: `WHERE revision = max_revision`
- First snapshot only: `WHERE revision = 1`

**Notes:**
- Data is kept in long-form (one row per data type) to support flexible
  downstream pivoting by consumers.
- `description` may be NULL if a security exists in the historical table
  but has not yet been registered in the tickers table.

{% enddocs %}


{% docs bbg_dapi_mart_gas_supply_demand %}

Daily gas supply & demand dashboard, materialized as a view in the
`bbg_dapi_cleaned` schema. Replaces the legacy `bloomberg.home`-based
gas supply/demand model with DAPI-sourced data.

**Grain:** One row per `date`.

**Categories:**
- **Supply** — `total_supply`, `production`, 14 regional production sub-columns, `cad_imports` + 6 Canada sub-routes
- **Demand** — `lower_48_demand`, `power_burn`, `industrial`, `rescom`, `plant_fuel`, `pipe_loss`
- **Mexico exports** — `mexico_exports` + 3 sub-routes
- **LNG** — `lng` (total) + 9 terminal-level columns (absolute values)
- **Storage** — `net_storage_change`, `net_implied_storage_change`, `balancing_item`, `salt`
- **Weather** — `elec_cdd`, `elec_hdd`, `gas_cdd`, `gas_hdd`
- **Spot prices** — 14 regional gas spot prices ($/MMBtu)

**Computed columns:**
- `total_supply = production + cad_imports`
- `lower_48_demand = power_burn + industrial + rescom + plant_fuel + pipe_loss`
- `total_demand = lower_48_demand + mexico_exports + lng`
- `net_implied_storage_change = total_supply - total_demand`
- `balancing_item = implied_storage - storage`

**New vs legacy:** Adds `plaquemines` (LNG), `cad_to_michigan`, `nyc_spot`,
and 4 weather columns not present in the old `bloomberg.home` model.

{% enddocs %}
