{% docs meteologica_pjm_source %}

Raw PJM data from the **Meteologica xTraders API**.

Tables in this source contain API responses stored in the `meteologica` schema. Data is
pulled by scheduled Python scripts in `backend/scrapes/meteologica/` and upserted into Azure
PostgreSQL.

**Raw table columns:** `content_id`, `content_name`, `update_id`, `issue_date` (VARCHAR),
`forecast_period_start` (TIMESTAMP), `forecast_period_end`, `utc_offset_from`, `utc_offset_to`,
`forecast_mw` or `day_ahead_price` or `observation_mw` or `observation` (for price observations) or `normal_mw`,
`created_at`, `updated_at`.

ECMWF-ENS tables additionally include: `average_mw`, `bottom_mw`, `top_mw`,
`ens_00_mw` through `ens_50_mw` (51 ensemble members).

**Included datasets (~202 tables):**

Forecasts:
- **Demand forecasts (36):** RTO + 3 macro regions + 17 Mid-Atlantic sub-regions + 1 South sub-region + 14 West sub-regions
- **Demand forecasts — ECMWF-ENS (36):** Same 36 regions, ensemble members + summary stats (content IDs 2724–2759)
- **Solar generation forecasts (4):** RTO, MIDATL, SOUTH, WEST
- **Wind generation forecasts (12):** RTO + 3 regions + 8 utility-level sub-regions
- **Hydro generation forecast (1):** RTO only
- **DA price forecasts (13):** System + 12 pricing hubs

Observations:
- **Demand observations (36):** Same 36 regions as demand forecasts (RTO + 3 macro regions + 32 utility-level sub-regions)
- **Generation observations (9):** Solar (4: RTO, MIDATL, WEST, SOUTH), Wind (3: RTO, MIDATL, SOUTH), Hydro (1: RTO), plus 1 additional
- **DA price observations (13):** System + 12 pricing hubs

Projections:
- **Demand projections (33):** RTO + 32 utility-level sub-regions (no MIDATL/SOUTH/WEST macro aggregates)

Normals:
- **Generation normals (9):** Solar (4: RTO, MIDATL, WEST, SOUTH), Wind (4: RTO, WEST, MIDATL, SOUTH), Hydro (1: RTO)

{% enddocs %}
