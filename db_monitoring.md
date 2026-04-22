# Database analysis — Azure Postgres `helioscta`

Snapshot taken 2026-04-22 via the queries in `.db_monitoring/`.

## Headline state
- **245 GB** in the `helioscta` DB, all on `pg_default`. No wraparound risk (XID age 18.5 M, limit 200 M).
- Connections: 33 of 200 slots used. Connection count is fine; **wait-time is the pressure point**.
- No current lock blockers. The bottleneck right now is **IO + autovacuum catch-up**, not contention.

## Critical — disk/cache pressure on a handful of hot tables
The DB-wide `helioscta` cache hit ratio is only **89.6 %** (target 95–99 %). The damage is concentrated:

| Table | Heap hit % | Heap blocks read from disk | Note |
|---|---|---|---|
| `pjm.seven_day_load_forecast` | **2.6 %** | 503 M | 162 K rows, scanned thousands of times |
| `wsi_cleaned.temp_observed_hourly` | **3.6 %** | 2.35 B | 55 M rows, seq-scanned 3 K times |
| `gridstatus.ercot_solar_actual_and_forecast_by_geo_region_hourly` | 11.8 % | 2.06 B | |
| `gridstatus.ercot_wind_actual_and_forecast_by_geo_region_hourly` | 14.1 % | 1.90 B | |
| `ercot.dam_stlmnt_pnt_prices` | **16.1 %** | **8.08 B** | 45 M rows; never autovacuumed, last autoanalyze 2026-02-15 |
| `gridstatus.ercot_fuel_mix` | n/a | 386 K seq scans × 423 B tup read on a 1.2 M row table | Missing an index |

`pg_stat_database` shows **40 TB of cumulative temp files** written on `helioscta` — queries are spilling to disk for sorts/hashes. `work_mem` is likely too low for the dbt workload.

## High — ~1.85 GB of unused indexes (116 of them)
Top offenders:

| Table | Index size |
|---|---|
| `meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly` (hash-named) | 362 MB |
| `meteologica_cleaned_v2.meteologica_pjm_demand_forecast_hourly` × 2 | 310 + 277 MB |
| `pjm.five_min_instantaneous_load_v1_2025_oct_15` | 103 MB |
| `meteologica_cleaned.meteologica_pjm_demand_observation_5min` | 68 MB |
| A dozen `idx_meteo_pjm_*_issue_date` indexes on PJM wind/solar forecast tables | ~8.6 MB each |

Only one true duplicate pair (`pjm.test_seven_day_outage_forecast_pkey` vs `idx_seven_day_outage_forecast_exec_date_region`, 7.3 MB) — low priority.

## High — autovacuum is chasing the dbt delete-then-insert pattern

| Table | Dead tup | `last_autovacuum` |
|---|---|---|
| `meteologica_cleaned.meteologica_pjm_demand_forecast_hourly` | 70 M | 2026-04-22 18:07 |
| `meteologica_cleaned.meteologica_pjm_generation_forecast_hourly` | 41 M | 2026-04-22 18:43 |
| `meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly` | 27 M | being vacuumed **now** (31 min) |
| `wsi.hourly_observed_temp_v2_20250722` | 9.5 M | **2026-02-25** — overdue |
| `ercot.dam_stlmnt_pnt_prices` | 4.0 M | **never** (only autoanalyzed once on 2026-02-15) |

Three autovacuum workers are currently running — autovac is saturated, not lazy. The `dead_pct > 500 %` entries are the symptom of dbt doing `DELETE … + INSERT …` between autoanalyze runs (the live count is stale, not truly inflated).

## Medium — seq scans that should be index-hitting
High `seq_tup_read` vs a reasonable `n_live_tup`:
- `ercot.dam_stlmnt_pnt_prices` — **883 B tuples** read via seq scan (45 M rows)
- `gridstatus.ercot_fuel_mix` — **423 B** via 386 K seq scans
- `meteologica_cleaned.meteologica_pjm_demand_forecast_hourly` — 179 B (9 GB table)
- `wsi_cleaned.temp_observed_hourly` — 148 B
- `pjm.seven_day_load_forecast` — 32 B (1.3 GB table)

These overlap exactly with the low-cache-hit list — same root cause.

## Medium — 42 tables have never been analyzed
Biggest unanalyzed:
- `pjm_cleaned_v3_2026_04_22.pjm_lmps_hourly` (3.9 M rows)
- `positions_cleaned.staging_v5_marex_and_nav_positions` (552 K rows)
- `pjm.seven_day_load_forecast` (308 K modifications since analyze)

The planner is guessing stats on those. Most look like freshly created dbt schemas.

## Medium — connection-pool leak
`204.191.16.134` has **14 idle sessions on `helioscta` + 3 on `postgres`**, oldest **4 d 23 h**. `application_name` is empty on all of them (so they're not dbt). Harmless capacity-wise (plenty of slots), but worth fixing the client so `application_name` is set and idle connections get recycled.

## Clean
- No blocking lock pairs
- No idle-in-transaction sessions older than 1 min
- XID age ~18 M (no wraparound risk)
- Only 1 duplicate index
- Per-DB commit/rollback ratio healthy

## Concrete next actions, ordered by bang-for-buck
1. **Raise `work_mem`** (e.g. 32 MB → 128 MB at session level for dbt) — collapses a lot of that 40 TB temp-file figure.
2. **Add targeted indexes** on `ercot.dam_stlmnt_pnt_prices`, `gridstatus.ercot_fuel_mix`, `meteologica_cleaned.meteologica_pjm_demand_forecast_hourly`, `wsi_cleaned.temp_observed_hourly`, `pjm.seven_day_load_forecast`. Column-level recommendations require inspecting the actual query patterns.
3. **Tune per-table autovacuum** on the `meteologica_cleaned.*` tables:
   ```sql
   ALTER TABLE meteologica_cleaned.meteologica_pjm_demand_forecast_hourly
   SET (autovacuum_vacuum_scale_factor = 0.02,
        autovacuum_analyze_scale_factor = 0.01);
   ```
4. **`VACUUM ANALYZE`** the 3 big never-analyzed tables (`pjm_cleaned_v3_2026_04_22.pjm_lmps_hourly` etc.).
5. **Drop the largest unused indexes** (362 MB + 310 MB + 277 MB ≈ ~1 GB of the 1.85 GB); then sweep the rest after a week of monitoring.
6. **Investigate the `204.191.16.134` idle pool** — check whichever service is running there for leaked connections and add `application_name`.
