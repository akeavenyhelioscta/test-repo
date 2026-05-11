# Promoting `pjm_model_outputs.forecast_runs` into `scorecard_da_onpeak_wh`

**Status:** design / decision document. No SQL yet.
**Date:** 2026-05-08
**Author:** planning pass per `.claude/prompts/Fri_May_08_promote_forecast_runs_to_scorecard.md`

---

## 1. Summary

- Add a **new** sibling mart `scorecard_da_onpeak_wh_with_forecasts` (view) that LEFT-joins per-model OnPeak HE 8-23 forecasts onto every row of the existing `scorecard_da_onpeak_wh`.
- Spine stays ICE-priced delivery dates. Existing `scorecard_da_onpeak_wh.sql` is **untouched** — `ice_error` semantics, columns, and tests are preserved exactly.
- Per-model wide layout: `knn_pjm_rto_hourly_onpeak`, `knn_pjm_rto_hourly_error`, `baseline_meteo_da_onpeak`, `baseline_meteo_da_error`. Two models today (`like_day_model_knn/pjm_rto_hourly`, `baseline_meteo_da_price`); a third would extend the column list, not the mart shape.
- Run-selection: latest by `created_at_utc` per `(model_name, target_date)`, mirroring `frontend/lib/server/forecastRuns.ts:116-128` (`readLatestForecastRun`). DA-cutoff-vintage selection deferred to a follow-up.
- New dbt source declared in `models/scorecards/sources.yml` (file currently doesn't exist after the prior scorecard rewrite, so this is greenfield): schema `pjm_model_outputs`, table `forecast_runs`.
- Out of scope: changes to either `publish.py`, RT scorecards, hourly-grain comparison, anything beyond OnPeak HE 8-23.

---

## 2. Run-selection policy

**Decision:** for each `(model_name, target_date)`, pick the row with the maximum `created_at_utc`. One forecast row per `(model, delivery_date)` joins the scorecard.

**Why:** mirrors the canonical "latest by `created_at_utc`" convention already established in `frontend/lib/server/forecastRuns.ts:116-128` and documented in `modelling/da_models/like_day_model_knn/pjm_rto_hourly/publish.py:19-21`. Both publishers upsert via `ON CONFLICT (model_name, target_date, run_id) DO UPDATE` (see `publish.py:326`), so re-runs of the *same* `run_id` overwrite in place; new `run_id` values create new rows. Latest-by-timestamp is the only stable definition of "the forecast" today and the rest of the read path agrees.

**Rejected alternative — DA-cutoff vintage:** filter `created_at_utc < (target_date - 1 day) || ' 14:30:00 UTC'::timestamptz` (PJM DA market closes ~10:30 ET / 14:30 UTC). This is a better measure of "what we forecast at the moment we could have placed a trade". It's the right answer eventually, but requires:

1. Confirming the DA-cutoff timestamp lives somewhere reusable (it doesn't — `lead_days=1` is the closest existing convention, see `modelling/CLAUDE.md` Forecast-vs-RT rule).
2. Deciding what to do with target dates whose only run is post-cutoff (drop? keep with a flag?).

Both questions are out of scope for this pass. Captured in section 11 as the highest-priority follow-up.

**Rejected alternative — keep all runs:** ungrouped LEFT JOIN would explode the spine (one delivery_date row × N runs per model = many rows). Breaks the unique-`delivery_date` test on the existing scorecard's PK and forces every consumer to dedupe. No.

**Implementation hint for the follow-up scaffolder:** use `DISTINCT ON (model_name, target_date) ... ORDER BY model_name, target_date, created_at_utc DESC`. Cheaper than a window function and Postgres-native.

---

## 3. Forecast number extraction

**Decision:** the OnPeak HE 8-23 number is extracted per-model with **different** jsonb paths because the `blocks[]` schema differs between the two publishers. This is the most important call-out in this doc.

### KNN (`like_day_model_knn/pjm_rto_hourly`)

Source: `modelling/da_models/like_day_model_knn/pjm_rto_hourly/publish.py:120-142`. `blocks[]` element shape:

```
{ "block": "OnPeak"|"OffPeak"|"Flat", "quantile_label": "P10"|"P25"|"P50"|"Forecast"|"P75"|"P90", "value": float }
```

Extraction (pseudo):

```
SELECT (b->>'value')::numeric
FROM jsonb_array_elements(payload->'blocks') AS b
WHERE b->>'block' = 'OnPeak'
  AND b->>'quantile_label' = COALESCE(<prefer 'Forecast'>, 'P50')
LIMIT 1
```

COALESCE precedence: prefer `quantile_label='Forecast'` (the deterministic point — see `_QUANTILE_LABELS` at `publish.py:117`), fall back to `'P50'` if absent.

### baseline_meteo_da_price

Source: `modelling/da_models/baseline_meteo_da_price/publish.py:170-190`. `blocks[]` element shape **does not have `quantile_label`** — it has `series`:

```
{ "series": "Det"|"ENS Avg"|"ENS Bottom"|"ENS Top", "block": "OnPeak"|"OffPeak"|"Flat", "value": float }
```

Extraction (pseudo):

```
SELECT (b->>'value')::numeric
FROM jsonb_array_elements(payload->'blocks') AS b
WHERE b->>'block' = 'OnPeak'
  AND b->>'series' = 'Det'
LIMIT 1
```

`Det` (deterministic) is the point forecast; `ENS Avg` is the ensemble mean and would be the natural P50 fallback. Use `Det` exclusively to keep the comparison "model-against-its-own-point", and document `ENS Avg` as the future fallback if a payload ever lands without `Det`.

### Why two paths and not one normalized path

The publishers' `blocks[]` schemas were designed independently and are stable contracts to the frontend. Per implementation rule 6 in the prompt, we don't change `publish.py`. The dbt mart adapts.

A future cleanup (section 11, follow-up #3) is to harmonize both publishers on a shared `{block, series, quantile_label, value}` shape — but until then, the scorecard owns the per-model dispatch. Centralize the dispatch in **one CTE per model** rather than scattering `WHEN model_name = ...` across the SELECT.

---

## 4. Multi-model layout

**Decision:** wide (per-model columns), not long.

**Resulting columns** (added to the existing scorecard column list):

| Column | Meaning |
|---|---|
| `knn_pjm_rto_hourly_onpeak` | KNN forecast, OnPeak HE 8-23, $/MWh, rounded to 2 decimals |
| `knn_pjm_rto_hourly_run_id` | UUID of the latest run used (debugging breadcrumb) |
| `knn_pjm_rto_hourly_created_at_utc` | When the chosen run was published |
| `knn_pjm_rto_hourly_error` | `da_lmp_total - knn_pjm_rto_hourly_onpeak`. Positive = forecast under-priced. NULL when either side is NULL. |
| `baseline_meteo_da_onpeak` | Baseline (Det) forecast, OnPeak HE 8-23 |
| `baseline_meteo_da_run_id` | UUID of the latest run used |
| `baseline_meteo_da_created_at_utc` | When the chosen run was published |
| `baseline_meteo_da_error` | `da_lmp_total - baseline_meteo_da_onpeak`. Same sign convention as ICE error. |

**Why wide:** today there are exactly 2 models with a shared OnPeak comparand. Wide reads naturally for a row-per-delivery scorecard ("what did each model say, what did we clear, what did ICE settle"). Direct comparison columns like `knn_minus_baseline` become trivial to add in a follow-up.

**Why NOT long:** long shape (`(delivery_date, model_name, onpeak_forecast, error)` per row) breaks the existing unique-`delivery_date` test, complicates the join with ICE columns, and forces every reader to pivot for display. Long is the right call once the model count exceeds ~5 — call it a future migration in section 11.

**Naming convention:** `<model_family_short>_<onpeak|run_id|created_at_utc|error>`. Family-short prefixes are stable strings the dbt mart owns — they're not derived from `model_name` literals at query time (because `model_name` includes hub/horizon suffixes that don't belong in column names). Mapping is:

- `like_day_model_knn` family + `pjm_rto_hourly` model_class → `knn_pjm_rto_hourly_*`
- `baseline_meteo_da_price` family → `baseline_meteo_da_*`

The follow-up scaffolder writes the family→prefix mapping as a Jinja dict at the top of the SQL.

---

## 5. Mart placement

**Decision:** new mart `scorecard_da_onpeak_wh_with_forecasts.sql`, sibling to the existing scorecard. Materialization: `view` (matches `models/scorecards/+materialized: view` policy in `dbt_project.yml`).

**Why:** existing `scorecard_da_onpeak_wh` is the realized DA-vs-ICE mart. Per implementation rule 7, its semantics MUST NOT change. Adding ~8 forecast columns to the same view would (a) confuse readers about the spine ("is this still ICE-spined or also forecast-spined?"), (b) inflate the column count past comfortable, and (c) tightly couple two release cycles — when we eventually re-shape the forecast columns into long format, the ICE-vs-DA scorecard shouldn't churn.

**Why NOT a fully separate mart with its own spine:** that's section 8's question. Punchline there: spine stays ICE-priced delivery_dates, identical to the parent. So the new mart is structurally `scorecard_da_onpeak_wh + forecast columns`, expressed as `SELECT … FROM ref('scorecard_da_onpeak_wh') s LEFT JOIN <forecast CTEs> …`.

**Naming:** `scorecard_da_onpeak_wh_with_forecasts` is 38 chars — fits well under the 51-char dbt-naming cap (per `MEMORY.md` `feedback_dbt_naming_length.md`).

**Schema entry:** new model gets its own block in `models/scorecards/schema.yml` with column descriptions + `unique`/`not_null` tests on `delivery_date` (same as parent).

---

## 6. dbt source declaration

**Decision:** declare `pjm_model_outputs.forecast_runs` in a new file `backend/dbt/dbt_azure_postgresql/models/scorecards/sources.yml`. Confirmed via grep that no other dbt file references `pjm_model_outputs` or `forecast_runs` today.

Source-block contract (yaml shape, no SQL):

```
sources:
  - name: pjm_model_outputs
    schema: pjm_model_outputs
    tables:
      - name: forecast_runs
        description: "Per-run jsonb payload published by modelling/da_models/<family>/publish.py. PK (model_name, target_date, run_id). Read path: latest-by-created_at_utc per (model, target_date). See root CLAUDE.md 'Cross-subtree contracts' for the contract."
        columns:
          - name: model_name        # text, e.g. 'like_day_model_knn_pjm_rto_hourly'
          - name: model_family      # text, e.g. 'like_day_model_knn'
          - name: model_class       # text, e.g. 'pjm_rto_hourly'
          - name: target_date       # date
          - name: run_id            # uuid (text)
          - name: created_at_utc    # timestamptz
          - name: created_at_local  # timestamp without time zone (naive MST/MDT)
          - name: payload           # jsonb
```

**Freshness:** none. Forecasts are published on-demand by the runner, not on a fixed schedule, and the scorecard tolerates missing rows (LEFT JOIN). Adding `freshness:` would cause spurious failures on weekends or paused runs.

**Why this file location:** the scorecard is the only dbt consumer of `forecast_runs` today. Co-locating the source declaration with its sole consumer matches the routing rule's "subtree-specific" principle. If a second consumer appears (e.g. `models/eval/`), lift to a project-level `sources/` directory then.

**Reference style:** `{{ source('pjm_model_outputs', 'forecast_runs') }}` everywhere it's read. Never hardcode the schema-qualified table name.

---

## 7. Forecast error column(s)

**Decision:** one `<model>_error` column per model. Definition:

```
<model>_error = da_lmp_total - <model>_onpeak
```

Both rounded to 2 decimals (matches existing `ice_error` rounding at `scorecard_da_onpeak_wh.sql:97`).

**Sign convention:** **positive = forecast under-priced the realized clear** (i.e. realized DA was higher than what the model predicted). Identical sign convention to `ice_error = da_lmp_total - ice_vwap`. This makes the three error columns directly comparable: a row with `ice_error = +5` and `knn_pjm_rto_hourly_error = +3` says "market cleared $5 over ICE, forecast was $2 closer to the clear than ICE was".

**Why this convention and not `forecast - actual`:** consistency with the existing `ice_error` is the dominant force. Different signs across the same row would be a constant footgun for readers.

**NULL handling:** NULL when either operand is NULL. Forward-dated rows: `da_lmp_total` is NULL until PJM clears, so all error columns are NULL — same as `ice_error` already behaves. No special-case logic needed.

**Why no separate `_abs_error` or `_rmse` column:** scorecard is row-grain, not aggregate. Aggregates belong in a downstream view (`scorecard_da_onpeak_wh_summary` or similar) that GROUPs by week/month. Out of scope.

---

## 8. Backfill / history scope

**Decision:** spine = ICE-priced delivery_dates only. Identical to the parent mart. Forecast rows whose `target_date` was never priced by ICE do not appear.

**Why:** the scorecard's purpose is grading model + DA against the ICE settle. Without an ICE row, there's nothing to grade against. A separate mart can expose "all forecasts published" — but that's an evaluation harness, not a scorecard. Different concern, different home.

**Practical effect:** `forecast_runs` rows with `target_date` outside the ICE-priced range are silently dropped by the LEFT JOIN. Rows inside the range with no forecast (e.g. backfill: ICE has Jan 2024 deliveries, forecasts only started 2026) get NULL forecast columns — same NULL pattern as forward-dated rows. Readers who want "forecast coverage starting 2026-01-01" filter `WHERE knn_pjm_rto_hourly_run_id IS NOT NULL` themselves.

**Default per `<open_questions>` is honored.**

---

## 9. Cross-subtree contract update

Add the following bullet under "Cross-subtree contracts" in root `CLAUDE.md` (after the existing `forecast_runs` paragraph that ends with "Currently 8 columns…"):

> **Readers (dbt scorecards).** The `backend/dbt/dbt_azure_postgresql/models/scorecards/` tree consumes `forecast_runs` via the `pjm_model_outputs` source declared in `models/scorecards/sources.yml`. Selection rule mirrors the frontend: `DISTINCT ON (model_name, target_date) ORDER BY ... created_at_utc DESC`. Per-model OnPeak HE 8-23 numbers are extracted from `payload->'blocks'` with model-specific filters (KNN: `quantile_label='Forecast'`; baseline_meteo_da_price: `series='Det'`) — the publishers' `blocks[]` shapes differ. Any new forecaster that wants its OnPeak number in the scorecard must either match one of these two shapes or extend the dispatch CTE in `scorecard_da_onpeak_wh_with_forecasts.sql`.

This belongs in repo-root `CLAUDE.md` (not a nested CLAUDE.md), per the existing routing rule, because the contract spans `modelling/` and `backend/dbt/`.

---

## 10. Concrete proposed mart shape

Final column list for `models/scorecards/scorecard_da_onpeak_wh_with_forecasts.sql`. Materialization: `view`. Grain: one row per `delivery_date` (unique, not_null).

| # | Column | Type | Source | Description |
|---|---|---|---|---|
| 1 | `delivery_date` | date | `ref('scorecard_da_onpeak_wh')` | Delivery date the ICE strip prices. PK. |
| 2 | `da_lmp_total` | numeric(?,2) | parent | PJM Western Hub DA total LMP, HE 8-23 mean. NULL when PJM hasn't cleared. |
| 3 | `da_lmp_system_energy` | numeric(?,2) | parent | DA system-energy component, HE 8-23 mean. |
| 4 | `da_lmp_congestion` | numeric(?,2) | parent | DA congestion component, HE 8-23 mean. |
| 5 | `da_lmp_loss` | numeric(?,2) | parent | DA marginal-loss component, HE 8-23 mean. |
| 6 | `ice_start_date` | date | parent | ICE strip start. |
| 7 | `ice_end_date` | date | parent | ICE strip end. |
| 8 | `product` | text | parent | `'PDA D1-IUS'` or `'PDO P1-IUS'`. |
| 9 | `ice_description` | text | parent | Human-readable ICE product label. |
| 10 | `ice_trade_date_start` | date | parent | First ICE session that priced the strip. |
| 11 | `ice_trade_date_last` | date | parent | Last ICE session. |
| 12 | `ice_open` | numeric(?,2) | parent | |
| 13 | `ice_high` | numeric(?,2) | parent | |
| 14 | `ice_low` | numeric(?,2) | parent | |
| 15 | `ice_close` | numeric(?,2) | parent | |
| 16 | `ice_vwap` | numeric(?,2) | parent | Volume-weighted average across the strip's life. Primary `ice_error` reference. |
| 17 | `ice_volume` | numeric | parent | |
| 18 | `ice_buy_volume` | numeric | parent | |
| 19 | `ice_sell_volume` | numeric | parent | |
| 20 | `ice_error` | numeric(?,2) | parent | `da_lmp_total - ice_vwap`. Positive = market under-priced. |
| 21 | `knn_pjm_rto_hourly_onpeak` | numeric(?,2) | `forecast_runs` (KNN, latest by `created_at_utc`) | KNN model OnPeak HE 8-23 forecast. NULL if no run for `target_date`. |
| 22 | `knn_pjm_rto_hourly_run_id` | text (uuid) | same | Run ID used (debug breadcrumb). |
| 23 | `knn_pjm_rto_hourly_created_at_utc` | timestamptz | same | When the chosen run was published. |
| 24 | `knn_pjm_rto_hourly_error` | numeric(?,2) | computed | `da_lmp_total - knn_pjm_rto_hourly_onpeak`. Positive = forecast under-priced. |
| 25 | `baseline_meteo_da_onpeak` | numeric(?,2) | `forecast_runs` (baseline, latest, `series='Det'`) | Baseline deterministic OnPeak HE 8-23. |
| 26 | `baseline_meteo_da_run_id` | text (uuid) | same | |
| 27 | `baseline_meteo_da_created_at_utc` | timestamptz | same | |
| 28 | `baseline_meteo_da_error` | numeric(?,2) | computed | Same sign convention as `ice_error`. |

**Tests** (in `schema.yml`):

- `delivery_date`: `unique`, `not_null`
- `ice_start_date`, `ice_end_date`, `product`, `ice_vwap`: `not_null` (inherited from parent)
- `product`: `accepted_values: ['PDA D1-IUS', 'PDO P1-IUS']`
- New forecast columns: no tests at this stage. NULL is a valid state (forward-dated, pre-coverage, or simply un-run).

**Order policy:** `ORDER BY delivery_date DESC` (matches parent).

---

## 11. Open risks + follow-ups

Ordered by priority.

1. **DA-cutoff vintage selection.** Current "latest by `created_at_utc`" can include runs published *after* the DA market cleared — useful for evaluation, misleading for "could we have traded on this?". Blocker: need a canonical DA-cutoff timestamp expression (`(target_date - 1 day) || '14:30:00 UTC'` is approximately right but ignores DST and no-cutoff-on-weekend semantics). Resolve with the user; if confirmed, add a parallel set of `<model>_pre_cutoff_*` columns rather than replacing the existing ones.

2. **Weekend forecast availability.** Confirmed via `modelling/da_models/common/configs.py:19-25` that the calendar maps Sat/Sun to `'weekend'`, and `like_day_model_knn/pjm_rto_hourly/printers.py:140-141` shows weekend-grouping config exists. **What's not confirmed** is whether the runner is actually scheduled on weekends — that's a Prefect/operational question. Effect on this design: zero. If weekends aren't run, weekend rows have NULL forecast columns; if they are, they join naturally. No code change either way.

3. **Payload-shape harmonization.** KNN's `blocks[]` uses `quantile_label`; baseline's uses `series`. Two divergent contracts is fine for two models; at three+ this becomes per-model-dispatch noise. Future cleanup: add a shared schema (e.g. `{ block, series_or_label, value }`) and migrate both publishers. Coordinate with frontend reader changes.

4. **`baseline_meteo_da_price` model_name string.** The publish helper signature in `modelling/da_models/baseline_meteo_da_price/publish.py:347` takes `model_name` from the caller — the actual literal isn't pinned in this doc because the caller (probably `run_baseline_da.py` or a Prefect flow) wasn't read. The follow-up scaffolder must grep for the call site to confirm the literal before pinning the column-prefix mapping. Same caveat for KNN: `model_name='like_day_model_knn'` vs `'like_day_model_knn_pjm_rto_hourly'` matters for the `WHERE model_name = ...` filter.

5. **Source-not-yet-populated edge case.** First run of `dbt build` on a new database where `pjm_model_outputs.forecast_runs` doesn't exist yet would fail. Not a real risk in production (the table exists), but local-dev setup docs may need a note. Defer.

6. **Aggregate scorecard.** Row-grain mart is the foundation; trader-facing summary ("model A beat ICE on 60% of delivery dates last 30 days") is a downstream view. Out of scope here, captured for future work.

---

## What the follow-up implementation prompt scaffolds

A new dbt model file `models/scorecards/scorecard_da_onpeak_wh_with_forecasts.sql` (view, ~80-120 lines) plus a new `models/scorecards/sources.yml` and a schema.yml block, all driven by the column contract in section 10 and the per-model dispatch rules in section 3 — no further design decisions required.
