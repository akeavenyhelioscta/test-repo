{{
  config(
    materialized='view'
  )
}}

----------------------------------------------------
-- FORECAST RUN REVISIONS — one row per forecast vintage of each delivery.
--
-- pjm_model_outputs.forecast_runs keeps every run. For a given
-- (model_name, target_date) the forecast for that delivery date is
-- re-issued each run_date as it approaches — e.g. a daily forecaster
-- writes run_date = target_date - 7 (made 7 days out), ...,
-- target_date - 1 (the latest run before delivery). This view is that
-- revision trail:
--
--   * forecast_run_revision — ROW_NUMBER over (model_name, target_date)
--     ordered run_date DESC. 1 = the latest model run for this delivery
--     (largest run_date); it climbs as you go back through earlier
--     vintages, so the highest value is the earliest / furthest-out
--     forecast — for a complete daily 7-day history, 1..7 with 7 =
--     "predicted 7 days out".
--   * onpeak_forecast — the OnPeak HE 8-23 headline the publisher
--     promotes off payload->'blocks', as it stood at this vintage.
--   * payload — the full jsonb (KnnPayload | IcePayload, keyed by
--     model_family) so the frontend can rebuild the hourly / band
--     tables per (delivery, vintage).
--
-- Grain: one row per (model_name, target_date, run_date). Re-runs of
-- the same vintage (same model/target/run_date, different run_id)
-- collapse to the latest by created_at. Output ordered model_name,
-- target_date, then run_date DESC (so forecast_run_revision goes 1..7
-- top to bottom per delivery — latest run first).
----------------------------------------------------

WITH per_vintage AS (
    SELECT DISTINCT ON (model_name, target_date, run_date)
        model_name
        ,model_family
        ,target_date
        ,run_date
        ,da_lmp_total_onpeak_forecast
        ,payload
        ,run_id
        ,created_at
    FROM {{ source('pjm_model_outputs', 'forecast_runs') }}
    ORDER BY model_name, target_date, run_date, created_at DESC
)

SELECT
    model_name
    ,model_family
    ,target_date
    ,run_date
    ,da_lmp_total_onpeak_forecast
    ,payload
    ,run_id
    ,created_at
    ,ROW_NUMBER() OVER (
        PARTITION BY model_name, target_date ORDER BY run_date DESC
     )                                   AS forecast_run_revision
FROM per_vintage
ORDER BY model_name, target_date, run_date DESC
