{{
  config(
    materialized='view'
  )
}}

---------------------------
-- MODEL RUN FORECASTS
---------------------------

WITH models AS (
    SELECT
        rank_forecast_execution_timestamps AS forecast_rank,
        labelled_forecast_execution_timestamp AS forecast_label,
        forecast_execution_datetime,
        forecast_execution_date,
        cycle,
        forecast_date,
        count_forecast_days,
        max_forecast_days,
        model,
        bias_corrected,
        region,

        -- forecast values
        electric_cdd,
        electric_hdd,
        gas_cdd,
        gas_hdd,
        pw_cdd,
        pw_hdd,

        -- run-over-run changes (12hr difference)
        electric_cdd_12hr_difference AS electric_cdd_diff_run_over_run,
        electric_hdd_12hr_difference AS electric_hdd_diff_run_over_run,
        gas_cdd_12hr_difference AS gas_cdd_diff_run_over_run,
        gas_hdd_12hr_difference AS gas_hdd_diff_run_over_run,
        pw_cdd_12hr_difference AS pw_cdd_diff_run_over_run,
        pw_hdd_12hr_difference AS pw_hdd_diff_run_over_run

    FROM {{ ref('staging_v1_wdd_forecast_models') }}
),

---------------------------
-- WSI BLEND FORECASTS
---------------------------

wsi AS (
    SELECT
        rank_forecast_execution_timestamps AS forecast_rank,
        labelled_forecast_execution_timestamp AS forecast_label,
        forecast_execution_datetime,
        forecast_execution_date,
        NULL::TEXT AS cycle,
        forecast_date,
        count_forecast_days,
        max_forecast_days,
        model,
        bias_corrected,
        region,

        -- forecast values
        electric_cdd,
        electric_hdd,
        gas_cdd,
        gas_hdd,
        pw_cdd,
        pw_hdd,

        -- run-over-run changes
        electric_cdd_difference AS electric_cdd_diff_run_over_run,
        electric_hdd_difference AS electric_hdd_diff_run_over_run,
        gas_cdd_difference AS gas_cdd_diff_run_over_run,
        gas_hdd_difference AS gas_hdd_diff_run_over_run,
        pw_cdd_difference AS pw_cdd_diff_run_over_run,
        pw_hdd_difference AS pw_hdd_diff_run_over_run

    FROM {{ ref('staging_v1_wdd_forecast_wsi') }}
)

SELECT * FROM models
UNION ALL
SELECT * FROM wsi
