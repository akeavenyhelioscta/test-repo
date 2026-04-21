{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- NWP MODEL FORECASTS
---------------------------

WITH COMPLETE_FORECASTS AS (
    SELECT * FROM {{ ref('staging_v1_wdd_forecast_2_complete') }}
    WHERE model IN ('GFS_OP', 'GFS_ENS', 'GEM_OP', 'GEM_ENS', 'ECMWF_OP', 'ECMWF_ENS', 'ECMWF_AIFS', 'ECMWF_AIFS_ENS')
        AND cycle IN ('00Z', '12Z')
),

---------------------------
-- RANK EXECUTION TIMES
---------------------------

DISTINCT_EXECUTIONS AS (
    SELECT DISTINCT model, forecast_execution_datetime
    FROM COMPLETE_FORECASTS
),

RANKED_EXECUTIONS AS (
    SELECT
        model,
        forecast_execution_datetime,
        MAX(forecast_execution_datetime) OVER (PARTITION BY model) AS latest_forecast_execution_datetime,
        DENSE_RANK() OVER (PARTITION BY model ORDER BY forecast_execution_datetime DESC) AS rank_forecast_execution_timestamps,
        forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
            AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
            AND EXTRACT(HOUR FROM forecast_execution_datetime::TIMESTAMP) = 12
            AND EXTRACT(MINUTE FROM forecast_execution_datetime::TIMESTAMP) = 0
        AS is_friday_12z,
        DENSE_RANK() OVER (
            PARTITION BY model, (
                forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
                AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
                AND EXTRACT(HOUR FROM forecast_execution_datetime::TIMESTAMP) = 12
                AND EXTRACT(MINUTE FROM forecast_execution_datetime::TIMESTAMP) = 0
            )
            ORDER BY forecast_execution_datetime DESC
        ) AS rank_friday_12z
    FROM DISTINCT_EXECUTIONS
),

---------------------------
-- LABEL FORECASTS
---------------------------

LABELLED_EXECUTIONS AS (
    SELECT
        *,
        CASE
            WHEN rank_forecast_execution_timestamps = 1 THEN 'Current Forecast'
            WHEN rank_forecast_execution_timestamps = 2 THEN '12hrs Ago'
            WHEN rank_forecast_execution_timestamps = 3 THEN '24hrs Ago'
            WHEN is_friday_12z AND rank_friday_12z = 1 THEN 'Friday 12z'
            ELSE NULL
        END AS labelled_forecast_execution_timestamp
    FROM RANKED_EXECUTIONS
),

---------------------------
-- FINAL JOIN
---------------------------

FINAL AS (
    SELECT
        r.rank_forecast_execution_timestamps,
        r.labelled_forecast_execution_timestamp,
        f.forecast_execution_datetime,
        f.forecast_execution_date,
        f.cycle,
        f.forecast_date,
        f.count_forecast_days,
        f.max_forecast_days,
        f.model,
        f.bias_corrected,
        f.region,

        -- forecast values
        f.electric_cdd,
        f.electric_hdd,
        f.gas_cdd,
        f.gas_hdd,
        f.pw_cdd,
        f.pw_hdd,

        -- normals (WSI-provided 10yr avg)
        f.electric_cdd_normal,
        f.electric_hdd_normal,
        f.gas_cdd_normal,
        f.gas_hdd_normal,
        f.pw_cdd_normal,
        f.pw_hdd_normal,

        -- departure from normal
        f.electric_cdd_dfn,
        f.electric_hdd_dfn,
        f.gas_cdd_dfn,
        f.gas_hdd_dfn,
        f.pw_cdd_dfn,
        f.pw_hdd_dfn,

        -- 6hr differences
        f.electric_cdd_6hr_difference,
        f.electric_hdd_6hr_difference,
        f.gas_cdd_6hr_difference,
        f.gas_hdd_6hr_difference,
        f.pw_cdd_6hr_difference,
        f.pw_hdd_6hr_difference,

        -- 12hr differences
        f.electric_cdd_12hr_difference,
        f.electric_hdd_12hr_difference,
        f.gas_cdd_12hr_difference,
        f.gas_hdd_12hr_difference,
        f.pw_cdd_12hr_difference,
        f.pw_hdd_12hr_difference,

        -- 18hr differences
        f.electric_cdd_18hr_difference,
        f.electric_hdd_18hr_difference,
        f.gas_cdd_18hr_difference,
        f.gas_hdd_18hr_difference,
        f.pw_cdd_18hr_difference,
        f.pw_hdd_18hr_difference,

        -- 24hr differences
        f.electric_cdd_24hr_difference,
        f.electric_hdd_24hr_difference,
        f.gas_cdd_24hr_difference,
        f.gas_hdd_24hr_difference,
        f.pw_cdd_24hr_difference,
        f.pw_hdd_24hr_difference,

        -- 30hr differences
        f.electric_cdd_30hr_difference,
        f.electric_hdd_30hr_difference,
        f.gas_cdd_30hr_difference,
        f.gas_hdd_30hr_difference,
        f.pw_cdd_30hr_difference,
        f.pw_hdd_30hr_difference,

        -- totals across forecast period
        SUM(f.electric_cdd) OVER (w_total) AS electric_cdd_total,
        SUM(f.electric_hdd) OVER (w_total) AS electric_hdd_total,
        SUM(f.gas_cdd) OVER (w_total) AS gas_cdd_total,
        SUM(f.gas_hdd) OVER (w_total) AS gas_hdd_total,
        SUM(f.pw_cdd) OVER (w_total) AS pw_cdd_total,
        SUM(f.pw_hdd) OVER (w_total) AS pw_hdd_total,

        -- normal totals across forecast period
        SUM(f.electric_cdd_normal) OVER (w_total) AS electric_cdd_normal_total,
        SUM(f.electric_hdd_normal) OVER (w_total) AS electric_hdd_normal_total,
        SUM(f.gas_cdd_normal) OVER (w_total) AS gas_cdd_normal_total,
        SUM(f.gas_hdd_normal) OVER (w_total) AS gas_hdd_normal_total,
        SUM(f.pw_cdd_normal) OVER (w_total) AS pw_cdd_normal_total,
        SUM(f.pw_hdd_normal) OVER (w_total) AS pw_hdd_normal_total,

        -- 12hr difference totals across forecast period
        SUM(f.electric_cdd_12hr_difference) OVER (w_total) AS electric_cdd_12hr_difference_total,
        SUM(f.electric_hdd_12hr_difference) OVER (w_total) AS electric_hdd_12hr_difference_total,
        SUM(f.gas_cdd_12hr_difference) OVER (w_total) AS gas_cdd_12hr_difference_total,
        SUM(f.gas_hdd_12hr_difference) OVER (w_total) AS gas_hdd_12hr_difference_total,
        SUM(f.pw_cdd_12hr_difference) OVER (w_total) AS pw_cdd_12hr_difference_total,
        SUM(f.pw_hdd_12hr_difference) OVER (w_total) AS pw_hdd_12hr_difference_total

    FROM COMPLETE_FORECASTS f
    JOIN LABELLED_EXECUTIONS r
      ON f.model = r.model
     AND f.forecast_execution_datetime = r.forecast_execution_datetime
    WINDOW w_total AS (
        PARTITION BY f.forecast_execution_datetime, f.model, f.cycle, f.bias_corrected, f.region
    )
)

SELECT * FROM FINAL
ORDER BY region, rank_forecast_execution_timestamps, model, cycle, bias_corrected, forecast_date
