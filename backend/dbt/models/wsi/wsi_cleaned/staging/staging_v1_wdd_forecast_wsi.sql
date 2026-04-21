{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- WSI BLEND FORECASTS
---------------------------

WITH COMPLETE_FORECASTS AS (
    SELECT * FROM {{ ref('staging_v1_wdd_forecast_2_complete') }}
    WHERE model = 'WSI'
),

---------------------------
-- RANK EXECUTION TIMES
---------------------------

DISTINCT_EXECUTIONS AS (
    SELECT DISTINCT forecast_execution_datetime
    FROM COMPLETE_FORECASTS
),

RANKED_EXECUTIONS AS (
    SELECT
        forecast_execution_datetime,
        MAX(forecast_execution_datetime) OVER () AS latest_forecast_execution_datetime,
        DENSE_RANK() OVER (ORDER BY forecast_execution_datetime DESC) AS rank_forecast_execution_timestamps,
        forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
            AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
        AS is_friday_12z,
        DENSE_RANK() OVER (
            PARTITION BY (
                forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
                AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
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
            WHEN rank_forecast_execution_timestamps = 2 THEN '24hrs Ago'
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

        -- difference from previous forecast (WSI blend single difference)
        f.electric_cdd_difference,
        f.electric_hdd_difference,
        f.gas_cdd_difference,
        f.gas_hdd_difference,
        f.pw_cdd_difference,
        f.pw_hdd_difference,

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

        -- difference totals across forecast period
        SUM(f.electric_cdd_difference) OVER (w_total) AS electric_cdd_difference_total,
        SUM(f.electric_hdd_difference) OVER (w_total) AS electric_hdd_difference_total,
        SUM(f.gas_cdd_difference) OVER (w_total) AS gas_cdd_difference_total,
        SUM(f.gas_hdd_difference) OVER (w_total) AS gas_hdd_difference_total,
        SUM(f.pw_cdd_difference) OVER (w_total) AS pw_cdd_difference_total,
        SUM(f.pw_hdd_difference) OVER (w_total) AS pw_hdd_difference_total

    FROM COMPLETE_FORECASTS f
    JOIN LABELLED_EXECUTIONS r ON f.forecast_execution_datetime = r.forecast_execution_datetime
    WINDOW w_total AS (
        PARTITION BY f.forecast_execution_datetime, f.model, f.bias_corrected, f.region
    )
)

SELECT * FROM FINAL
ORDER BY region, rank_forecast_execution_timestamps, model, bias_corrected, forecast_date
