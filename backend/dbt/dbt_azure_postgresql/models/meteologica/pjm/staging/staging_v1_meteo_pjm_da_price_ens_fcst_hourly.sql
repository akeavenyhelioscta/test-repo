{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Western-Hub DA Price Forecast — ECMWF ensemble (Hourly)
-- 51 ensemble members (ens_00..ens_50) plus average/bottom/top.
-- Western-Hub only (single price node, no region dim).
-- Produces UTC/timezone/local triplets for issue time, ranks by issue time
-- (most recent first).
-- Grain: 1 row per forecast_execution_datetime x forecast_date x hour_ending
---------------------------

WITH RAW AS (
    SELECT
        update_id
        ,issue_date
        ,forecast_period_start
        ,average_price
        ,bottom_price
        ,top_price
        ,ens_00_price ,ens_01_price ,ens_02_price ,ens_03_price ,ens_04_price
        ,ens_05_price ,ens_06_price ,ens_07_price ,ens_08_price ,ens_09_price
        ,ens_10_price ,ens_11_price ,ens_12_price ,ens_13_price ,ens_14_price
        ,ens_15_price ,ens_16_price ,ens_17_price ,ens_18_price ,ens_19_price
        ,ens_20_price ,ens_21_price ,ens_22_price ,ens_23_price ,ens_24_price
        ,ens_25_price ,ens_26_price ,ens_27_price ,ens_28_price ,ens_29_price
        ,ens_30_price ,ens_31_price ,ens_32_price ,ens_33_price ,ens_34_price
        ,ens_35_price ,ens_36_price ,ens_37_price ,ens_38_price ,ens_39_price
        ,ens_40_price ,ens_41_price ,ens_42_price ,ens_43_price ,ens_44_price
        ,ens_45_price ,ens_46_price ,ens_47_price ,ens_48_price ,ens_49_price
        ,ens_50_price
    FROM {{ ref('src_meteo_pjm_wh_da_price_ens') }}
),

NORMALIZED AS (
    SELECT
        issue_date::TIMESTAMP AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS forecast_execution_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS forecast_execution_date

        ,forecast_period_start::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,average_price::NUMERIC AS da_price_ens_average
        ,bottom_price::NUMERIC AS da_price_ens_bottom
        ,top_price::NUMERIC    AS da_price_ens_top

        ,ens_00_price::NUMERIC AS da_price_ens_00
        ,ens_01_price::NUMERIC AS da_price_ens_01
        ,ens_02_price::NUMERIC AS da_price_ens_02
        ,ens_03_price::NUMERIC AS da_price_ens_03
        ,ens_04_price::NUMERIC AS da_price_ens_04
        ,ens_05_price::NUMERIC AS da_price_ens_05
        ,ens_06_price::NUMERIC AS da_price_ens_06
        ,ens_07_price::NUMERIC AS da_price_ens_07
        ,ens_08_price::NUMERIC AS da_price_ens_08
        ,ens_09_price::NUMERIC AS da_price_ens_09
        ,ens_10_price::NUMERIC AS da_price_ens_10
        ,ens_11_price::NUMERIC AS da_price_ens_11
        ,ens_12_price::NUMERIC AS da_price_ens_12
        ,ens_13_price::NUMERIC AS da_price_ens_13
        ,ens_14_price::NUMERIC AS da_price_ens_14
        ,ens_15_price::NUMERIC AS da_price_ens_15
        ,ens_16_price::NUMERIC AS da_price_ens_16
        ,ens_17_price::NUMERIC AS da_price_ens_17
        ,ens_18_price::NUMERIC AS da_price_ens_18
        ,ens_19_price::NUMERIC AS da_price_ens_19
        ,ens_20_price::NUMERIC AS da_price_ens_20
        ,ens_21_price::NUMERIC AS da_price_ens_21
        ,ens_22_price::NUMERIC AS da_price_ens_22
        ,ens_23_price::NUMERIC AS da_price_ens_23
        ,ens_24_price::NUMERIC AS da_price_ens_24
        ,ens_25_price::NUMERIC AS da_price_ens_25
        ,ens_26_price::NUMERIC AS da_price_ens_26
        ,ens_27_price::NUMERIC AS da_price_ens_27
        ,ens_28_price::NUMERIC AS da_price_ens_28
        ,ens_29_price::NUMERIC AS da_price_ens_29
        ,ens_30_price::NUMERIC AS da_price_ens_30
        ,ens_31_price::NUMERIC AS da_price_ens_31
        ,ens_32_price::NUMERIC AS da_price_ens_32
        ,ens_33_price::NUMERIC AS da_price_ens_33
        ,ens_34_price::NUMERIC AS da_price_ens_34
        ,ens_35_price::NUMERIC AS da_price_ens_35
        ,ens_36_price::NUMERIC AS da_price_ens_36
        ,ens_37_price::NUMERIC AS da_price_ens_37
        ,ens_38_price::NUMERIC AS da_price_ens_38
        ,ens_39_price::NUMERIC AS da_price_ens_39
        ,ens_40_price::NUMERIC AS da_price_ens_40
        ,ens_41_price::NUMERIC AS da_price_ens_41
        ,ens_42_price::NUMERIC AS da_price_ens_42
        ,ens_43_price::NUMERIC AS da_price_ens_43
        ,ens_44_price::NUMERIC AS da_price_ens_44
        ,ens_45_price::NUMERIC AS da_price_ens_45
        ,ens_46_price::NUMERIC AS da_price_ens_46
        ,ens_47_price::NUMERIC AS da_price_ens_47
        ,ens_48_price::NUMERIC AS da_price_ens_48
        ,ens_49_price::NUMERIC AS da_price_ens_49
        ,ens_50_price::NUMERIC AS da_price_ens_50
    FROM RAW
),

FORECAST_RANK AS (
    SELECT
        forecast_date
        ,forecast_execution_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date
            ORDER BY forecast_execution_datetime_local DESC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date
        FROM NORMALIZED
    ) sub
),

FINAL AS (
    SELECT
        n.forecast_execution_datetime_utc
        ,n.timezone
        ,n.forecast_execution_datetime_local
        ,r.forecast_rank
        ,n.forecast_execution_date

        ,(n.forecast_date + INTERVAL '1 hour' * (n.hour_ending - 1)) AS forecast_datetime
        ,n.forecast_date
        ,n.hour_ending

        ,n.da_price_ens_average
        ,n.da_price_ens_bottom
        ,n.da_price_ens_top
        ,n.da_price_ens_00, n.da_price_ens_01, n.da_price_ens_02, n.da_price_ens_03, n.da_price_ens_04
        ,n.da_price_ens_05, n.da_price_ens_06, n.da_price_ens_07, n.da_price_ens_08, n.da_price_ens_09
        ,n.da_price_ens_10, n.da_price_ens_11, n.da_price_ens_12, n.da_price_ens_13, n.da_price_ens_14
        ,n.da_price_ens_15, n.da_price_ens_16, n.da_price_ens_17, n.da_price_ens_18, n.da_price_ens_19
        ,n.da_price_ens_20, n.da_price_ens_21, n.da_price_ens_22, n.da_price_ens_23, n.da_price_ens_24
        ,n.da_price_ens_25, n.da_price_ens_26, n.da_price_ens_27, n.da_price_ens_28, n.da_price_ens_29
        ,n.da_price_ens_30, n.da_price_ens_31, n.da_price_ens_32, n.da_price_ens_33, n.da_price_ens_34
        ,n.da_price_ens_35, n.da_price_ens_36, n.da_price_ens_37, n.da_price_ens_38, n.da_price_ens_39
        ,n.da_price_ens_40, n.da_price_ens_41, n.da_price_ens_42, n.da_price_ens_43, n.da_price_ens_44
        ,n.da_price_ens_45, n.da_price_ens_46, n.da_price_ens_47, n.da_price_ens_48, n.da_price_ens_49
        ,n.da_price_ens_50

    FROM NORMALIZED n
    JOIN FORECAST_RANK r
        ON n.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND n.forecast_date = r.forecast_date
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending
