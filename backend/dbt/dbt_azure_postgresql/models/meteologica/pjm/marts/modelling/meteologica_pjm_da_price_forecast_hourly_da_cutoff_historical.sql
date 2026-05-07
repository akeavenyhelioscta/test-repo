{{
  config(
    materialized='incremental',
    unique_key=['as_of_date', 'forecast_date', 'hour_ending'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['as_of_date'], 'type': 'btree'},
      {'columns': ['forecast_date'], 'type': 'btree'},
      {'columns': ['as_of_date', 'forecast_date', 'hour_ending'], 'type': 'btree'}
    ]
  )
}}

---------------------------
-- Meteologica PJM Western-Hub DA Price Forecast — DA Cutoff, full captured history
-- Joins two independent Meteologica products at Western Hub:
--   * deterministic point forecast (content_id 4397)
--   * ECMWF ensemble (content_id 4400, 51 members + avg/bottom/top)
-- Each side is independently DA-cutoff-vintage-selected: for each as_of_date D,
-- the latest issue per source in (D 10:00 EPT - 48h, D 10:00 EPT] for each
-- (forecast_date x hour_ending) is kept. The two selected vintages are then
-- FULL OUTER JOINed so the mart emits a row whenever either source has data
-- for that (as_of_date, forecast_date, hour_ending).
--
-- Vintage rationale matches sibling load/solar/wind marts: Meteologica
-- forecasts only cover FUTURE hours from issue time, so the 48h lookback
-- window (vs simple "issued today before 10 AM") is needed to pull D-1's
-- late-evening issue for HE 1 of D.
--
-- Grain: 1 row per as_of_date x forecast_date x hour_ending (Western Hub —
-- single price node, no region dim).
--
-- Incremental: regular runs recompute as_of_dates in a 3-day rolling window
-- (max as_of_date already loaded, minus 2 days). Older buckets are immutable
-- once their 48h window closes. Use --full-refresh to rebuild from scratch.
---------------------------

{% set as_of_filter %}
    {% if is_incremental() %}
    AND forecast_execution_date >= (SELECT MAX(as_of_date) - INTERVAL '2 days' FROM {{ this }})::DATE
    {% endif %}
{% endset %}

-- ────── Deterministic point: source + DA-cutoff vintage selection ──────

WITH det_source AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,da_price_deterministic
    FROM {{ ref('staging_v1_meteo_pjm_da_price_fcst_hourly') }}
),

det_as_of_dates AS (
    SELECT DISTINCT forecast_execution_date AS as_of_date
    FROM det_source
    WHERE 1 = 1
    {{ as_of_filter }}
),

det_eligible AS (
    SELECT
        d.as_of_date
        ,f.forecast_execution_datetime_utc
        ,f.timezone
        ,f.forecast_execution_datetime_local
        ,f.forecast_execution_date
        ,f.forecast_date
        ,f.hour_ending
        ,f.da_price_deterministic
    FROM det_source f
    JOIN det_as_of_dates d
        ON f.forecast_execution_datetime_local <= (d.as_of_date + TIME '10:00:00')
       AND f.forecast_execution_datetime_local >  (d.as_of_date + TIME '10:00:00' - INTERVAL '48 hours')
    WHERE f.forecast_date >= d.as_of_date
),

det_ranked AS (
    SELECT
        e.*
        ,ROW_NUMBER() OVER (
            PARTITION BY e.as_of_date, e.forecast_date, e.hour_ending
            ORDER BY e.forecast_execution_datetime_local DESC
        ) AS rn
    FROM det_eligible e
),

det_pick AS (
    SELECT
        as_of_date
        ,forecast_execution_datetime_utc AS det_forecast_execution_datetime_utc
        ,timezone                        AS det_timezone
        ,forecast_execution_datetime_local AS det_forecast_execution_datetime_local
        ,forecast_execution_date         AS det_forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,da_price_deterministic
    FROM det_ranked
    WHERE rn = 1
),

-- ────── ENS: source + DA-cutoff vintage selection (independent of det) ──────

ens_source AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,da_price_ens_average
        ,da_price_ens_bottom
        ,da_price_ens_top
        ,da_price_ens_00, da_price_ens_01, da_price_ens_02, da_price_ens_03, da_price_ens_04
        ,da_price_ens_05, da_price_ens_06, da_price_ens_07, da_price_ens_08, da_price_ens_09
        ,da_price_ens_10, da_price_ens_11, da_price_ens_12, da_price_ens_13, da_price_ens_14
        ,da_price_ens_15, da_price_ens_16, da_price_ens_17, da_price_ens_18, da_price_ens_19
        ,da_price_ens_20, da_price_ens_21, da_price_ens_22, da_price_ens_23, da_price_ens_24
        ,da_price_ens_25, da_price_ens_26, da_price_ens_27, da_price_ens_28, da_price_ens_29
        ,da_price_ens_30, da_price_ens_31, da_price_ens_32, da_price_ens_33, da_price_ens_34
        ,da_price_ens_35, da_price_ens_36, da_price_ens_37, da_price_ens_38, da_price_ens_39
        ,da_price_ens_40, da_price_ens_41, da_price_ens_42, da_price_ens_43, da_price_ens_44
        ,da_price_ens_45, da_price_ens_46, da_price_ens_47, da_price_ens_48, da_price_ens_49
        ,da_price_ens_50
    FROM {{ ref('staging_v1_meteo_pjm_da_price_ens_fcst_hourly') }}
),

ens_as_of_dates AS (
    SELECT DISTINCT forecast_execution_date AS as_of_date
    FROM ens_source
    WHERE 1 = 1
    {{ as_of_filter }}
),

ens_eligible AS (
    SELECT
        d.as_of_date
        ,f.*
    FROM ens_source f
    JOIN ens_as_of_dates d
        ON f.forecast_execution_datetime_local <= (d.as_of_date + TIME '10:00:00')
       AND f.forecast_execution_datetime_local >  (d.as_of_date + TIME '10:00:00' - INTERVAL '48 hours')
    WHERE f.forecast_date >= d.as_of_date
),

ens_ranked AS (
    SELECT
        e.*
        ,ROW_NUMBER() OVER (
            PARTITION BY e.as_of_date, e.forecast_date, e.hour_ending
            ORDER BY e.forecast_execution_datetime_local DESC
        ) AS rn
    FROM ens_eligible e
),

ens_pick AS (
    SELECT
        as_of_date
        ,forecast_execution_datetime_utc AS ens_forecast_execution_datetime_utc
        ,timezone                        AS ens_timezone
        ,forecast_execution_datetime_local AS ens_forecast_execution_datetime_local
        ,forecast_execution_date         AS ens_forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,da_price_ens_average
        ,da_price_ens_bottom
        ,da_price_ens_top
        ,da_price_ens_00, da_price_ens_01, da_price_ens_02, da_price_ens_03, da_price_ens_04
        ,da_price_ens_05, da_price_ens_06, da_price_ens_07, da_price_ens_08, da_price_ens_09
        ,da_price_ens_10, da_price_ens_11, da_price_ens_12, da_price_ens_13, da_price_ens_14
        ,da_price_ens_15, da_price_ens_16, da_price_ens_17, da_price_ens_18, da_price_ens_19
        ,da_price_ens_20, da_price_ens_21, da_price_ens_22, da_price_ens_23, da_price_ens_24
        ,da_price_ens_25, da_price_ens_26, da_price_ens_27, da_price_ens_28, da_price_ens_29
        ,da_price_ens_30, da_price_ens_31, da_price_ens_32, da_price_ens_33, da_price_ens_34
        ,da_price_ens_35, da_price_ens_36, da_price_ens_37, da_price_ens_38, da_price_ens_39
        ,da_price_ens_40, da_price_ens_41, da_price_ens_42, da_price_ens_43, da_price_ens_44
        ,da_price_ens_45, da_price_ens_46, da_price_ens_47, da_price_ens_48, da_price_ens_49
        ,da_price_ens_50
    FROM ens_ranked
    WHERE rn = 1
),

-- ────── FULL OUTER JOIN deterministic and ENS picks on the row key ──────

joined AS (
    SELECT
        COALESCE(d.as_of_date, e.as_of_date) AS as_of_date
        ,COALESCE(d.forecast_date, e.forecast_date) AS forecast_date
        ,COALESCE(d.hour_ending, e.hour_ending) AS hour_ending

        ,d.det_forecast_execution_datetime_utc
        ,d.det_timezone
        ,d.det_forecast_execution_datetime_local
        ,d.det_forecast_execution_date

        ,e.ens_forecast_execution_datetime_utc
        ,e.ens_timezone
        ,e.ens_forecast_execution_datetime_local
        ,e.ens_forecast_execution_date

        ,d.da_price_deterministic

        ,e.da_price_ens_average
        ,e.da_price_ens_bottom
        ,e.da_price_ens_top
        ,e.da_price_ens_00, e.da_price_ens_01, e.da_price_ens_02, e.da_price_ens_03, e.da_price_ens_04
        ,e.da_price_ens_05, e.da_price_ens_06, e.da_price_ens_07, e.da_price_ens_08, e.da_price_ens_09
        ,e.da_price_ens_10, e.da_price_ens_11, e.da_price_ens_12, e.da_price_ens_13, e.da_price_ens_14
        ,e.da_price_ens_15, e.da_price_ens_16, e.da_price_ens_17, e.da_price_ens_18, e.da_price_ens_19
        ,e.da_price_ens_20, e.da_price_ens_21, e.da_price_ens_22, e.da_price_ens_23, e.da_price_ens_24
        ,e.da_price_ens_25, e.da_price_ens_26, e.da_price_ens_27, e.da_price_ens_28, e.da_price_ens_29
        ,e.da_price_ens_30, e.da_price_ens_31, e.da_price_ens_32, e.da_price_ens_33, e.da_price_ens_34
        ,e.da_price_ens_35, e.da_price_ens_36, e.da_price_ens_37, e.da_price_ens_38, e.da_price_ens_39
        ,e.da_price_ens_40, e.da_price_ens_41, e.da_price_ens_42, e.da_price_ens_43, e.da_price_ens_44
        ,e.da_price_ens_45, e.da_price_ens_46, e.da_price_ens_47, e.da_price_ens_48, e.da_price_ens_49
        ,e.da_price_ens_50

    FROM det_pick d
    FULL OUTER JOIN ens_pick e
        ON  d.as_of_date    = e.as_of_date
        AND d.forecast_date = e.forecast_date
        AND d.hour_ending   = e.hour_ending
),

final AS (
    SELECT
        as_of_date
        ,(forecast_date + INTERVAL '1 hour' * (hour_ending - 1)) AS forecast_datetime
        ,forecast_date
        ,hour_ending

        ,det_forecast_execution_datetime_utc
        ,det_timezone
        ,det_forecast_execution_datetime_local
        ,det_forecast_execution_date

        ,ens_forecast_execution_datetime_utc
        ,ens_timezone
        ,ens_forecast_execution_datetime_local
        ,ens_forecast_execution_date

        ,da_price_deterministic

        ,da_price_ens_average
        ,da_price_ens_bottom
        ,da_price_ens_top
        ,da_price_ens_00, da_price_ens_01, da_price_ens_02, da_price_ens_03, da_price_ens_04
        ,da_price_ens_05, da_price_ens_06, da_price_ens_07, da_price_ens_08, da_price_ens_09
        ,da_price_ens_10, da_price_ens_11, da_price_ens_12, da_price_ens_13, da_price_ens_14
        ,da_price_ens_15, da_price_ens_16, da_price_ens_17, da_price_ens_18, da_price_ens_19
        ,da_price_ens_20, da_price_ens_21, da_price_ens_22, da_price_ens_23, da_price_ens_24
        ,da_price_ens_25, da_price_ens_26, da_price_ens_27, da_price_ens_28, da_price_ens_29
        ,da_price_ens_30, da_price_ens_31, da_price_ens_32, da_price_ens_33, da_price_ens_34
        ,da_price_ens_35, da_price_ens_36, da_price_ens_37, da_price_ens_38, da_price_ens_39
        ,da_price_ens_40, da_price_ens_41, da_price_ens_42, da_price_ens_43, da_price_ens_44
        ,da_price_ens_45, da_price_ens_46, da_price_ens_47, da_price_ens_48, da_price_ens_49
        ,da_price_ens_50
    FROM joined
)

SELECT * FROM final
