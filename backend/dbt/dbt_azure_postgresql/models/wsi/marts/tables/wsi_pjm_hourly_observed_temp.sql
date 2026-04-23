{{
  config(
    materialized='incremental',
    unique_key=['observed_datetime_ept', 'site_id'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['observed_datetime_ept', 'site_id'], 'type': 'btree'},
      {'columns': ['site_id', 'date_ept'], 'type': 'btree'},
      {'columns': ['date', 'site_id'], 'type': 'btree'}
    ]
  )
}}

---------------------------
-- WSI Hourly Observed Weather — PJM cities only (incremental)
-- Grain: 1 row per observed_datetime_ept × site_id
--
-- Filtered to region = 'PJM' (34 PJM cities + the 'PJM' aggregate).
--
-- TIMEZONE COLUMNS (two parallel representations, pick what fits your join):
--   • *_local_standard / date / hour_ending  — raw WSI values in each station's
--     local STANDARD timezone, year-round (EST for Eastern cities, CST for
--     KORD/KMDW/KRFD). Use for traceability back to the source upsert.
--   • *_ept / date_ept / hour_ending_ept     — same moment expressed in EPT
--     (Eastern prevailing, DST-aware). Use for joins to PJM LMPs / load data.
--
-- Central PJM cities (KORD, KMDW, KRFD) are converted from CST→EPT;
-- everything else from EST→EPT.
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_wsi_hourly_observed_temp') }}
    WHERE region = 'PJM'
    {% if is_incremental() %}
      AND date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '7 days'
    {% endif %}
),

WITH_EPT AS (
    SELECT
        -- ── EPT (absolute moment, Eastern prevailing, DST-aware) ──
        (observed_datetime_local_standard
            AT TIME ZONE CASE WHEN site_id IN ('KORD','KMDW','KRFD') THEN 'CST' ELSE 'EST' END
            AT TIME ZONE 'US/Eastern'
        )::TIMESTAMP AS observed_datetime_ept,

        -- ── raw local (per-station local STANDARD, year-round) ──
        observed_datetime_local_standard,
        date,
        hour_starting,
        hour_ending,

        region,
        site_id,
        station_name,
        temperature,
        dewpoint,
        cloud_cover_pct,
        wind_direction,
        wind_speed,
        heat_index,
        wind_chill,
        relative_humidity,
        precipitation
    FROM FINAL
)

SELECT
    observed_datetime_ept,
    observed_datetime_ept::DATE                       AS date_ept,
    EXTRACT(HOUR FROM observed_datetime_ept)::INT + 1 AS hour_ending_ept,

    observed_datetime_local_standard,
    date,
    hour_starting,
    hour_ending,

    region,
    site_id,
    station_name,
    temperature,
    dewpoint,
    cloud_cover_pct,
    wind_direction,
    wind_speed,
    heat_index,
    wind_chill,
    relative_humidity,
    precipitation
FROM WITH_EPT
ORDER BY observed_datetime_ept DESC, site_id
