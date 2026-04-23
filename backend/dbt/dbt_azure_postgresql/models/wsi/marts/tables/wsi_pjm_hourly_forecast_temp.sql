{{
  config(
    materialized='incremental',
    unique_key=['forecast_execution_datetime_utc', 'forecast_datetime_ept', 'site_id'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['forecast_execution_datetime_utc', 'forecast_datetime_ept', 'site_id'], 'type': 'btree'},
      {'columns': ['forecast_datetime_ept', 'site_id'], 'type': 'btree'},
      {'columns': ['site_id', 'forecast_date_ept'], 'type': 'btree'}
    ]
  )
}}

---------------------------
-- WSI Hourly Forecast Temp — PJM cities only, FULL REVISION HISTORY
-- Grain: 1 row per forecast_execution_datetime_utc × forecast_datetime_ept × site_id
--
-- v5 source: scrape parses "Made <date> UTC" from the WSI CSV header and
-- includes that in the PK so each daily run preserves a fresh revision row
-- instead of overwriting. This mart is incremental on
-- forecast_execution_datetime_utc, so once it's a few days populated it
-- supports the "forecast evolution matrix" view.
--
-- TIMEZONE COLUMNS:
--   • *_local_prevailing / forecast_date / hour_ending — raw WSI per-station
--     local prevailing (EDT/EST for Eastern; CDT/CST for KORD/KMDW/KRFD).
--   • *_ept / forecast_date_ept / hour_ending_ept — same moment in EPT,
--     for joining to PJM LMPs / load.
--
-- For "current latest forecast" use the wsi_pjm_hourly_forecast_temp_latest
-- view, which keeps only the most recent revision per (forecast_datetime_ept,
-- site_id).
---------------------------

WITH STG AS (
    SELECT * FROM {{ ref('staging_v1_wsi_hourly_forecast_temp') }}
    WHERE region = 'PJM'
    {% if is_incremental() %}
      -- 2-day lookback covers any late-arriving revisions or reruns
      AND forecast_execution_datetime_utc >= (
            SELECT MAX(forecast_execution_datetime_utc) FROM {{ this }}
          ) - INTERVAL '2 days'
    {% endif %}
),

WITH_EPT AS (
    SELECT
        forecast_execution_datetime_utc,

        -- ── EPT (Eastern prevailing, DST-aware) ──
        (forecast_datetime_local
            AT TIME ZONE CASE WHEN site_id IN ('KORD','KMDW','KRFD') THEN 'America/Chicago' ELSE 'America/New_York' END
            AT TIME ZONE 'US/Eastern'
        )::TIMESTAMP AS forecast_datetime_ept,

        -- ── raw local (per-station local prevailing) ──
        forecast_datetime_local AS forecast_datetime_local_prevailing,
        forecast_date,
        hour_ending,

        region,
        site_id,
        station_name,
        temperature,
        temperature_diff,
        temperature_normal,
        dewpoint,
        cloud_cover_pct,
        feels_like_temperature,
        feels_like_temperature_diff,
        precipitation,
        wind_direction,
        wind_speed,
        ghi_irradiance,
        created_at,
        updated_at
    FROM STG
)

SELECT
    forecast_execution_datetime_utc,
    forecast_execution_datetime_utc::DATE             AS forecast_execution_date_utc,

    forecast_datetime_ept,
    forecast_datetime_ept::DATE                       AS forecast_date_ept,
    EXTRACT(HOUR FROM forecast_datetime_ept)::INT + 1 AS hour_ending_ept,

    forecast_datetime_local_prevailing,
    forecast_date,
    hour_ending,

    region,
    site_id,
    station_name,
    temperature,
    temperature_diff,
    temperature_normal,
    dewpoint,
    cloud_cover_pct,
    feels_like_temperature,
    feels_like_temperature_diff,
    precipitation,
    wind_direction,
    wind_speed,
    ghi_irradiance,
    created_at,
    updated_at
FROM WITH_EPT
ORDER BY forecast_execution_datetime_utc DESC, forecast_datetime_ept DESC, site_id
