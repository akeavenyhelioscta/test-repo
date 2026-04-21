{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Generation Observation
-- UNIONs 9 raw tables (solar, wind, hydro x regions), produces UTC/timezone/local triplets
-- for issue time and hour-ending observation time, ranks by issue time (earliest first).
-- Grain: 1 row per update_rank x observation_date x hour_ending x source x region
---------------------------

WITH UNIONED AS (

    SELECT
        'solar' AS source
        ,'RTO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_pv_power_generation_observation') }}

    UNION ALL

    SELECT
        'solar' AS source
        ,'MIDATL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pv_power_generation_observation') }}

    UNION ALL

    SELECT
        'solar' AS source
        ,'WEST' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_pv_power_generation_observation') }}

    UNION ALL

    SELECT
        'solar' AS source
        ,'SOUTH' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_pv_power_generation_observation') }}

    UNION ALL

    SELECT
        'wind' AS source
        ,'RTO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_wind_power_generation_observation') }}

    UNION ALL

    SELECT
        'wind' AS source
        ,'WEST' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_wind_power_generation_observation') }}

    UNION ALL

    SELECT
        'wind' AS source
        ,'MIDATL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_wind_power_generation_observation') }}

    UNION ALL

    SELECT
        'wind' AS source
        ,'SOUTH' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_wind_power_generation_observation') }}

    UNION ALL

    SELECT
        'hydro' AS source
        ,'RTO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_hydro_power_generation_observation') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
---------------------------

NORMALIZED AS (
    SELECT
        source
        ,region
        ,issue_date::TIMESTAMP AS update_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS update_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS update_date

        ,((forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS observation_datetime_ending_utc
        ,(forecast_period_start + INTERVAL '1 hour') AS observation_datetime_ending_local
        ,forecast_period_start::DATE AS observation_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,observation_mw::NUMERIC AS observation_mw
    FROM UNIONED
),

--------------------------------
-- Rank updates per (observation_date, source, region) by issue time (earliest first)
--------------------------------

UPDATE_RANK AS (
    SELECT
        observation_date
        ,source
        ,region
        ,update_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY observation_date, source, region
            ORDER BY update_datetime_local ASC
        ) AS update_rank

    FROM (
        SELECT DISTINCT update_datetime_local, observation_date, source, region
        FROM NORMALIZED
    ) sub
),

--------------------------------
--------------------------------

FINAL AS (
    SELECT
        r.update_rank

        ,n.update_datetime_utc
        ,n.timezone
        ,n.update_datetime_local
        ,n.update_date

        ,n.observation_datetime_ending_utc
        ,n.observation_datetime_ending_local
        ,n.observation_date
        ,n.hour_ending

        ,n.source
        ,n.region
        ,n.observation_mw AS observation_generation_mw

    FROM NORMALIZED n
    JOIN UPDATE_RANK r
        ON n.update_datetime_local = r.update_datetime_local
        AND n.observation_date = r.observation_date
        AND n.source = r.source
        AND n.region = r.region
)

SELECT * FROM FINAL
ORDER BY observation_date DESC, update_datetime_local DESC, hour_ending, source, region
