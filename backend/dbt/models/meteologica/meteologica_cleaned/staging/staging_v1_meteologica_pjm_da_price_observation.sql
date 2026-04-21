{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Day-Ahead Price Observation
-- UNIONs 13 raw tables (system + 12 hubs), produces UTC/timezone/local triplets for issue
-- time and hour-ending observation time, ranks by issue time (earliest first).
-- Grain: 1 row per update_rank x observation_date x hour_ending x hub
---------------------------

WITH UNIONED AS (

    SELECT
        'SYSTEM' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_da_power_price_system_observation') }}

    UNION ALL

    SELECT
        'AEP DAYTON' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_aep_dayton_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'AEP GEN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_aep_gen_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'ATSI GEN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_atsi_gen_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'CHICAGO GEN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_chicago_gen_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'CHICAGO' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_chicago_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'DOMINION' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_dominion_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'EASTERN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_eastern_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'NEW JERSEY' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_new_jersey_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'N ILLINOIS' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_n_illinois_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'OHIO' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_ohio_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'WESTERN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_western_hub_da_power_price_observation') }}

    UNION ALL

    SELECT
        'WEST INT' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,dayahead
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_int_hub_da_power_price_observation') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
---------------------------

NORMALIZED AS (
    SELECT
        hub
        ,issue_date::TIMESTAMP AS update_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS update_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS update_date

        ,((forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS observation_datetime_ending_utc
        ,(forecast_period_start + INTERVAL '1 hour') AS observation_datetime_ending_local
        ,forecast_period_start::DATE AS observation_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,dayahead::NUMERIC AS observation_da_price
    FROM UNIONED
),

--------------------------------
-- Rank updates per (observation_date, hub) by issue time (earliest first)
--------------------------------

UPDATE_RANK AS (
    SELECT
        observation_date
        ,hub
        ,update_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY observation_date, hub
            ORDER BY update_datetime_local ASC
        ) AS update_rank

    FROM (
        SELECT DISTINCT update_datetime_local, observation_date, hub
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

        ,n.hub
        ,n.observation_da_price

    FROM NORMALIZED n
    JOIN UPDATE_RANK r
        ON n.update_datetime_local = r.update_datetime_local
        AND n.observation_date = r.observation_date
        AND n.hub = r.hub
)

SELECT * FROM FINAL
ORDER BY observation_date DESC, update_datetime_local DESC, hour_ending, hub
