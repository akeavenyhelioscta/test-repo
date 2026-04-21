{{
  config(
    materialized='view'
  )
}}

-------------------------------------------------------------
-- Hourly rollup of 5-min demand observations.
-- Derives hour_ending from the (start-valued) 5-min observation_datetime_ending_local,
-- then reconstructs the true hour-ending triplet from (observation_date, hour_ending).
-------------------------------------------------------------

WITH HOURED AS (
    SELECT
        update_rank
        ,update_datetime_utc
        ,timezone
        ,update_datetime_local
        ,update_date
        ,observation_date
        ,EXTRACT(HOUR FROM observation_datetime_ending_local)::INT + 1 AS hour_ending
        ,region
        ,observation_load_mw
    FROM {{ ref('meteologica_pjm_demand_observation_5min') }}
)

SELECT
    update_rank
    ,update_datetime_utc
    ,timezone
    ,update_datetime_local
    ,update_date

    ,((observation_date::TIMESTAMP + INTERVAL '1 hour' * hour_ending)
        AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS observation_datetime_ending_utc
    ,(observation_date::TIMESTAMP + INTERVAL '1 hour' * hour_ending) AS observation_datetime_ending_local
    ,observation_date
    ,hour_ending

    ,region
    ,ROUND(AVG(observation_load_mw), 0) AS observation_load_mw

FROM HOURED

GROUP BY
    update_rank
    ,update_datetime_utc
    ,timezone
    ,update_datetime_local
    ,update_date
    ,observation_date
    ,hour_ending
    ,region

ORDER BY observation_date DESC, update_datetime_local DESC, hour_ending, region
