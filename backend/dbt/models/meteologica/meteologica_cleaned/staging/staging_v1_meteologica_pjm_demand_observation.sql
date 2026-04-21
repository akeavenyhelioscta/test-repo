{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Demand Observation (5-min)
-- UNIONs 36 raw tables, produces UTC/timezone/local triplets for issue time and 5-min
-- observation time, ranks by issue time (earliest first).
-- Grain: 1 row per update_rank x observation_datetime_ending_local x region  (5-min intervals)
-- NOTE: 5-min `observation_datetime_ending_*` values preserve the prior naive timestamp
-- semantics (actually represent the start of each 5-min block) to avoid regressing downstream
-- hourly rollup logic in `meteologica_pjm_demand_observation_hourly`.
---------------------------

WITH UNIONED AS (

    SELECT
        'RTO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_power_demand_observation') }}

    UNION ALL

    SELECT
        'SOUTH' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_AE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_ae_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_BC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_bc_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_DPL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_DPL_DPLCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_dplco_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_DPL_EASTON' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_easton_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_JC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_jc_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_ME' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_me_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pe_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PEP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PEP_PEPCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_pepco_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PEP_SMECO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_smeco_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PL_PLCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_plco_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PL_UGI' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_ugi_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PN' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pn_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_PS' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_ps_power_demand_observation') }}

    UNION ALL

    SELECT
        'MIDATL_RECO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_reco_power_demand_observation') }}

    UNION ALL

    SELECT
        'SOUTH_DOM' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_dom_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_AEP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPAPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepapt_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPIMP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepimp_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPKPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepkpt_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPOPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepopt_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_AP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ap_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_ATSI' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_ATSI_OE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_oe_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_ATSI_PAPWR' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_papwr_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_CE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ce_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_DAY' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_day_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_DEOK' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_deok_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_DUQ' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_duq_power_demand_observation') }}

    UNION ALL

    SELECT
        'WEST_EKPC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,observation_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ekpc_power_demand_observation') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
-- 5-min exception: observation_datetime_ending_* keep the prior naive timestamp values
-- unchanged (so EXTRACT(HOUR) + 1 rollups in the hourly mart still produce hour_ending 1-24).
---------------------------

NORMALIZED AS (
    SELECT
        region
        ,issue_date::TIMESTAMP AS update_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS update_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS update_date

        ,forecast_period_start::TIMESTAMP AS observation_datetime_ending_utc
        ,(forecast_period_start::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS observation_datetime_ending_local
        ,(forecast_period_start::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS observation_date

        ,observation_mw::NUMERIC AS observation_mw
    FROM UNIONED
),

--------------------------------
-- Rank updates per (observation_date, region) by issue time (earliest first)
--------------------------------

UPDATE_RANK AS (
    SELECT
        observation_date
        ,region
        ,update_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY observation_date, region
            ORDER BY update_datetime_local ASC
        ) AS update_rank

    FROM (
        SELECT DISTINCT update_datetime_local, observation_date, region
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

        ,n.region
        ,n.observation_mw AS observation_load_mw

    FROM NORMALIZED n
    JOIN UPDATE_RANK r
        ON n.update_datetime_local = r.update_datetime_local
        AND n.observation_date = r.observation_date
        AND n.region = r.region
)

SELECT * FROM FINAL
ORDER BY observation_date DESC, update_datetime_local DESC, observation_datetime_ending_local, region
