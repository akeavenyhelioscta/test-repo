{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Demand Forecast (Hourly)
-- UNIONs 36 raw tables (RTO + 3 macro regions + 32 sub-regions), produces UTC/timezone/local
-- triplets for issue time and hour-ending target time, ranks by issue time (earliest first).
-- Grain: 1 row per forecast_rank × forecast_date × hour_ending × region
---------------------------

WITH UNIONED AS (

    ---------------------------
    -- RTO + 3 macro regions
    ---------------------------

    SELECT
        'RTO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'SOUTH' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_power_demand_forecast_hourly') }}

    UNION ALL

    ---------------------------
    -- Mid-Atlantic sub-regions (17)
    ---------------------------

    SELECT
        'MIDATL_AE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_ae_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_BC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_bc_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_DPL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_DPL_DPLCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_dplco_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_DPL_EASTON' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_easton_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_JC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_jc_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_ME' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_me_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pe_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PEP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PEP_PEPCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_pepco_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PEP_SMECO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_smeco_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PL_PLCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_plco_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PL_UGI' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_ugi_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PN' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pn_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PS' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_ps_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_RECO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_reco_power_demand_forecast_hourly') }}

    UNION ALL

    ---------------------------
    -- South sub-regions (1)
    ---------------------------

    SELECT
        'SOUTH_DOM' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_dom_power_demand_forecast_hourly') }}

    UNION ALL

    ---------------------------
    -- West sub-regions (14)
    ---------------------------

    SELECT
        'WEST_AEP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPAPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepapt_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPIMP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepimp_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPKPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepkpt_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPOPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepopt_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_AP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ap_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_ATSI' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_ATSI_OE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_oe_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_ATSI_PAPWR' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_papwr_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_CE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ce_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_DAY' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_day_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_DEOK' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_deok_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_DUQ' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_duq_power_demand_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST_EKPC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ekpc_power_demand_forecast_hourly') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
---------------------------

NORMALIZED AS (
    SELECT
        region
        ,issue_date::TIMESTAMP AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS forecast_execution_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS forecast_execution_date

        ,((forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS forecast_datetime_ending_utc
        ,(forecast_period_start + INTERVAL '1 hour') AS forecast_datetime_ending_local
        ,forecast_period_start::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,forecast_mw::NUMERIC AS forecast_mw
    FROM UNIONED
),

--------------------------------
-- Rank forecasts per (forecast_date, region) by issue time (earliest first)
-- Partitioned by region because Meteologica regions come from
-- separate API endpoints with potentially different issue_dates
--------------------------------

FORECAST_RANK AS (
    SELECT
        forecast_date
        ,region
        ,forecast_execution_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date, region
            ORDER BY forecast_execution_datetime_local ASC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date, region
        FROM NORMALIZED
    ) sub
),

--------------------------------
--------------------------------

FINAL AS (
    SELECT
        r.forecast_rank

        ,n.forecast_execution_datetime_utc
        ,n.timezone
        ,n.forecast_execution_datetime_local
        ,n.forecast_execution_date

        ,n.forecast_datetime_ending_utc
        ,n.forecast_datetime_ending_local
        ,n.forecast_date
        ,n.hour_ending

        ,n.region
        ,n.forecast_mw AS forecast_load_mw

    FROM NORMALIZED n
    JOIN FORECAST_RANK r
        ON n.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND n.forecast_date = r.forecast_date
        AND n.region = r.region
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending, region


