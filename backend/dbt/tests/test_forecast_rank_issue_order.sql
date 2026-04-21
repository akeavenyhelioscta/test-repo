WITH ALL_VINTAGES AS (
    SELECT
        'staging_v1_meteo_pjm_demand_fcst_ecmwf_ens_hourly' AS model_name,
        forecast_date,
        region::TEXT AS partition_dim_1,
        NULL::TEXT AS partition_dim_2,
        forecast_execution_datetime_local AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, region, forecast_execution_datetime_local, forecast_rank
        FROM {{ ref('staging_v1_meteo_pjm_demand_fcst_ecmwf_ens_hourly') }}
    ) d

    UNION ALL

    SELECT
        'staging_v1_meteologica_pjm_gen_forecast_hourly' AS model_name,
        forecast_date,
        source::TEXT AS partition_dim_1,
        region::TEXT AS partition_dim_2,
        forecast_execution_datetime_local AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, source, region, forecast_execution_datetime_local, forecast_rank
        FROM {{ ref('staging_v1_meteologica_pjm_gen_forecast_hourly') }}
    ) d

    UNION ALL

    SELECT
        'staging_v1_meteologica_pjm_da_price_forecast_hourly' AS model_name,
        forecast_date,
        hub::TEXT AS partition_dim_1,
        NULL::TEXT AS partition_dim_2,
        forecast_execution_datetime_local AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, hub, forecast_execution_datetime_local, forecast_rank
        FROM {{ ref('staging_v1_meteologica_pjm_da_price_forecast_hourly') }}
    ) d

    UNION ALL

    SELECT
        'staging_v1_pjm_load_forecast_hourly' AS model_name,
        forecast_date,
        NULL::TEXT AS partition_dim_1,
        NULL::TEXT AS partition_dim_2,
        forecast_execution_datetime_local AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, forecast_execution_datetime_local, forecast_rank
        FROM {{ ref('staging_v1_pjm_load_forecast_hourly') }}
    ) d

    UNION ALL

    SELECT
        'staging_v1_gridstatus_pjm_load_forecast_hourly' AS model_name,
        forecast_date,
        NULL::TEXT AS partition_dim_1,
        NULL::TEXT AS partition_dim_2,
        forecast_execution_datetime_local AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, forecast_execution_datetime_local, forecast_rank
        FROM {{ ref('staging_v1_gridstatus_pjm_load_forecast_hourly') }}
    ) d

    UNION ALL

    SELECT
        'staging_v1_gridstatus_pjm_solar_forecast_hourly' AS model_name,
        forecast_date,
        NULL::TEXT AS partition_dim_1,
        NULL::TEXT AS partition_dim_2,
        forecast_execution_datetime_local AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, forecast_execution_datetime_local, forecast_rank
        FROM {{ ref('staging_v1_gridstatus_pjm_solar_forecast_hourly') }}
    ) d

    UNION ALL

    SELECT
        'staging_v1_gridstatus_pjm_wind_forecast_hourly' AS model_name,
        forecast_date,
        NULL::TEXT AS partition_dim_1,
        NULL::TEXT AS partition_dim_2,
        forecast_execution_datetime_local AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, forecast_execution_datetime_local, forecast_rank
        FROM {{ ref('staging_v1_gridstatus_pjm_wind_forecast_hourly') }}
    ) d

    UNION ALL

    SELECT
        'staging_v1_pjm_outages_forecast_daily' AS model_name,
        forecast_date,
        NULL::TEXT AS partition_dim_1,
        NULL::TEXT AS partition_dim_2,
        forecast_execution_date::TIMESTAMP AS issue_timestamp,
        forecast_rank
    FROM (
        SELECT DISTINCT forecast_date, forecast_execution_date, forecast_rank
        FROM {{ ref('staging_v1_pjm_outages_forecast_daily') }}
    ) d
),

EXPECTED_RANKS AS (
    SELECT
        model_name,
        forecast_date,
        partition_dim_1,
        partition_dim_2,
        issue_timestamp,
        forecast_rank,

        DENSE_RANK() OVER (
            PARTITION BY model_name, forecast_date, partition_dim_1, partition_dim_2
            ORDER BY issue_timestamp ASC
        ) AS expected_rank,

        MIN(issue_timestamp) OVER (
            PARTITION BY model_name, forecast_date, partition_dim_1, partition_dim_2
        ) AS earliest_issue_timestamp
    FROM ALL_VINTAGES
),

RANK_MISMATCH AS (
    SELECT
        'rank_sequence_mismatch' AS check_name,
        model_name,
        forecast_date,
        partition_dim_1,
        partition_dim_2,
        issue_timestamp,
        forecast_rank,
        expected_rank
    FROM EXPECTED_RANKS
    WHERE forecast_rank <> expected_rank
),

RANK_ONE_NOT_EARLIEST AS (
    SELECT
        'rank_one_not_earliest' AS check_name,
        model_name,
        forecast_date,
        partition_dim_1,
        partition_dim_2,
        issue_timestamp,
        forecast_rank,
        expected_rank
    FROM EXPECTED_RANKS
    WHERE
        forecast_rank = 1
        AND issue_timestamp <> earliest_issue_timestamp
),

FAILURES AS (
    SELECT * FROM RANK_MISMATCH
    UNION ALL
    SELECT * FROM RANK_ONE_NOT_EARLIEST
)

SELECT * FROM FAILURES
