{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH TEMP_FORECASTS AS (
    select

        avg_temp.rank_forecast_execution_timestamps
        ,avg_temp.labelled_forecast_execution_timestamp

        ,avg_temp.forecast_execution_datetime
        ,avg_temp.forecast_execution_date

        ,avg_temp.forecast_date
        ,avg_temp.count_forecast_days
        ,avg_temp.max_forecast_days

        ,avg_temp.region
        ,avg_temp.site_id
        ,avg_temp.station_name

        -- temps
        ,min_max.min_temp
        ,min_max.max_temp
        ,avg_temp.avg_temp
        ,hdd_cdd.cdd
        ,hdd_cdd.hdd

    FROM {{ ref('source_v1_wsi_homepage_forecast_table_avg') }} avg_temp

    LEFT JOIN {{ ref('source_v1_wsi_homepage_forecast_table_hddcdd') }} hdd_cdd
        ON avg_temp.labelled_forecast_execution_timestamp = hdd_cdd.labelled_forecast_execution_timestamp
        AND avg_temp.forecast_date = hdd_cdd.forecast_date
        AND avg_temp.site_id = hdd_cdd.site_id

    LEFT JOIN {{ ref('source_v1_wsi_homepage_forecast_table_minmax') }} min_max
        ON avg_temp.labelled_forecast_execution_timestamp = min_max.labelled_forecast_execution_timestamp
        AND avg_temp.forecast_date = min_max.forecast_date
        AND avg_temp.site_id = min_max.site_id

    WHERE
        avg_temp.labelled_forecast_execution_timestamp = 'Current Forecast'
        AND hdd_cdd.labelled_forecast_execution_timestamp = 'Current Forecast'
        AND min_max.labelled_forecast_execution_timestamp = 'Current Forecast'
)

SELECT * FROM TEMP_FORECASTS
ORDER BY forecast_execution_datetime desc, forecast_date asc, region asc, site_id asc, station_name asc
