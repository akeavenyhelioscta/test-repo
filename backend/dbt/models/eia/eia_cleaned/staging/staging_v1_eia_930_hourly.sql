{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- UTC TO EST CONVERSION
---------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v1_eia_930_fuel_type_hrl_gen') }}
),

EST_CONVERSION AS (
    SELECT
        (((date + (hour || ' hour')::INTERVAL)::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York' - '1 hour'::INTERVAL) AS datetime_est,
        ((((date + (hour || ' hour')::INTERVAL)::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York' - '1 hour'::INTERVAL))::DATE AS date_est,
        EXTRACT(HOUR FROM (((date + (hour || ' hour')::INTERVAL)::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York' - '1 hour'::INTERVAL)) AS hour_est,
        *
    FROM SOURCE
),

---------------------------
-- RESPONDENT NORMALIZATION AND HOURLY AGGREGATION
---------------------------

HOURLY_AGG AS (
    SELECT
        datetime_utc::TIMESTAMP AS datetime_utc,
        datetime_est::TIMESTAMP AS datetime,
        date_est::DATE AS date,
        (hour_est::INTEGER + 1)::INTEGER AS hour_ending,

        CASE
            WHEN respondent = 'ISNE' THEN 'ISONE'
            WHEN respondent = 'NYIS' THEN 'NYISO'
            WHEN respondent = 'ERCO' THEN 'ERCOT'
            WHEN respondent = 'CISO' THEN 'CAISO'
            ELSE respondent
        END::VARCHAR AS respondent,

        -- total
        ROUND(
              COALESCE(AVG(wind), 0)
            + COALESCE(AVG(solar), 0)
            + COALESCE(AVG(natural_gas), 0)
            + COALESCE(AVG(coal), 0)
            + COALESCE(AVG(petroleum), 0)
            + COALESCE(AVG(nuclear), 0)
            + COALESCE(AVG(hydro), 0)
            + COALESCE(AVG(pumped_storage), 0)
            + COALESCE(AVG(geothermal), 0)
            + COALESCE(AVG(battery_storage), 0)
            + COALESCE(AVG(solar_with_integrated_battery_storage), 0)
            + COALESCE(AVG(wind_with_integrated_battery_storage), 0)
            + COALESCE(AVG(other_energy_storage), 0)
            + COALESCE(AVG(unknown_energy_storage), 0)
            + COALESCE(AVG(other), 0)
            + COALESCE(AVG(unknown), 0)
        ) AS total,

        -- renewables
        ROUND(
              COALESCE(AVG(wind), 0)
            + COALESCE(AVG(solar), 0)
        ) AS renewables,

        -- thermal
        ROUND(
              COALESCE(AVG(natural_gas), 0)
            + COALESCE(AVG(coal), 0)
        ) AS thermal,

        ROUND(AVG(battery_storage)) AS battery,
        ROUND(AVG(coal)) AS coal,
        ROUND(AVG(geothermal)) AS geothermal,
        ROUND(AVG(hydro)) AS hydro,
        ROUND(AVG(natural_gas)) AS natural_gas,
        ROUND(AVG(nuclear)) AS nuclear,
        ROUND(AVG(other)) AS other,
        ROUND(AVG(other_energy_storage)) AS other_energy_storage,
        ROUND(AVG(petroleum)) AS oil,
        ROUND(AVG(pumped_storage)) AS pumped_storage,
        ROUND(AVG(solar)) AS solar,
        ROUND(AVG(solar_with_integrated_battery_storage)) AS solar_battery,
        ROUND(AVG(unknown)) AS unknown,
        ROUND(AVG(unknown_energy_storage)) AS unknown_energy_storage,
        ROUND(AVG(wind)) AS wind,
        ROUND(AVG(wind_with_integrated_battery_storage)) AS wind_battery

    FROM EST_CONVERSION
    GROUP BY datetime_est, date_est, hour_est, datetime_utc, respondent
),

---------------------------
-- JOIN RESPONDENT LOOKUP
---------------------------

FINAL AS (
    SELECT
        a.datetime_utc,
        a.datetime,
        a.date,
        a.hour_ending,

        b.is_iso,
        b.time_zone,
        b.region,
        a.respondent,
        b.balancing_authority_name,

        a.total,
        a.renewables,
        a.thermal,

        a.wind,
        a.solar,
        a.natural_gas,
        a.coal,
        a.oil,
        a.nuclear,
        a.hydro,
        a.pumped_storage,
        a.geothermal,
        a.battery,
        a.solar_battery,
        a.wind_battery,
        a.other_energy_storage,
        a.unknown_energy_storage,
        a.other,
        a.unknown

    FROM HOURLY_AGG a
    LEFT JOIN {{ ref('utils_v1_eia_respondent_lookup') }} b
        ON a.respondent = b.respondent
)

SELECT * FROM FINAL
ORDER BY date DESC, hour_ending DESC, respondent
