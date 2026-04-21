{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RAW BLOOMBERG DAPI HISTORICAL
---------------------------

WITH RAW AS (
    SELECT
        security::VARCHAR AS security,
        date::DATE AS date,
        snapshot_at::TIMESTAMP AS snapshot_at,
        data_type::VARCHAR AS data_type,
        value::NUMERIC AS value,
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at
    FROM {{ source('bbg_dapi_v1', 'bbg_historical') }}
),

FINAL AS (
    SELECT * FROM RAW
)

SELECT * FROM FINAL
