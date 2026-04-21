{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- materialized='ephemeral'
-------------------------------------------------------------

WITH NAV AS (
    SELECT * FROM {{ ref('staging_v5_nav_positions_2_product_lookup') }}
)

SELECT * FROM NAV
ORDER BY sftp_date desc, contract_yyyymm ASC
