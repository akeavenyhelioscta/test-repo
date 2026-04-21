{{
  config(
    materialized='incremental',
    unique_key='sftp_date',
    incremental_strategy='delete+insert'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH NAV AS (
    SELECT * FROM {{ ref('source_v5_marex_positions') }}
    {% if is_incremental() %}
    WHERE sftp_date >= (SELECT MAX(sftp_date) - INTERVAL '14 days' FROM {{ this }})
    {% endif %}
)

SELECT * FROM NAV
ORDER BY sftp_date desc, contract_yyyymm ASC
