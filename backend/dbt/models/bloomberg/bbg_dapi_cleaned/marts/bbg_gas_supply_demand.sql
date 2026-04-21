{{
  config(
    materialized='view'
  )
}}

---------------------------
-- BLOOMBERG GAS SUPPLY & DEMAND MART
---------------------------

WITH FINAL AS (
    SELECT
        -- date
        date
        ,snapshot_at
        ,revision
        ,max_revision

        -- weather
        ,elec_cdd
        ,elec_hdd
        ,gas_cdd
        ,gas_hdd

        -- supply
        ,total_supply

        -- production
        ,production
        ,appalachia
        ,bakken
        ,gulf
        ,haynesville
        ,midcon
        ,mw_east
        ,mw_west
        ,north_east
        ,new_england
        ,pac_nw
        ,permian
        ,rockies
        ,southeast
        ,southwest

        -- canada imports
        ,cad_imports
        ,cad_to_north_east
        ,cad_to_new_england
        ,cad_to_midwest
        ,cad_to_michigan
        ,cad_to_bakken
        ,cad_to_pac_nw

        -- demand
        ,lower_48_demand
        ,power_burn
        ,industrial
        ,rescom
        ,plant_fuel
        ,pipe_loss

        -- mexico exports
        ,mexico_exports
        ,gulf_to_mexico
        ,sw_to_mexico
        ,cali_to_mexico

        -- lng
        ,lng
        ,sabine
        ,cameron
        ,corpus
        ,cove_point
        ,elba
        ,freeport
        ,calcasieu
        ,plaquemines

        -- total demand
        ,total_demand

        -- storage
        ,storage AS net_storage_change
        ,implied_storage AS net_implied_storage_change
        ,balancing_item

        -- salt
        ,salt

    FROM {{ ref('staging_v1_bbg_gas_supply_demand') }}
)

SELECT * FROM FINAL
ORDER BY date DESC, snapshot_at DESC
