{% macro create_meteologica_pjm_indexes() %}

    {#
        Creates indexes on Meteologica PJM raw source tables.
        Expression index on (issue_date::DATE) matches the cast used in staging views.

        Usage:  dbt run-operation create_meteologica_pjm_indexes
                (or called via: dbt run-operation create_source_indexes)
    #}

    {% set indexes = [

        {
            "table": "meteologica.usa_pjm_power_demand_forecast_hourly",
            "name": "idx_meteo_pjm_demand_rto_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_midatlantic_power_demand_forecast_hourly",
            "name": "idx_meteo_pjm_demand_midatl_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_south_power_demand_forecast_hourly",
            "name": "idx_meteo_pjm_demand_south_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_power_demand_forecast_hourly",
            "name": "idx_meteo_pjm_demand_west_issue_date",
            "columns": "issue_date"
        },

        {
            "table": "meteologica.usa_pjm_pv_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_solar_rto_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_midatlantic_pv_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_solar_midatl_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_south_pv_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_solar_south_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_pv_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_solar_west_issue_date",
            "columns": "issue_date"
        },

        {
            "table": "meteologica.usa_pjm_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_rto_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_midatlantic_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_midatl_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_south_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_south_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_west_issue_date",
            "columns": "issue_date"
        },

        {
            "table": "meteologica.usa_pjm_midatlantic_ae_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_midatl_ae_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_midatlantic_pl_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_midatl_pl_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_midatlantic_pn_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_midatl_pn_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_south_dom_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_south_dom_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_aep_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_west_aep_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_ap_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_west_ap_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_atsi_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_west_atsi_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_ce_wind_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_wind_west_ce_issue_date",
            "columns": "issue_date"
        },

        {
            "table": "meteologica.usa_pjm_hydro_power_generation_forecast_hourly",
            "name": "idx_meteo_pjm_hydro_rto_issue_date",
            "columns": "issue_date"
        },

        {
            "table": "meteologica.usa_pjm_da_power_price_system_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_system_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_aep_dayton_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_aep_dayton_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_aep_gen_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_aep_gen_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_atsi_gen_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_atsi_gen_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_chicago_gen_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_chicago_gen_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_chicago_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_chicago_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_dominion_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_dominion_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_eastern_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_eastern_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_new_jersey_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_new_jersey_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_n_illinois_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_n_illinois_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_ohio_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_ohio_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_western_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_western_issue_date",
            "columns": "issue_date"
        },
        {
            "table": "meteologica.usa_pjm_west_int_hub_da_power_price_forecast_hourly",
            "name": "idx_meteo_pjm_da_price_west_int_issue_date",
            "columns": "issue_date"
        },
    ] %}

    {% for idx in indexes %}

        {% set sql %}
            CREATE INDEX IF NOT EXISTS {{ idx.name }}
                ON {{ idx.table }} ({{ idx.columns }});
        {% endset %}

        {{ log("  Creating index: " ~ idx.name ~ " on " ~ idx.table, info=True) }}
        {% do run_query(sql) %}
        {{ log("    Done.", info=True) }}

    {% endfor %}

{% endmacro %}
