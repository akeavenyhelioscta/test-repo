-- -- macros/slack_notifications/setup_triggers_for_unique_upserts.sql

{% macro setup_postgres_triggers() %}

    -- NOTE: pjm
    {{ pjm_da_hrl_lmps() }}

{% endmacro %}