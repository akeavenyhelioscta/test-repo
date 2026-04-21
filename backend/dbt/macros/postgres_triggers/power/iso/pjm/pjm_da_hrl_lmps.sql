{% macro pjm_da_hrl_lmps() %}

-- Create the trigger function for pjm.da_hrl_lmps
-- Only fires when a complete 24-hour WESTERN HUB day exists for tomorrow
-- AND all rows are fresh inserts (created_at = updated_at), not re-upserts
CREATE OR REPLACE FUNCTION pjm.trigger_function_da_hrl_lmps()
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT
            datetime_beginning_ept::date as da_date,
            type,
            COUNT(*) as row_count,
            COUNT(DISTINCT pnode_name) as pnode_count,
            COUNT(DISTINCT datetime_beginning_ept) as hour_count,
            MIN(datetime_beginning_ept) as min_datetime,
            MAX(datetime_beginning_ept) as max_datetime
        FROM pjm.da_hrl_lmps
        WHERE
            datetime_beginning_ept::date = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::date + INTERVAL '1 day'
            AND pnode_name = 'WESTERN HUB'
            AND created_at = updated_at
        GROUP BY
            datetime_beginning_ept::date, type
        HAVING
            COUNT(*) = 24
    LOOP
        payload := json_build_object(
            'table', TG_TABLE_NAME,
            'operation', TG_OP,
            'da_date', rec.da_date,
            'type', rec.type,
            'row_count', rec.row_count,
            'pnode_count', rec.pnode_count,
            'hour_count', rec.hour_count,
            'min_datetime', rec.min_datetime,
            'max_datetime', rec.max_datetime
        );
        PERFORM pg_notify('notifications_pjm_da_hrl_lmps', payload::text);
    END LOOP;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS trigger_pjm_da_hrl_lmps ON pjm.da_hrl_lmps;

-- Statement-level trigger with transition table to capture inserted rows
CREATE TRIGGER trigger_pjm_da_hrl_lmps
AFTER INSERT ON pjm.da_hrl_lmps
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION pjm.trigger_function_da_hrl_lmps();

{% endmacro %}
