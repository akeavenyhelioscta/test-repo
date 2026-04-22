-- Sessions grouped by app / user / database / client. Use to identify which
-- client is responsible for the bulk of connections or for stale sessions.
SELECT
    datname,
    usename,
    application_name,
    client_addr,
    state,
    count(*) AS sessions,
    max(now() - state_change) AS oldest_in_state
FROM pg_stat_activity
WHERE backend_type = 'client backend'
GROUP BY datname, usename, application_name, client_addr, state
ORDER BY sessions DESC;
