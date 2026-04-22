-- All non-idle sessions with full client/query detail. Use to see exactly
-- what work is in flight right now.
SELECT
    pid,
    now() - query_start AS duration,
    client_addr,
    client_hostname,
    client_port,
    application_name,
    usename,
    datname,
    state,
    query
FROM pg_stat_activity
WHERE state != 'idle'
  AND query NOT ILIKE '%pg_stat_activity%'
ORDER BY duration DESC NULLS LAST;
