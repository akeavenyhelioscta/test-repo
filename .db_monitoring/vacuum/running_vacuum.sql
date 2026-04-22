-- Long-running (auto)vacuum / analyze workers currently executing.
SELECT
    pid,
    datname,
    usename,
    state,
    now() - query_start AS duration,
    wait_event_type,
    wait_event,
    query
FROM pg_stat_activity
WHERE query ILIKE 'autovacuum:%'
   OR query ILIKE 'VACUUM%'
   OR query ILIKE 'ANALYZE%'
ORDER BY duration DESC;
