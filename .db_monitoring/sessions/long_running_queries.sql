-- Active queries running longer than the threshold below. Adjust to taste.
--
-- To act on a stuck query:
--   soft cancel:   SELECT pg_cancel_backend(<pid>);
--   hard kill:     SELECT pg_terminate_backend(<pid>);
WITH params AS (SELECT interval '30 seconds' AS threshold)
SELECT
    pid,
    now() - query_start    AS duration,
    state,
    wait_event_type,
    wait_event,
    usename,
    application_name,
    client_addr,
    datname,
    query
FROM pg_stat_activity, params
WHERE state != 'idle'
  AND backend_type = 'client backend'
  AND query_start IS NOT NULL
  AND now() - query_start > params.threshold
ORDER BY duration DESC;
