-- Blocking pairs: each row shows a session being blocked and the session
-- blocking it. Start here when investigating lock contention.
SELECT
    blocked.pid                            AS blocked_pid,
    blocked.usename                        AS blocked_user,
    blocked.application_name               AS blocked_app,
    blocked.client_addr                    AS blocked_client,
    now() - blocked.query_start            AS blocked_duration,
    blocked.wait_event_type,
    blocked.wait_event,
    blocked.query                          AS blocked_query,
    blocking.pid                           AS blocking_pid,
    blocking.usename                       AS blocking_user,
    blocking.application_name              AS blocking_app,
    blocking.client_addr                   AS blocking_client,
    blocking.state                         AS blocking_state,
    now() - blocking.query_start           AS blocking_duration,
    blocking.query                         AS blocking_query
FROM pg_stat_activity AS blocked
JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS blocker(pid) ON true
JOIN pg_stat_activity AS blocking ON blocking.pid = blocker.pid
WHERE blocked.wait_event_type = 'Lock'
ORDER BY blocked_duration DESC;
