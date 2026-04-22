-- Tables most in need of (auto)vacuum, ranked by dead-tuple count and
-- dead-to-live ratio.
SELECT
    schemaname || '.' || relname              AS relation,
    n_live_tup,
    n_dead_tup,
    CASE WHEN n_live_tup = 0 THEN NULL
         ELSE round(100.0 * n_dead_tup / n_live_tup, 2)
    END                                       AS dead_pct,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    vacuum_count,
    autovacuum_count,
    analyze_count,
    autoanalyze_count
FROM pg_stat_user_tables
WHERE n_dead_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 50;
