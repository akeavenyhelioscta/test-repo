-- Top 50 tables by total size (heap + indexes + toast).
SELECT
    n.nspname || '.' || c.relname                         AS relation,
    c.reltuples::bigint                                   AS approx_rows,
    pg_size_pretty(pg_total_relation_size(c.oid))         AS total,
    pg_size_pretty(pg_relation_size(c.oid))               AS heap,
    pg_size_pretty(pg_indexes_size(c.oid))                AS indexes,
    pg_size_pretty(
        pg_total_relation_size(c.oid)
        - pg_relation_size(c.oid)
        - pg_indexes_size(c.oid))                         AS toast,
    pg_total_relation_size(c.oid)                         AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY total_bytes DESC
LIMIT 50;
