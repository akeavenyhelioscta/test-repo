-- Per-schema size in the current database (tables + matviews + partitioned).
SELECT
    n.nspname AS schema,
    pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS total_size,
    sum(pg_total_relation_size(c.oid))                 AS total_size_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
GROUP BY n.nspname
ORDER BY total_size_bytes DESC;
