-- Transaction ID age per database. If any approaches autovacuum_freeze_max_age
-- (default 200M) you risk a forced anti-wraparound vacuum.
SELECT
    datname,
    age(datfrozenxid) AS xid_age,
    pg_size_pretty(pg_database_size(datname)) AS size
FROM pg_database
WHERE datistemplate = false
ORDER BY age(datfrozenxid) DESC;
