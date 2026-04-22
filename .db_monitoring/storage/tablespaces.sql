-- Tablespace usage. Azure Flexible Server typically only has pg_default
-- and pg_global, but useful as a sanity check.
SELECT
    spcname,
    pg_size_pretty(pg_tablespace_size(spcname)) AS size
FROM pg_tablespace
ORDER BY pg_tablespace_size(spcname) DESC;
