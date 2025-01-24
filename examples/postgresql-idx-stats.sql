SELECT
  relname,
  indexrelname,
  SUM(idx_scan) AS pg_idx_scan,
  SUM(idx_tup_read) AS pg_idx_tup_read,
  SUM(idx_tup_fetch) AS pg_idx_tup_fetch
FROM pg_stat_user_indexes
GROUP BY relname, indexrelname
