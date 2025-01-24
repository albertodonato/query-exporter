SELECT
  relname,
  indexrelname,
  SUM(idx_blks_read) AS pg_idx_io_blks_read,
  sum(idx_blks_hit) AS pg_idx_io_blks_hit
FROM pg_statio_user_indexes
GROUP BY relname, indexrelname
