SELECT
  relname,
  SUM(heap_blks_read) AS pg_table_io_heap_blks_read,
  SUM(heap_blks_hit) AS pg_table_io_heap_blks_hit,
  SUM(idx_blks_read) AS pg_table_io_idx_blks_read,
  SUM(idx_blks_hit) AS pg_table_io_idx_blks_hit,
  SUM(toast_blks_read) AS pg_table_io_toast_blks_read,
  SUM(toast_blks_hit) AS pg_table_io_toast_blks_hit,
  SUM(tidx_blks_read) AS pg_table_io_tidx_blks_read,
  SUM(tidx_blks_hit) AS pg_table_io_tidx_blks_hit
FROM pg_statio_user_tables
GROUP BY relname
