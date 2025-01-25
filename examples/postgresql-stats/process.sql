SELECT
  state,
  COUNT(*) AS pg_process
FROM pg_stat_activity
WHERE state IS NOT NULL
GROUP BY state
