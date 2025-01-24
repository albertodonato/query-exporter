SELECT
  status,
  type,
  COUNT(*) AS oracle_sessions
FROM v$session
GROUP BY status, type
