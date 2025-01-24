SELECT
  name,
  value AS oracle_activity
FROM v$sysstat
WHERE name IN ('parse count (total)', 'execute count', 'user commits', 'user rollbacks')
