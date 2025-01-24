SELECT
  n.wait_class AS wait_class,
  round(m.time_waited / m.INTSIZE_CSEC, 3) AS oracle_wait_time
FROM v$waitclassmetric AS m, v$system_wait_class AS n
WHERE m.wait_class_id = n.wait_class_id AND n.wait_class != 'Idle'
