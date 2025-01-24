SELECT
  resource_name AS name
  current_utilization AS oracle_resouce_current_utilization
  limit_value AS oracle_resource_limit
FROM v$resource_limit
