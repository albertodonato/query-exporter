"""Loop to execute database queries and collect metrics."""

import asyncio
from collections import defaultdict
from collections.abc import Iterator, Mapping
from datetime import datetime
from itertools import chain
import time
import typing as t

from croniter import croniter
from dateutil.tz import gettz
from prometheus_aioexporter import (
    MetricConfig,
    MetricsRegistry,
)
from prometheus_client import Counter
from prometheus_client.metrics import MetricWrapperBase
import structlog
from toolrack.aio import (
    PeriodicCall,
    TimedCall,
)

from .config import (
    DATABASE_LABEL,
    Config,
)
from .db import (
    DataBase,
    DataBaseConnectError,
    DataBaseError,
    MetricResult,
    Query,
    QueryExecution,
    QueryTimeoutExpired,
)
from .metrics import (
    DB_ERRORS_METRIC_NAME,
    QUERIES_METRIC_NAME,
    QUERY_LATENCY_METRIC_NAME,
    QUERY_TIMESTAMP_METRIC_NAME,
)

from .alert_manager import AlertManager, AlertGenerator


class InvalidMetricValue(Exception):
    """Raised when a query result is an invalid value for the metric."""


class MetricsLastSeen:
    """Track last seen times for metrics.

    It assumes labels are sorted by name in metrics.

    """

    def __init__(self, expirations: dict[str, int | None]):
        self._expirations = expirations
        self._last_seen: dict[str, dict[tuple[str, ...], float]] = defaultdict(
            dict
        )

    def update(
        self,
        name: str,
        labels: dict[str, str],
        timestamp: float,
    ) -> None:
        """Update last seen for a metric with a set of labels to given timestamp."""
        if not self._expirations.get(name):
            return

        # sort by label name
        label_values = tuple(value for _, value in sorted(labels.items()))
        self._last_seen[name][label_values] = timestamp

    def expire_series(
        self, timestamp: float
    ) -> dict[str, list[tuple[str, ...]]]:
        """Expire and return expired metric series at the given timestamp.

        Expired series are removed internally.

        Return a dict mapping metric names to a list of tuples of sorted label
        values for expired series.

        """
        expired = {}
        for name, metric_last_seen in self._last_seen.items():
            expiration = t.cast(int, self._expirations[name])
            expired_labels = [
                label_values
                for label_values, last_seen in metric_last_seen.items()
                if timestamp > last_seen + expiration
            ]
            if expired_labels:
                expired[name] = expired_labels

        # clear expired series from tracking
        for name, series_labels in expired.items():
            for label_values in series_labels:
                del self._last_seen[name][label_values]
                if not self._last_seen[name]:
                    del self._last_seen[name]
        return expired


class QueryExecutor:
    """Run database queries and collect metrics."""

    def __init__(
        self,
        config: Config,
        registry: MetricsRegistry,
        logger: structlog.stdlib.BoundLogger | None = None,
    ):
        self._config = config
        self._registry = registry
        self._logger = logger or structlog.get_logger()
        self._timed_query_executions: list[QueryExecution] = []
        self._aperiodic_query_executions: list[QueryExecution] = []
        # map query names to their TimedCalls
        self._timed_calls: dict[str, TimedCall] = {}
        # map query names to list of database names
        self._doomed_queries: dict[str, set[str]] = defaultdict(set)
        self._loop = asyncio.get_event_loop()
        self._last_seen = MetricsLastSeen(
            {
                name: metric.config.get("expiration")
                for name, metric in self._config.metrics.items()
            }
        )
        self._databases: dict[str, DataBase] = {
            db_config.name: DataBase(db_config, logger=self._logger)
            for db_config in self._config.databases.values()
        }
        # 新增：初始化告警管理器
        self._alert_manager = AlertManager(
            config.alertmanager.url if config.alertmanager else "",
            logger=self._logger
        )
        self._alert_generator = AlertGenerator(
            self._alert_manager,
            {name: alert.model_dump() for name, alert in config.alerts.items()},
            logger=self._logger
        )

        for query_execution in chain(
            *(query.executions for query in self._config.queries.values())
        ):
            if query_execution.query.timed:
                self._timed_query_executions.append(query_execution)
            else:
                self._aperiodic_query_executions.append(query_execution)

    async def start(self) -> None:
        """Start timed queries execution."""
        await self._alert_manager.start()  # 新增
        for query_execution in self._timed_query_executions:
            query = query_execution.query
            call: TimedCall
            if query.interval:
                print(f"[Executor] query interval: {query_execution}")
                call = PeriodicCall(self._run_query, query_execution)
                call.start(query.interval, now=True)
            elif query.schedule is not None:
                print(f"[Executor] query schedule: {query_execution}")
                call = TimedCall(self._run_query, query_execution)
                call.start(self._loop_times_iter(query.schedule))
            self._timed_calls[query_execution.name] = call

    async def stop(self) -> None:
        """Stop timed query execution."""
        print(f"[Executor] stop timed query execution")
        coros = (call.stop() for call in self._timed_calls.values())
        await asyncio.gather(*coros, return_exceptions=True)
        self._timed_calls.clear()
        coros = (db.close() for db in self._databases.values())
        await asyncio.gather(*coros, return_exceptions=True)
        await self._alert_manager.stop()  # 新增

    def clear_expired_series(self) -> None:
        """Clear metric series that have expired."""
        expired_series = self._last_seen.expire_series(self._loop.time())
        for name, label_values in expired_series.items():
            metric = self._registry.get_metric(name)
            for values in label_values:
                metric.remove(*values)

    async def run_aperiodic_queries(self) -> None:
        """Run queries on request."""
        coros = (
            self._execute_query(query_execution, dbname)
            for query_execution in self._aperiodic_query_executions
            for dbname in query_execution.query.databases
        )
        await asyncio.gather(*coros, return_exceptions=True)

    def _loop_times_iter(self, schedule: str) -> Iterator[float | int]:
        """Wrap a croniter iterator to sync time with the loop clock."""
        cron_iter = croniter(schedule, datetime.now(gettz()))
        while True:
            cc = next(cron_iter)
            t = time.time()
            delta = cc - t
            yield self._loop.time() + delta

    def _run_query(self, query_execution: QueryExecution) -> None:
        """Periodic task to run a query."""
        print(f"[Executor] _run_query: {query_execution}")
        for dbname in query_execution.query.databases:
            self._loop.create_task(
                self._execute_query(query_execution, dbname)
            )

    async def _execute_query(
        self, query_execution: QueryExecution, dbname: str
    ) -> None:
        """'Execute a Query on a DataBase."""
        if await self._remove_if_dooomed(query_execution, dbname):
            return

        db = self._databases[dbname]
        print(f"[Executor] _execute_query db: {db}")
        query = query_execution.query
        print(f"[Executor] _execute_query: {query}")
        try:
            metric_results = await db.execute(query_execution)
            if metric_results.latency:
                self._update_query_latency_metric(
                    db, query, metric_results.latency
                )
            if metric_results.timestamp:
                self._update_query_timestamp_metric(
                    db, query, metric_results.timestamp
                )
            self._update_metrics_from_results(
                db, query_execution.name, metric_results.results
            )
            # 新增：处理告警
            if query_execution.query.alerts and metric_results.results:
                await self._process_alerts(
                    query_execution, db, metric_results.results
                )
            self._increment_queries_count(db, query, "success")
        except InvalidMetricValue:
            self._increment_queries_count(db, query, "invalid-value")
        except DataBaseConnectError:
            self._increment_db_error_count(db)
        except QueryTimeoutExpired:
            self._increment_queries_count(db, query, "timeout")
        except DataBaseError as error:
            self._increment_queries_count(db, query, "error")
            if error.fatal:
                self._logger.debug(
                    "removing failed query",
                    query=query_execution.name,
                    database=dbname,
                )
                self._doomed_queries[query_execution.name].add(dbname)
    
    async def _process_alerts(
        self,
        query_execution: QueryExecution,
        database: DataBase,
        results: list[MetricResult],
    ) -> None:
        """Process query results and generate alerts."""
        try:
            # 过滤出 alerts 相关的结果
            alert_results = []
            for result in results:
                # 检查结果是否对应某个告警
                for alert in query_execution.query.alerts:
                    if result.metric == alert.name:
                        alert_results.append(result)
                        break
            print(f"[Executor] _process_alerts alert_results: {alert_results}")
            if not alert_results:
                return
                
            # 将 MetricResult 转换为字典格式
            result_dicts = []
            for result in alert_results:
                result_dict = {
                    'value': result.value,
                    'metric': result.metric  # 保留 metric 名称
                }
                # 添加 labels
                result_dict.update(result.labels)
                result_dicts.append(result_dict)

            # 从 QueryAlert 对象中提取告警名称
            alert_names = [alert.name for alert in query_execution.query.alerts]
            
            # 生成告警
            self._logger.info(
                    "[Executor] _process_alerts before generate_alerts_from_results",
                    query_execution=query_execution,
                    name=query_execution.name,
                    alert_names=alert_names,
                    result_dicts=result_dicts
                )
            alerts = self._alert_generator.generate_alerts_from_results(
                query_execution.name,
                alert_names,
                result_dicts,
                database.config.labels
            )

            # 发送告警
            if alerts:
                self._logger.info(
                    "Processing alerts for query",
                    query=query_execution.name,
                    alert_count=len(alerts),
                    database=database.config.name
                )
                success = await self._alert_manager.send_alerts(alerts)
                if success:
                    self._logger.info(
                        "Alerts sent successfully to AlertManager",
                        query=query_execution.name,
                        count=len(alerts),
                        database=database.config.name
                    )
                else:
                    self._logger.error(
                        "Failed to send alerts to AlertManager",
                        query=query_execution.name,
                        count=len(alerts),
                        database=database.config.name
                    )

        except Exception as e:
            self._logger.error(
                "Failed to process alerts",
                query=query_execution.name,
                error=str(e)
            )

    async def _remove_if_dooomed(
        self, query_execution: QueryExecution, dbname: str
    ) -> bool:
        """Remove a query execution if it will never work.

        Return whether the query has been removed for the database.

        """
        if dbname not in self._doomed_queries[query_execution.name]:
            return False

        query = query_execution.query

        if set(query.databases) == self._doomed_queries[query_execution.name]:
            # the query has failed on all databases
            if query.timed:
                self._timed_query_executions.remove(query_execution)
                call = self._timed_calls.pop(query_execution.name, None)
                if call is not None:
                    await call.stop()
            else:
                self._aperiodic_query_executions.remove(query_execution)
        return True

    def _update_metrics_from_results(
        self,
        database: DataBase,
        query_execution_name: str,
        results: list[MetricResult],
    ) -> None:
        has_invalid = False
        for result in results:
            try:
                # 只更新在 metrics 配置中定义的结果
                if result.metric not in self._config.metrics:
                    continue
                self._update_metric(
                    database, result.metric, result.value, labels=result.labels
                )
            except ValueError as e:
                self._logger.debug(
                    "invalid metric result",
                    error=str(e),
                    query=query_execution_name,
                    database=database.config.name,
                )
                has_invalid = True

        # only raise the error after processing all values
        if has_invalid:
            raise InvalidMetricValue()

    def _update_metric(
        self,
        database: DataBase,
        name: str,
        value: t.Any,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        """Update value for a metric."""
        if value is None:
            # count queries might return NULL, treat it as zero
            value = 0.0
        metric_config = self._config.metrics[name]
        all_labels = {DATABASE_LABEL: database.config.name}
        all_labels.update(database.config.labels)
        if labels:
            all_labels.update(labels)
        method = self._get_metric_method(metric_config)
        self._logger.debug(
            "updating metric",
            metric=name,
            method=method,
            value=value,
            labels=all_labels,
        )
        metric = self._registry.get_metric(name, labels=all_labels)
        self._update_metric_value(metric, method, value)
        self._last_seen.update(name, all_labels, self._loop.time())

    def _get_metric_method(self, metric: MetricConfig) -> str:
        if metric.type == "counter" and not metric.config.get(
            "increment", False
        ):
            method = "set"
        else:
            method = {
                "counter": "inc",
                "gauge": "set",
                "histogram": "observe",
                "summary": "observe",
                "enum": "state",
            }[metric.type]
        return method

    def _update_metric_value(
        self, metric: MetricWrapperBase, method: str, value: t.Any
    ) -> None:
        if metric._type == "counter" and method == "set":
            # counters can only be incremented, directly set the underlying value
            t.cast(Counter, metric)._value.set(value)
        else:
            getattr(metric, method)(value)

    def _increment_queries_count(
        self, database: DataBase, query: Query, status: str
    ) -> None:
        """Increment count of queries in a status for a database."""
        self._update_metric(
            database,
            QUERIES_METRIC_NAME,
            1,
            labels={"query": query.name, "status": status},
        )

    def _increment_db_error_count(self, database: DataBase) -> None:
        """Increment number of errors for a database."""
        self._update_metric(database, DB_ERRORS_METRIC_NAME, 1)

    def _update_query_latency_metric(
        self, database: DataBase, query: Query, latency: float
    ) -> None:
        """Update latency metric for a query on a database."""
        self._update_metric(
            database,
            QUERY_LATENCY_METRIC_NAME,
            latency,
            labels={"query": query.name},
        )

    def _update_query_timestamp_metric(
        self, database: DataBase, query: Query, timestamp: float
    ) -> None:
        """Update timestamp metric for a query on a database."""
        self._update_metric(
            database,
            QUERY_TIMESTAMP_METRIC_NAME,
            timestamp,
            labels={"query": query.name},
        )
