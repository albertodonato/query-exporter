"""Enhanced query executor with improved parallelism."""

import asyncio
from collections import defaultdict
from itertools import chain

import structlog

from .config import Config
from .executor import QueryExecutor, MetricsLastSeen
from .db import QueryExecution
from .db_parallel import ParallelDataBase


class ParallelQueryExecutor(QueryExecutor):
    """Enhanced QueryExecutor with better parallel execution."""

    def __init__(
        self,
        config: Config,
        registry,
        logger: structlog.stdlib.BoundLogger | None = None,
        pool_size: int = 5,
        max_overflow: int = 10,
    ):
        """Initialize with parallel database connections.
        
        Args:
            config: Configuration object
            registry: Metrics registry
            logger: Logger instance
            pool_size: Number of connections per database pool
            max_overflow: Maximum additional connections beyond pool_size
        """
        self._config = config
        self._registry = registry
        self._logger = logger or structlog.get_logger()
        self._timed_query_executions = []
        self._aperiodic_query_executions = []
        self._timed_calls = {}
        self._doomed_queries = defaultdict(set)
        self._loop = asyncio.get_event_loop()
        self._last_seen = MetricsLastSeen(
            {
                name: metric.config.get("expiration")
                for name, metric in self._config.metrics.items()
            }
        )
        
        # Use ParallelDataBase instead of DataBase
        self._databases = {
            db_config.name: ParallelDataBase(
                db_config,
                logger=self._logger,
                pool_size=pool_size,
                max_overflow=max_overflow,
            )
            for db_config in self._config.databases.values()
        }

        for query_execution in chain(
            *(query.executions for query in self._config.queries.values())
        ):
            if query_execution.query.timed:
                self._timed_query_executions.append(query_execution)
            else:
                self._aperiodic_query_executions.append(query_execution)

    def _run_query(self, query_execution: QueryExecution) -> None:
        """Run query on all databases in parallel."""
        # Create all tasks at once for parallel execution
        tasks = [
            self._execute_query(query_execution, dbname)
            for dbname in query_execution.query.databases
        ]
        
        # Execute all tasks concurrently
        if tasks:
            self._loop.create_task(self._run_parallel_tasks(tasks))

    async def _run_parallel_tasks(self, tasks: list) -> None:
        """Execute multiple tasks in parallel and handle errors."""
        # gather with return_exceptions=True to prevent one failure from stopping others
        await asyncio.gather(*tasks, return_exceptions=True)

    async def run_aperiodic_queries(self) -> None:
        """Run all aperiodic queries with improved parallelism."""
        # Group queries by database to batch execution
        db_query_map = defaultdict(list)
        for query_execution in self._aperiodic_query_executions:
            for dbname in query_execution.query.databases:
                db_query_map[dbname].append(query_execution)
        
        # Execute all queries for all databases in parallel
        all_tasks = []
        for dbname, query_executions in db_query_map.items():
            for query_execution in query_executions:
                all_tasks.append(self._execute_query(query_execution, dbname))
        
        # Run everything in parallel
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

