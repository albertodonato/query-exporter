"""Database wrapper."""

import asyncio
from collections.abc import (
    Iterable,
    Iterator,
    Sequence,
)
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import (
    InitVar,
    dataclass,
    field,
)
from itertools import chain
from time import (
    perf_counter,
    time,
)
import typing as t

from sqlalchemy import (
    create_engine,
    event,
    make_url,
    text,
)
from sqlalchemy.engine import (
    CursorResult,
    Engine,
)
from sqlalchemy.engine.interfaces import DBAPIConnection, DBAPICursor
from sqlalchemy.exc import (
    ArgumentError,
    NoSuchModuleError,
)
from sqlalchemy.pool import ConnectionPoolEntry, NullPool, QueuePool
import structlog

from . import schema

# Timeout for a query
QueryTimeout = int | float


class DatabaseError(Exception):
    """A databease error.

    if `fatal` is True, it means the Query will never succeed.
    """

    def __init__(self, message: str, fatal: bool = False) -> None:
        super().__init__(message)
        self.fatal = fatal


class DatabaseConnectError(DatabaseError):
    """Database connection error."""


class DatabaseQueryError(DatabaseError):
    """Database query error."""


class QueryTimeoutExpired(Exception):
    """Query execution timeout expired."""


class InvalidResultCount(Exception):
    """Number of results from a query don't match metrics count."""

    def __init__(self, expected: int, got: int) -> None:
        super().__init__(
            f"Wrong result count from query: expected {expected}, got {got}"
        )


class InvalidResultColumnNames(Exception):
    """Invalid column names in query results."""

    def __init__(self, expected: list[str], got: list[str]) -> None:
        super().__init__(
            "Wrong column names from query: "
            f"expected {self._names(expected)}, got {self._names(got)}"
        )

    def _names(self, names: list[str]) -> str:
        names_list = ", ".join(names)
        return f"({names_list})"


class InvalidQueryParameters(Exception):
    """Query parameter names don't match those in query SQL."""

    def __init__(self, query_name: str) -> None:
        super().__init__(
            f'Parameters for query "{query_name}" don\'t match those from SQL'
        )


# Database errors that mean the query won't ever succeed.  Not all possible
# fatal errors are tracked here, because some DBAPI errors can happen in
# circumstances which can be fatal or not.  Since there doesn't seem to be a
# reliable way to know, there might be cases when a query will never succeed
# but will end up being retried.
FATAL_ERRORS = (InvalidResultCount, InvalidResultColumnNames)


def create_db_engine(config: schema.Database) -> Engine:
    """Create the database engine, validating the configuration."""
    try:
        url = make_url(str(config.dsn))
        connect_args = {}
        if url.get_backend_name() == "sqlite":
            # disable check as the logic ensures connections aren't used by
            # multiple threads at once
            connect_args["check_same_thread"] = False

        pool = config.connection_pool
        if pool.size == 0:
            return create_engine(
                url,
                poolclass=NullPool,
                connect_args=connect_args,
            )
        else:
            return create_engine(
                url,
                poolclass=QueuePool,
                pool_size=config.connection_pool.size,
                max_overflow=config.connection_pool.max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args=connect_args,
            )
    except ImportError as error:
        raise DatabaseError(f'Module "{error.name}" not found')
    except (ArgumentError, ValueError, NoSuchModuleError):
        raise DatabaseError(f'Invalid database DSN: "{config.dsn}"')


class QueryMetric(t.NamedTuple):
    """Metric details for a Query."""

    name: str
    labels: Iterable[str]


class QueryResults(t.NamedTuple):
    """Results of a database query."""

    keys: list[str]
    rows: Sequence[Sequence[t.Any]]
    timestamp: float | None = None
    latency: float | None = None

    @classmethod
    def from_result(cls, result: CursorResult[t.Any]) -> t.Self:
        """Return a QueryResults from results for a query."""
        timestamp = time()
        keys: list[str] = []
        rows: Sequence[Sequence[t.Any]] = []
        if result.returns_rows:
            keys, rows = list(result.keys()), result.all()
        latency = result.connection.info.get("query_latency", None)
        return cls(keys, rows, timestamp=timestamp, latency=latency)


class MetricResult(t.NamedTuple):
    """A result for a metric from a query."""

    metric: str
    value: t.Any
    labels: dict[str, str]


class MetricResults(t.NamedTuple):
    """Collection of metric results for a query."""

    results: list[MetricResult]
    timestamp: float | None = None
    latency: float | None = None


@dataclass
class Query:
    """Query definition and configuration."""

    name: str
    databases: list[str]
    metrics: list[QueryMetric]
    sql: str
    timeout: QueryTimeout | None = None
    interval: int | None = None
    schedule: str | None = None

    parameter_sets: InitVar[list[dict[str, t.Any]] | None] = None
    executions: list["QueryExecution"] = field(init=False, compare=False)

    def __post_init__(
        self, parameter_sets: list[dict[str, t.Any]] | None
    ) -> None:
        if not parameter_sets:
            self.executions = [QueryExecution(self.name, self)]
        else:
            self.executions = [
                QueryExecution(f"{self.name}[params{index}]", self, parameters)
                for index, parameters in enumerate(parameter_sets, 1)
            ]

    @property
    def timed(self) -> bool:
        """Whether the query is run periodically via interval or schedule."""
        return bool(self.interval or self.schedule)

    def labels(self) -> frozenset[str]:
        """Resturn all labels for metrics in the query."""
        return frozenset(chain(*(metric.labels for metric in self.metrics)))

    def results(self, query_results: QueryResults) -> MetricResults:
        """Return MetricResults from a query."""
        if not query_results.rows:
            return MetricResults([])

        result_keys = sorted(query_results.keys)
        labels = self.labels()
        metrics = [metric.name for metric in self.metrics]
        expected_keys = sorted(set(metrics) | labels)
        if len(expected_keys) != len(result_keys):
            raise InvalidResultCount(len(expected_keys), len(result_keys))
        if result_keys != expected_keys:
            raise InvalidResultColumnNames(expected_keys, result_keys)
        results = []
        for row in query_results.rows:
            values = dict(zip(query_results.keys, row))
            for metric in self.metrics:
                metric_result = MetricResult(
                    metric.name,
                    values[metric.name],
                    {label: values[label] for label in metric.labels},
                )
                results.append(metric_result)

        return MetricResults(
            results,
            timestamp=query_results.timestamp,
            latency=query_results.latency,
        )


@dataclass(frozen=True)
class QueryExecution:
    """A single execution configuration for a query, with parameters."""

    name: str
    query: Query
    parameters: dict[str, t.Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._check_query_parameters()

    def _check_query_parameters(self) -> None:
        expr = text(self.query.sql)
        query_params = set(expr.compile().params)
        if set(self.parameters) != query_params:
            raise InvalidQueryParameters(self.name)


class Database:
    """A database to perform Queries."""

    def __init__(
        self,
        name: str,
        config: schema.Database,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self.name = name
        self.config = config
        if logger is None:
            logger = structlog.get_logger()
        self.logger = logger.bind(database=self.name)

        self._engine = self._setup_engine()

        pool_config = self.config.connection_pool
        # need at least one worker even if connection pooling is disabledG
        max_workers = max(pool_config.size + pool_config.max_overflow, 1)
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"[Database-{self.name}]",
        )

    def close(self) -> None:
        self._engine.dispose()
        self._executor.shutdown(wait=True, cancel_futures=True)

    async def execute(self, query_execution: QueryExecution) -> MetricResults:
        """Execute a query."""
        self.logger.debug("run query", query=query_execution.name)
        query = query_execution.query
        try:
            query_results = await self.execute_sql(
                query.sql,
                parameters=query_execution.parameters,
                timeout=query.timeout,
            )
            return query.results(query_results)
        except DatabaseError:
            raise
        except TimeoutError:
            self.logger.warning("query timeout", query=query_execution.name)
            raise QueryTimeoutExpired()
        except Exception as error:
            raise self._query_db_error(
                query_execution.name,
                error,
                fatal=isinstance(error, FATAL_ERRORS),
            )

    async def execute_sql(
        self,
        sql: str,
        parameters: dict[str, t.Any] | None = None,
        timeout: QueryTimeout | None = None,
    ) -> QueryResults:
        """Execute a raw SQL query."""
        if parameters is None:
            parameters = {}

        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(
                self._executor, self._execute_sync, sql, parameters
            ),
            timeout=timeout,
        )

    def _setup_engine(self) -> Engine:
        engine = create_db_engine(self.config)

        @event.listens_for(engine, "connect")
        def on_connect(
            dbapi_conn: DBAPIConnection, conn: ConnectionPoolEntry
        ) -> None:
            if self.config.connect_sql:
                try:
                    with self._dbapi_transaction(dbapi_conn) as cursor:
                        for sql in self.config.connect_sql:
                            cursor.execute(sql)
                except Exception as error:
                    raise self._db_error(
                        f"failed executing connect SQL: {error}",
                        exc_class=DatabaseQueryError,
                    )
            self.logger.debug("connected")

        @event.listens_for(engine, "close")
        def on_close(
            dbapi_conn: DBAPIConnection, conn: ConnectionPoolEntry
        ) -> None:
            conn.info.clear()
            self.logger.debug("disconnected")

        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(
            conn: ConnectionPoolEntry,
            cursor: t.Any,
            statement: str,
            parameters: t.Any,
            context: t.Any,
            executemany: bool,
        ) -> None:
            conn.info["query_start_time"] = perf_counter()

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(
            conn: ConnectionPoolEntry,
            cursor: t.Any,
            statement: str,
            parameters: t.Any,
            context: t.Any,
            executemany: bool,
        ) -> None:
            conn.info["query_latency"] = perf_counter() - conn.info.pop(
                "query_start_time"
            )

        return engine

    def _execute_sync(
        self,
        sql: str,
        parameters: dict[str, t.Any],
    ) -> QueryResults:
        try:
            with self._engine.begin() as conn:
                try:
                    result = conn.execute(text(sql), parameters)
                    return QueryResults.from_result(result)
                except Exception as error:
                    raise self._db_error(error, exc_class=DatabaseQueryError)
        except DatabaseQueryError:
            raise
        except Exception as error:
            raise self._db_error(error, exc_class=DatabaseConnectError)

    def _query_db_error(
        self,
        query_name: str,
        error: str | Exception,
        fatal: bool = False,
    ) -> DatabaseError:
        """Create and log a DatabaseError for a failed query."""
        message = self._error_message(error)
        self.logger.exception(
            "query failed", query=query_name, error=message, exception=error
        )
        return DatabaseQueryError(message, fatal=fatal)

    def _db_error(
        self,
        error: str | Exception,
        exc_class: type[DatabaseError] = DatabaseError,
        fatal: bool = False,
    ) -> DatabaseError:
        """Create and log a DatabaseError."""
        message = self._error_message(error)
        self.logger.exception("database error", exception=error)
        return exc_class(message, fatal=fatal)

    def _error_message(self, error: str | Exception) -> str:
        """Return a message from an error."""
        message = str(error).strip()
        if not message and isinstance(error, Exception):
            message = error.__class__.__name__
        return message

    @contextmanager
    def _dbapi_transaction(
        self, dbapi_conn: DBAPIConnection
    ) -> Iterator[DBAPICursor]:
        cursor = dbapi_conn.cursor()
        try:
            yield cursor
            dbapi_conn.commit()
        except Exception:
            dbapi_conn.rollback()
            raise
        finally:
            cursor.close()
