"""Database wrapper."""

import asyncio
from collections.abc import (
    Callable,
    Iterable,
    Sequence,
)
from dataclasses import (
    dataclass,
    field,
)
from functools import partial
from itertools import chain
from threading import (
    Thread,
    current_thread,
)
from time import (
    perf_counter,
    time,
)
from types import TracebackType
import typing as t

from croniter import croniter
from sqlalchemy import (
    create_engine,
    event,
    text,
)
from sqlalchemy.engine import (
    Connection,
    CursorResult,
    Engine,
)
from sqlalchemy.exc import (
    ArgumentError,
    NoSuchModuleError,
)
from sqlalchemy.sql.elements import TextClause
import structlog

#: Timeout for a query
QueryTimeout = int | float


#: Label used to tag metrics by database
DATABASE_LABEL = "database"


class DataBaseError(Exception):
    """A databease error.

    if `fatal` is True, it means the Query will never succeed.
    """

    def __init__(self, message: str, fatal: bool = False) -> None:
        super().__init__(message)
        self.fatal = fatal


class DataBaseConnectError(DataBaseError):
    """Database connection error."""


class DataBaseQueryError(DataBaseError):
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


class InvalidQuerySchedule(Exception):
    """Query schedule is wrong or both schedule and interval specified."""

    def __init__(self, query_name: str, message: str) -> None:
        super().__init__(
            f'Invalid schedule for query "{query_name}": {message}'
        )


# Database errors that mean the query won't ever succeed.  Not all possible
# fatal errors are tracked here, because some DBAPI errors can happen in
# circumstances which can be fatal or not.  Since there doesn't seem to be a
# reliable way to know, there might be cases when a query will never succeed
# but will end up being retried.
FATAL_ERRORS = (InvalidResultCount, InvalidResultColumnNames)


@dataclass(frozen=True)
class DataBaseConfig:
    """Configuration for a database."""

    name: str
    dsn: str
    connect_sql: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    keep_connected: bool = True
    autocommit: bool = True

    def __post_init__(self) -> None:
        # raise DatabaseError error if the DSN in invalid
        create_db_engine(self.dsn)


def create_db_engine(dsn: str, **kwargs: t.Any) -> Engine:
    """Create the database engine, validating the DSN"""
    try:
        return create_engine(dsn, **kwargs)
    except ImportError as error:
        raise DataBaseError(f'module "{error.name}" not found')
    except (ArgumentError, ValueError, NoSuchModuleError):
        raise DataBaseError(f'Invalid database DSN: "{dsn}"')


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
    parameters: dict[str, t.Any] = field(default_factory=dict)
    timeout: QueryTimeout | None = None
    interval: int | None = None
    schedule: str | None = None
    config_name: str = ""

    def __post_init__(self) -> None:
        if not self.config_name:
            self.config_name = self.name
        self._check_schedule()
        self._check_query_parameters()

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

    def _check_schedule(self) -> None:
        if self.interval and self.schedule:
            raise InvalidQuerySchedule(
                self.name, "both interval and schedule specified"
            )
        if self.schedule and not croniter.is_valid(self.schedule):
            raise InvalidQuerySchedule(self.name, "invalid schedule format")

    def _check_query_parameters(self) -> None:
        expr = text(self.sql)
        query_params = set(expr.compile().params)
        if set(self.parameters) != query_params:
            raise InvalidQueryParameters(self.name)


class WorkerAction:
    """An action to be called in the worker thread."""

    def __init__(
        self, func: Callable[..., t.Any], *args: t.Any, **kwargs: t.Any
    ) -> None:
        self._func = partial(func, *args, **kwargs)
        self._loop = asyncio.get_event_loop()
        self._future = self._loop.create_future()

    def __str__(self) -> str:
        return self._func.func.__name__

    __repr__ = __str__

    def __call__(self) -> None:
        """Call the action asynchronously in a thread-safe way."""
        try:
            result = self._func()
        except Exception as e:
            self._call_threadsafe(self._future.set_exception, e)
        else:
            self._call_threadsafe(self._future.set_result, result)

    async def result(self) -> t.Any:
        """Wait for completion and return the action result."""
        return await self._future

    def _call_threadsafe(
        self, call: Callable[..., t.Any], *args: t.Any
    ) -> None:
        self._loop.call_soon_threadsafe(partial(call, *args))


class DataBaseConnection:
    """A connection to a database engine."""

    _conn: Connection | None = None
    _worker: Thread | None = None

    def __init__(
        self,
        dbname: str,
        engine: Engine,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self.dbname = dbname
        self.engine = engine
        self.logger = logger or structlog.get_logger()
        self._loop = asyncio.get_event_loop()
        self._queue: asyncio.Queue[WorkerAction] = asyncio.Queue()

    @property
    def connected(self) -> bool:
        """Whether the connection is open."""
        return self._conn is not None

    async def open(self) -> None:
        """Open the connection."""
        if self.connected:
            return

        self._create_worker()
        await self._call_in_thread(self._connect)

    async def close(self) -> None:
        """Close the connection."""
        if not self.connected:
            return

        await self._call_in_thread(self._close)
        self._terminate_worker()

    async def execute(
        self,
        sql: TextClause,
        parameters: dict[str, t.Any] | None = None,
    ) -> QueryResults:
        """Execute a query, returning results."""
        if parameters is None:
            parameters = {}
        result = await self._call_in_thread(self._execute, sql, parameters)
        query_results: QueryResults = await self._call_in_thread(
            QueryResults.from_result, result
        )
        return query_results

    def _create_worker(self) -> None:
        assert not self._worker
        self._worker = Thread(
            target=self._run, name=f"DataBase-{self.dbname}", daemon=True
        )
        self._worker.start()

    def _terminate_worker(self) -> None:
        assert self._worker
        self._worker.join()
        self._worker = None

    def _connect(self) -> None:
        self._conn = self.engine.connect()

    def _execute(
        self, sql: TextClause, parameters: dict[str, t.Any]
    ) -> CursorResult[t.Any]:
        assert self._conn
        return self._conn.execute(sql, parameters)

    def _close(self) -> None:
        assert self._conn
        self._conn.detach()
        self._conn.close()
        self._conn = None

    def _run(self) -> None:
        """The worker thread function."""
        logger = self.logger.bind(worker_id=current_thread().native_id)
        logger.debug("start")
        while True:
            future = asyncio.run_coroutine_threadsafe(
                self._queue.get(), self._loop
            )
            action = future.result()
            logger.debug("action received", action=str(action))
            action()
            self._loop.call_soon_threadsafe(self._queue.task_done)
            if self._conn is None:
                # the connection has been closed, exit the thread
                logger.debug("shutdown")
                return

    async def _call_in_thread(
        self, func: Callable[..., t.Any], *args: t.Any, **kwargs: t.Any
    ) -> t.Any:
        """Call a sync action in the worker thread."""
        call = WorkerAction(func, *args, **kwargs)
        await self._queue.put(call)
        return await call.result()


class DataBase:
    """A database to perform Queries."""

    _conn: DataBaseConnection
    _pending_queries: int = 0

    def __init__(
        self,
        config: DataBaseConfig,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self.config = config
        if logger is None:
            logger = structlog.get_logger()
        self.logger = logger.bind(database=self.config.name)
        self._connect_lock = asyncio.Lock()
        execution_options = {}
        if self.config.autocommit:
            execution_options["isolation_level"] = "AUTOCOMMIT"
        engine = create_db_engine(
            self.config.dsn,
            execution_options=execution_options,
        )
        self._conn = DataBaseConnection(self.config.name, engine, self.logger)
        self._setup_query_latency_tracking(engine)

    async def __aenter__(self) -> t.Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type,
        exc_value: Exception,
        traceback: TracebackType,
    ) -> None:
        await self.close()

    @property
    def connected(self) -> bool:
        """Whether the database is connected."""
        return self._conn.connected

    async def connect(self) -> None:
        """Connect to the database."""
        async with self._connect_lock:
            if self.connected:
                return

            try:
                await self._conn.open()
            except Exception as error:
                raise self._db_error(error, exc_class=DataBaseConnectError)

            self.logger.debug("connected")
            for sql in self.config.connect_sql:
                try:
                    await self.execute_sql(sql)
                except Exception as error:
                    await self._close()
                    raise self._db_error(
                        f'failed executing query "{sql}": {error}',
                        exc_class=DataBaseQueryError,
                    )

    async def close(self) -> None:
        """Close the database connection."""
        async with self._connect_lock:
            if not self.connected:
                return
            await self._close()

    async def execute(self, query: Query) -> MetricResults:
        """Execute a query."""
        await self.connect()
        self.logger.debug("run query", query=query.name)
        self._pending_queries += 1
        try:
            query_results = await self.execute_sql(
                query.sql, parameters=query.parameters, timeout=query.timeout
            )
            return query.results(query_results)
        except TimeoutError:
            self.logger.warning("query timeout", query=query.name)
            raise QueryTimeoutExpired()
        except Exception as error:
            raise self._query_db_error(
                query.name, error, fatal=isinstance(error, FATAL_ERRORS)
            )
        finally:
            assert self._pending_queries >= 0, "pending queries is negative"
            self._pending_queries -= 1
            if not self.config.keep_connected and not self._pending_queries:
                await self.close()

    async def execute_sql(
        self,
        sql: str,
        parameters: dict[str, t.Any] | None = None,
        timeout: QueryTimeout | None = None,
    ) -> QueryResults:
        """Execute a raw SQL query."""
        return await asyncio.wait_for(
            self._conn.execute(text(sql), parameters),
            timeout=timeout,
        )

    async def _close(self) -> None:
        # ensure the connection with the DB is actually closed
        await self._conn.close()
        self.logger.debug("disconnected")

    def _setup_query_latency_tracking(self, engine: Engine) -> None:
        @event.listens_for(engine, "before_cursor_execute")  # type: ignore
        def before_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ) -> None:
            conn.info["query_start_time"] = perf_counter()

        @event.listens_for(engine, "after_cursor_execute")  # type: ignore
        def after_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ) -> None:
            conn.info["query_latency"] = perf_counter() - conn.info.pop(
                "query_start_time"
            )

    def _query_db_error(
        self,
        query_name: str,
        error: str | Exception,
        fatal: bool = False,
    ) -> DataBaseError:
        """Create and log a DataBaseError for a failed query."""
        message = self._error_message(error)
        self.logger.exception(
            "query failed", query=query_name, error=message, exception=error
        )
        return DataBaseQueryError(message, fatal=fatal)

    def _db_error(
        self,
        error: str | Exception,
        exc_class: type[DataBaseError] = DataBaseError,
        fatal: bool = False,
    ) -> DataBaseError:
        """Create and log a DataBaseError."""
        message = self._error_message(error)
        self.logger.exception("database error", exception=error)
        return exc_class(message, fatal=fatal)

    def _error_message(self, error: str | Exception) -> str:
        """Return a message from an error."""
        message = str(error).strip()
        if not message and isinstance(error, Exception):
            message = error.__class__.__name__
        return message
