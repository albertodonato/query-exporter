"""Enhanced database wrapper with parallel execution support."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from .db import (
    DataBase,
    DataBaseConfig,
    QueryExecution,
    QueryResults,
    MetricResults,
    QueryTimeoutExpired,
    FATAL_ERRORS,
)


class ParallelDataBase(DataBase):
    """Enhanced DataBase with connection pooling and parallel execution."""

    def __init__(
        self,
        config: DataBaseConfig,
        logger=None,
        pool_size: int = 5,
        max_overflow: int = 10,
    ):
        """Initialize with connection pooling.
        
        Args:
            config: Database configuration
            logger: Logger instance
            pool_size: Number of connections to maintain in the pool
            max_overflow: Maximum number of connections to create beyond pool_size
        """
        self.config = config
        if logger is None:
            import structlog
            logger = structlog.get_logger()
        self.logger = logger.bind(database=self.config.name)
        
        # Create engine with connection pooling
        self._engine = self._create_pooled_engine(pool_size, max_overflow)
        self._executor = ThreadPoolExecutor(
            max_workers=pool_size + max_overflow,
            thread_name_prefix=f"DB-{self.config.name}"
        )
        self._pending_queries = 0
        self._connect_lock = asyncio.Lock()

    def _create_pooled_engine(
        self, pool_size: int, max_overflow: int
    ) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        from .db import create_db_engine
        
        # Use QueuePool for connection pooling instead of NullPool
        engine = create_engine(
            self.config.dsn,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,  # Verify connections before using
            pool_recycle=3600,   # Recycle connections after 1 hour
            echo=False,
        )
        
        # Setup query latency tracking
        self._setup_query_latency_tracking(engine)
        return engine

    async def execute(self, query_execution: QueryExecution) -> MetricResults:
        """Execute a query with connection pooling."""
        self.logger.debug("run query", query=query_execution.name)
        self._pending_queries += 1
        query = query_execution.query
        
        try:
            query_results = await self._execute_with_pool(
                query.sql,
                parameters=query_execution.parameters,
                timeout=query.timeout,
            )
            return query.results(query_results)
        except TimeoutError:
            self.logger.warning("query timeout", query=query_execution.name)
            raise QueryTimeoutExpired()
        except Exception as error:
            raise self._query_db_error(
                query_execution.name,
                error,
                fatal=isinstance(error, FATAL_ERRORS),
            )
        finally:
            self._pending_queries -= 1

    async def _execute_with_pool(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
        timeout: int | float | None = None,
    ) -> QueryResults:
        """Execute SQL using connection pool."""
        if parameters is None:
            parameters = {}
        
        loop = asyncio.get_event_loop()
        
        # Execute in thread pool to avoid blocking
        def _execute_sync():
            from time import perf_counter, time as current_time
            
            start_time = perf_counter()
            with self._engine.connect() as conn:
                if self.config.autocommit:
                    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                    
                result = conn.execute(text(sql), parameters)
                latency = perf_counter() - start_time
                
                keys = []
                rows = []
                if result.returns_rows:
                    keys, rows = list(result.keys()), result.all()
                
                return QueryResults(
                    keys=keys,
                    rows=rows,
                    timestamp=current_time(),
                    latency=latency,
                )
        
        # Run with timeout
        return await asyncio.wait_for(
            loop.run_in_executor(self._executor, _execute_sync),
            timeout=timeout,
        )

    async def close(self) -> None:
        """Close the database connection pool and executor."""
        async with self._connect_lock:
            self._executor.shutdown(wait=True)
            self._engine.dispose()
            self.logger.debug("disconnected and pool disposed")

    @property
    def connected(self) -> bool:
        """Connection pool is always available."""
        return True

    async def connect(self) -> None:
        """No-op for pooled connections - pool manages connections."""
        pass

