"""Fakes for testing."""


class FakeAsyncpg:
    """Fake asyncpg module."""

    dsn = None
    pool = None

    def __init__(self, connect_error=None, query_results=None,
                 query_error=None):
        self.connect_error = connect_error
        self.query_results = query_results
        self.query_error = query_error

    async def create_pool(self, dsn):
        self.dsn = dsn
        if self.connect_error:
            raise Exception(self.connect_error)
        self.pool = FakePool(
            dsn, query_results=self.query_results,
            query_error=self.query_error)
        return self.pool


class FakePool:
    """Fake connection pool."""

    closed = False
    connection = None

    def __init__(self, dsn, query_results=None, query_error=None):
        self.dsn = dsn
        self.query_results = query_results
        self.query_error = query_error

    def acquire(self):
        self.connection = FakeConnection(
            query_results=self.query_results, query_error=self.query_error)
        return self.connection

    async def close(self):
        self.closed = True


class FakeConnection:
    """Fake connection."""

    closed = False
    sql = None

    def __init__(self, query_results=None, query_error=None):
        self.query_results = query_results
        self.query_error = query_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def close(self):
        self.closed = True

    def transaction(self):
        return self

    async def fetch(self, sql):
        if self.query_error:
            raise Exception(self.query_error)
        self.sql = sql
        return self.query_results
