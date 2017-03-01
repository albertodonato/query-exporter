'''Fakes for testing.'''

from psycopg2 import OperationalError, ProgrammingError


class FakeAiopg:

    dsn = None

    def __init__(self, connect_error=None):
        self.connect_error = connect_error

    async def create_pool(self, dsn):
        self.dsn = dsn
        if self.connect_error:
            raise OperationalError(self.connect_error)
        return FakePool(dsn)


class FakePool:

    closed = False

    def __init__(self, dsn):
        self.dsn = dsn

    async def acquire(self):
        return FakeConnection()

    def close(self):
        self.closed = True


class FakeConnection:

    closed = False
    curr = None

    async def close(self):
        self.closed = True

    def cursor(self):
        if not self.curr:
            self.curr = FakeCursor()
        return self.curr


class FakeCursor:

    sql = None

    def __init__(self, results=None, query_error=None):
        self.results = results
        self.query_error = query_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    async def execute(self, sql):
        if self.query_error:
            raise ProgrammingError(self.query_error)
        self.sql = sql

    async def fetchone(self):
        return self.results
