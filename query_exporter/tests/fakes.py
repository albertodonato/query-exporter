'''Fakes for testing.'''

from psycopg2 import OperationalError, ProgrammingError


class FakeAiopg:

    dsn = None

    def __init__(self, connect_error=None, query_results=None):
        self.connect_error = connect_error
        self.query_results = query_results

    async def create_pool(self, dsn):
        self.dsn = dsn
        if self.connect_error:
            raise OperationalError(self.connect_error)
        return FakePool(dsn, query_results=self.query_results)


class FakePool:

    closed = False

    def __init__(self, dsn, query_results=None):
        self.dsn = dsn
        self.query_results = query_results

    async def acquire(self):
        return FakeConnection(query_results=self.query_results)

    def close(self):
        self.closed = True


class FakeConnection:

    closed = False
    curr = None

    def __init__(self, query_results=None):
        self.query_results = query_results

    async def close(self):
        self.closed = True

    def cursor(self):
        if not self.curr:
            self.curr = FakeCursor(results=self.query_results)
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

    async def fetchmany(self):
        return self.results
