"""Fakes for testing."""


class FakeSQLAlchemy:
    """Fake sqlalchemy module."""

    # overridable attributes
    connect_error = None
    query_results = None
    query_error = None
    missing_module = None

    # set internally
    dsn = None
    strategy = None
    engine = None

    def create_engine(self, dsn, strategy=None):
        self.dsn = dsn
        self.strategy = strategy
        if self.missing_module is not None:
            raise ImportError(
                f'Failed to import module "{self.missing_module}"',
                name=self.missing_module)
        self.engine = FakeEngine(
            dsn,
            query_results=self.query_results,
            query_error=self.query_error,
            connect_error=self.connect_error)
        return self.engine


class FakeEngine:
    """Fake database engine."""

    connection = None

    def __init__(
            self, dsn, query_results=None, query_error=None,
            connect_error=None):
        self.dsn = dsn
        self.query_results = query_results
        self.query_error = query_error
        self.connect_error = connect_error

    async def connect(self):
        if self.connect_error:
            raise Exception(self.connect_error)
        self.connection = FakeConnection()
        self.connection.query_results = self.query_results
        self.connection.query_error = self.query_error
        return self.connection


class FakeConnection:
    """Fake connection."""

    # overridable attributes
    query_results = None
    query_error = None

    # set internally
    closed = False
    sql = None

    async def close(self):
        self.closed = True

    async def execute(self, sql):
        if self.query_error:
            raise Exception(self.query_error)
        self.sql = sql
        return FakeQueryResult(self.query_results)


class FakeQueryResult:
    """Fake query result."""

    def __init__(self, query_results):
        self.query_results = query_results

    async def fetchall(self):
        return self.query_results
