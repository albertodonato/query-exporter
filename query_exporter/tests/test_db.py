import pytest
from sqlalchemy import create_engine
from sqlalchemy_aio import ASYNCIO_STRATEGY
from sqlalchemy_aio.base import AsyncConnection

from ..db import (
    DataBase,
    DataBaseError,
    InvalidDatabaseDSN,
    InvalidResultCount,
    Query,
    QueryResults,
    validate_dsn,
)


class TestQuery:

    def test_instantiate(self):
        """A query can be instantiated with the specified arguments."""
        query = Query(
            'query', 20, ['db1', 'db2'], ['metric1', 'metric2'], 'SELECT 1')
        assert query.name == 'query'
        assert query.interval == 20
        assert query.databases == ['db1', 'db2']
        assert query.metrics == ['metric1', 'metric2']
        assert query.sql == 'SELECT 1'

    def test_results(self):
        """The results method returns a dict mapping metrics to results."""
        query = Query('query', 20, ['db'], ['metric1', 'metric2'], 'SELECT 1')
        query_results = QueryResults(['one', 'two'], [(11, 22), (33, 44)])
        assert query.results(query_results) == {
            'metric1': (11, 33),
            'metric2': (22, 44)
        }

    def test_results_wrong_result_count(self):
        """An error is raised if the result column count is wrong."""
        query = Query('query', 20, ['db'], ['metric'], 'SELECT 1, 2')
        query_results = QueryResults(['one', 'two'], [(1, 2)])
        with pytest.raises(InvalidResultCount):
            query.results(query_results)

    def test_results_empty(self):
        """No error is raised if the result set is empty"""
        query = Query('query', 20, ['db'], ['metric'], 'SELECT 1, 2')
        query_results = QueryResults(['one', 'two'], [])
        assert query.results(query_results) == {}


class TestQueryResults:

    @pytest.mark.asyncio
    async def test_from_results(self, event_loop):
        engine = create_engine(
            'sqlite://', strategy=ASYNCIO_STRATEGY, loop=event_loop)
        async with engine.connect() as conn:
            result = await conn.execute('SELECT 1 AS a, 2 AS b')
            query_results = await QueryResults.from_results(result)
        assert query_results.keys == ['a', 'b']
        assert query_results.rows == [(1, 2)]


@pytest.fixture
async def db():
    db = DataBase('db', 'sqlite://')
    yield db
    await db.close()


class TestDataBase:

    def test_instantiate(self, db):
        """A DataBase can be instantiated with the specified arguments, db."""
        db = DataBase('db', 'sqlite:///foo')
        assert db.name == 'db'
        assert db.dsn == 'sqlite:///foo'

    @pytest.mark.asyncio
    async def test_connect(self, db):
        """The connect connects to the database."""
        await db.connect()
        assert isinstance(db._conn, AsyncConnection)

    @pytest.mark.asyncio
    async def test_connect_missing_engine_module(self, event_loop):
        """An error is raised if a module for the engine is missing."""
        db = DataBase('db', 'postgresql:///foo')
        with pytest.raises(DataBaseError) as error:
            await db.connect(loop=event_loop)
        assert str(error.value) == 'module "psycopg2" not found'

    @pytest.mark.asyncio
    async def test_connect_error(self, event_loop):
        """A DataBaseError is raised if database connection fails."""
        db = DataBase('db', f'sqlite:////invalid')
        with pytest.raises(DataBaseError) as error:
            await db.connect(loop=event_loop)
        assert 'unable to open database file' in str(error.value)

    @pytest.mark.asyncio
    async def test_close(self, db):
        """The close method closes database connection."""
        await db.connect()
        connection = db._conn
        await db.close()
        assert connection.closed
        assert db._conn is None

    @pytest.mark.asyncio
    async def test_execute_field_order(self, db):
        """The execute method executes a query."""
        sql = 'SELECT * FROM (SELECT 10, 20 UNION SELECT 30, 40)'
        query = Query('query', 20, ['db'], ['metric1', 'metric2'], sql)
        await db.connect()
        result = await db.execute(query)
        assert result == {'metric1': (10, 30), 'metric2': (20, 40)}

    @pytest.mark.asyncio
    async def test_execute_matching_column_names(self, db):
        """The execute method executes a query."""
        sql = (
            'SELECT metric2, metric1 FROM ('
            ' SELECT 10 AS metric2, 20 AS metric1 UNION'
            ' SELECT 30 AS metric2, 40 AS metric1)')
        query = Query('query', 20, ['db'], ['metric1', 'metric2'], sql)
        await db.connect()
        result = await db.execute(query)
        assert result == {'metric1': (20, 40), 'metric2': (10, 30)}

    @pytest.mark.asyncio
    async def test_execute_query_error(self, db):
        """If the query fails an error is raised."""
        query = Query('query', 20, ['db'], ['metric'], 'WRONG')
        await db.connect()
        with pytest.raises(DataBaseError) as err:
            await db.execute(query)
        assert 'syntax error' in str(err.value)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_execute_not_connected(self, db):
        """The execute recconnects to the database if not connected."""
        query = Query('query', 20, ['db'], ['metric'], 'SELECT 1')
        result = await db.execute(query)
        assert result == {'metric': (1, )}
        # the connection is kept for reuse
        assert not db._conn.closed


class TestValidateDSN:

    def test_valid(self):
        assert validate_dsn('postgresql://user:pass@host/database') is None

    def test_invalid(self):
        with pytest.raises(InvalidDatabaseDSN):
            validate_dsn('foo-bar')
