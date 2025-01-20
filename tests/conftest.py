import asyncio
from collections.abc import AsyncIterator, Iterator

import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
from toolrack.testing.fixtures import advance_time

from query_exporter.db import DataBase, MetricResults, QueryExecution

__all__ = ["QueryTracker", "advance_time", "query_tracker"]


@pytest.fixture(autouse=True)
def _autouse(log: StructuredLogCapture) -> Iterator[None]:
    """Autouse dependent fixtures."""
    yield None


class QueryTracker:
    def __init__(self) -> None:
        self.queries: list[QueryExecution] = []
        self.results: list[MetricResults] = []
        self.failures: list[Exception] = []
        self._loop = asyncio.get_event_loop()

    async def wait_queries(self, count: int = 1, timeout: int = 5) -> None:
        await self._wait("queries", count, timeout)

    async def wait_results(self, count: int = 1, timeout: int = 5) -> None:
        await self._wait("results", count, timeout)

    async def wait_failures(self, count: int = 1, timeout: int = 5) -> None:
        await self._wait("failures", count, timeout)

    async def _wait(self, attr: str, count: int, timeout: int) -> None:
        end = self._loop.time() + timeout
        while self._loop.time() < end:
            collection = getattr(self, attr)
            if len(collection) >= count:
                return
            await asyncio.sleep(0.05)
        raise TimeoutError(f"No {attr} found after {timeout}s")


@pytest.fixture
async def query_tracker(
    request: pytest.FixtureRequest, mocker: MockerFixture
) -> AsyncIterator[QueryTracker]:
    """Return a list collecting Query executed by DataBases."""
    tracker = QueryTracker()
    orig_execute = DataBase.execute

    async def execute(
        db: DataBase, query_execution: QueryExecution
    ) -> MetricResults:
        tracker.queries.append(query_execution)
        try:
            result = await orig_execute(db, query_execution)
        except Exception as e:
            tracker.failures.append(e)
            raise
        tracker.results.append(result)
        return result

    mocker.patch.object(DataBase, "execute", execute)
    yield tracker
