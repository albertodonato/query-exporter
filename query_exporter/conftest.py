import asyncio

import pytest
from toolrack.conftest import advance_time

from .db import DataBase

__all__ = ["advance_time", "query_tracker"]


@pytest.fixture
def query_tracker(request, mocker, event_loop):
    """Return a list collecting Query executed by DataBases."""

    class QueryTracker:
        def __init__(self):
            self.queries = []
            self.results = []
            self.failures = []

        async def wait_queries(self, count=1, timeout=5):
            await self._wait("queries", count, timeout)

        async def wait_results(self, count=1, timeout=5):
            await self._wait("results", count, timeout)

        async def wait_failures(self, count=1, timeout=5):
            await self._wait("failures", count, timeout)

        async def _wait(self, attr, count, timeout):
            if "advance_time" in request.fixturenames:
                # can't depend explicitly on the advance_time fixture or it
                # would activate and always require explicit time advance
                time_advance_func = request.getfixturevalue("advance_time")
            else:
                time_advance_func = asyncio.sleep

            end = event_loop.time() + timeout
            while event_loop.time() < end:
                collection = getattr(self, attr)
                if len(collection) >= count:
                    return collection
                await time_advance_func(0.05)
            raise TimeoutError(f"No {attr} found after {timeout}s")

    tracker = QueryTracker()
    orig_execute = DataBase.execute

    async def execute(self, query):
        tracker.queries.append(query)
        try:
            result = await orig_execute(self, query)
        except Exception as e:
            tracker.failures.append(e)
            raise
        tracker.results.append(result)
        return result

    mocker.patch.object(DataBase, "execute", execute)
    yield tracker
