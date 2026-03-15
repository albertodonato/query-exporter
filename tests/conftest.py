import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import timedelta
from pathlib import Path
import time as time_module
from typing import Any
import uuid

from prometheus_client.metrics import MetricWrapperBase
import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
import time_machine
import yaml

from query_exporter.db import Database, MetricResults, QueryExecution

__all__ = ["MetricValues", "QueryTracker", "advance_time", "metric_values"]


@pytest.fixture(autouse=True)
def _autouse(log: StructuredLogCapture) -> Iterator[None]:
    """Autouse dependent fixtures."""
    yield None


@pytest.fixture
def traveler() -> Iterator[time_machine.Traveller]:
    """Freeze wall-clock time at the current instant."""
    with time_machine.travel(time_module.time(), tick=False) as traveler:
        yield traveler


AdvanceTime = Callable[[float], Awaitable[None]]


@pytest.fixture
async def advance_time(
    traveler: time_machine.Traveller,
) -> AsyncIterator[AdvanceTime]:
    """Return a function to manually advance time in tests.

    Freezes both the asyncio loop clock and the wall clock, advancing them in
    sync so that both asyncio-scheduled callbacks and APScheduler jobs fire at
    the right simulated time.
    """
    loop = asyncio.get_event_loop()
    fake_time: float = 0.0

    def _next_scheduled() -> float | None:
        try:
            return loop._scheduled[0]._when  # type: ignore
        except IndexError:
            return None

    async def _drain_loop() -> None:
        while True:
            next_time = _next_scheduled()
            if not loop._ready and (  # type: ignore
                next_time is None or next_time > fake_time
            ):
                break
            await asyncio.sleep(0)

    async def advance(seconds: float) -> None:
        nonlocal fake_time
        await _drain_loop()
        target_time = fake_time + seconds
        while True:
            next_time = _next_scheduled()
            if next_time is None or next_time > target_time:
                break
            delta = next_time - fake_time
            fake_time = next_time
            traveler.shift(timedelta(seconds=delta))
            await _drain_loop()
        delta = target_time - fake_time
        fake_time = target_time
        traveler.shift(timedelta(seconds=delta))
        await _drain_loop()

    original_loop_time = loop.time
    loop.time = lambda: fake_time  # type: ignore
    try:
        yield advance
    finally:
        loop.time = original_loop_time  # type: ignore


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
    """Return a list collecting Query executed by Databases."""
    tracker = QueryTracker()
    orig_execute = Database.execute

    async def execute(
        db: Database, query_execution: QueryExecution
    ) -> MetricResults:
        tracker.queries.append(query_execution)
        try:
            result = await orig_execute(db, query_execution)
        except Exception as e:
            tracker.failures.append(e)
            raise
        tracker.results.append(result)
        return result

    mocker.patch.object(Database, "execute", execute)
    yield tracker


@pytest.fixture
def sample_config() -> Iterator[dict[str, Any]]:
    yield {
        "databases": {"db": {"dsn": "sqlite:///:memory:"}},
        "metrics": {"m": {"type": "gauge", "labels": ["l1", "l2"]}},
        "queries": {
            "q": {
                "interval": 10,
                "databases": ["db"],
                "metrics": ["m"],
                "sql": "SELECT 1 AS m",
            }
        },
    }


ConfigWriter = Callable[[Any], Path]


@pytest.fixture
def write_config(tmp_path: Path) -> Iterator[ConfigWriter]:
    def write(data: Any) -> Path:
        path = tmp_path / f"{uuid.uuid4()}.yaml"
        path.write_text(yaml.dump(data), "utf-8")
        return path

    yield write


MetricValues = list[int | float] | dict[tuple[str, ...], int | float]


def metric_values(
    metric: MetricWrapperBase, by_labels: tuple[str, ...] = ()
) -> MetricValues:
    """Return values for the metric."""
    suffix = ""
    if metric._type == "counter":
        suffix = "_total"

    values_by_label: dict[tuple[str, ...], int | float] = {}
    values_by_suffix: dict[str, list[int | float]] = defaultdict(list)
    for sample_suffix, labels, value, *_ in metric._samples():
        if sample_suffix == suffix:
            if by_labels:
                label_values = tuple(labels[label] for label in by_labels)
                values_by_label[label_values] = value
            else:
                values_by_suffix[sample_suffix].append(value)

    return values_by_label if by_labels else values_by_suffix[suffix]
