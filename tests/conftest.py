import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
import typing as t
import uuid

from prometheus_client.metrics import MetricWrapperBase
import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
from toolrack.testing.fixtures import advance_time
import yaml

from query_exporter.db import DataBase, MetricResults, QueryExecution

__all__ = ["MetricValues", "QueryTracker", "advance_time", "metric_values"]


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


@pytest.fixture
def sample_config() -> Iterator[dict[str, t.Any]]:
    yield {
        "databases": {"db": {"dsn": "sqlite://"}},
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


ConfigWriter = t.Callable[[t.Any], Path]


@pytest.fixture
def write_config(tmp_path: Path) -> Iterator[ConfigWriter]:
    def write(data: t.Any) -> Path:
        path = tmp_path / f"{uuid.uuid4()}.yaml"
        path.write_text(yaml.dump(data), "utf-8")
        return path

    yield write


MetricValues = list[int | float] | dict[tuple[str, ...], int | float]


def metric_values(
    metric: MetricWrapperBase, by_labels: tuple[str, ...] = ()
) -> MetricValues:
    """Return values for the metric."""
    if metric._type == "gauge":
        suffix = ""
    elif metric._type == "counter":
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
