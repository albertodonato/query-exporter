import asyncio
from collections.abc import AsyncIterator, Iterator
import typing as t

import pytest

from query_exporter.periodic_call import (
    AlreadyRunning,
    NotRunning,
    PeriodicCall,
    TimedCall,
)

from .conftest import AdvanceTime


def loop_time() -> float:
    return asyncio.get_event_loop().time()


@pytest.fixture
def calls() -> Iterator[list[t.Any]]:
    yield []


@pytest.fixture
def sync_func(calls: list[t.Any]) -> Iterator[t.Callable[[], None]]:
    yield lambda: calls.append(loop_time())


@pytest.fixture
def async_func(
    calls: list[t.Any],
) -> Iterator[t.Callable[[], t.Awaitable[None]]]:
    async def func() -> None:
        calls.append(loop_time())
        await asyncio.sleep(0.1)

    yield func


@pytest.fixture
async def timed_call(
    sync_func: t.Callable[[], None],
) -> AsyncIterator[TimedCall]:
    call = TimedCall(sync_func)
    yield call
    if call.running:
        await call.stop()


@pytest.fixture
def periodic_call(sync_func: t.Callable[[], None]) -> Iterator[PeriodicCall]:
    yield PeriodicCall(sync_func)


@pytest.fixture
def time_intervals() -> Iterator[list[float]]:
    yield [1.0, 5.0]


TimesIterator = t.Callable[[], Iterator[float]]


@pytest.fixture
def times_iter(time_intervals: list[float]) -> Iterator[TimesIterator]:
    def times() -> Iterator[float]:
        time = loop_time()
        for interval in time_intervals:
            time += interval
            yield time

    yield times


class TestTimedCall:
    def test_running(
        self, timed_call: TimedCall, times_iter: TimesIterator
    ) -> None:
        """The TimedCall is not running by default."""
        assert not timed_call.running
        timed_call.start(times_iter())
        assert timed_call.running

    @pytest.mark.asyncio
    async def test_start(
        self,
        advance_time: AdvanceTime,
        timed_call: TimedCall,
        times_iter: TimesIterator,
        calls: list[t.Any],
    ) -> None:
        """Starting the TimedCall makes it call the function."""
        timed_call.start(times_iter())
        await advance_time(2)
        assert calls == [1.0]

    def test_start_already_running(
        self, timed_call: TimedCall, times_iter: TimesIterator
    ) -> None:
        """Starting an already started TimedCall raises an error."""
        timed_call.start(times_iter())
        with pytest.raises(AlreadyRunning):
            timed_call.start(times_iter())

    @pytest.mark.asyncio
    async def test_stop(
        self,
        advance_time: AdvanceTime,
        timed_call: TimedCall,
        times_iter: TimesIterator,
        calls: list[t.Any],
    ) -> None:
        """Stopping the TimedCall stops runs."""
        timed_call.start(times_iter())
        await advance_time(2)
        await timed_call.stop()
        await advance_time(10)
        # Only the initial call is performed
        assert calls == [1.0]

    @pytest.mark.asyncio
    async def test_stop_not_running(self, timed_call: TimedCall) -> None:
        """Stopping a TimedCall that is not running raises an error."""
        with pytest.raises(NotRunning):
            await timed_call.stop()

    @pytest.mark.asyncio
    async def test_func_arguments(
        self,
        advance_time: AdvanceTime,
        times_iter: TimesIterator,
        calls: list[t.Any],
    ) -> None:
        """Specified arguments are passed to the function on call."""

        def func(*args: t.Any, **kwargs: t.Any) -> None:
            calls.append((args, kwargs))

        timed_call = TimedCall(func, "foo", "bar", baz="baz", bza="bza")
        timed_call.start(times_iter())
        await advance_time(2)
        await timed_call.stop()
        assert calls == [(("foo", "bar"), {"baz": "baz", "bza": "bza"})]

    @pytest.mark.asyncio
    async def test_run_at_time_intervals(
        self,
        advance_time: AdvanceTime,
        timed_call: TimedCall,
        times_iter: TimesIterator,
        calls: list[t.Any],
    ) -> None:
        """The sync function is called at specified time intervals."""
        timed_call.start(times_iter())
        await advance_time(10)
        assert calls == [1.0, 6.0]

    @pytest.mark.asyncio
    async def test_run_async_at_time_intervals(
        self,
        advance_time: AdvanceTime,
        async_func: t.Callable[[], t.Awaitable[None]],
        times_iter: TimesIterator,
        calls: list[t.Any],
    ) -> None:
        """The async function is called at specified time intervals."""
        timed_call = TimedCall(async_func)
        timed_call.start(times_iter())
        await advance_time(10)
        assert calls == [1.0, 6.0]

    @pytest.mark.asyncio
    async def test_stop_after_iterator_ends(
        self,
        advance_time: AdvanceTime,
        timed_call: TimedCall,
        times_iter: TimesIterator,
        time_intervals: list[float],
        calls: list[t.Any],
    ) -> None:
        """The TimedCall stops if the time iterator ends."""
        timed_call.start(times_iter())
        await advance_time(10)
        assert not timed_call.running


@pytest.mark.asyncio
class TestPeriodicCall:
    async def test_start(
        self,
        advance_time: AdvanceTime,
        periodic_call: PeriodicCall,
        calls: list[t.Any],
    ) -> None:
        """Starting the PeriodicCall makes it call the function immediately."""
        periodic_call.start(5)
        await advance_time(1)
        assert calls == [0]

    async def test_stop(
        self,
        advance_time: AdvanceTime,
        periodic_call: PeriodicCall,
        calls: list[t.Any],
    ) -> None:
        """Stopping the PeriodicCall stops periodic runs."""
        periodic_call.start(5)
        await advance_time(1)
        await periodic_call.stop()
        await advance_time(10)
        # Only the initial call is performed
        assert calls == [0]

    async def test_periodic(
        self,
        advance_time: AdvanceTime,
        periodic_call: PeriodicCall,
        calls: list[t.Any],
    ) -> None:
        """The PeriodicCall gets called at each interval."""
        periodic_call.start(5)
        await advance_time(6)
        assert calls == [0, 5.0]
        await advance_time(5)
        assert calls == [0, 5.0, 10.0]

    async def test_start_later(
        self,
        advance_time: AdvanceTime,
        periodic_call: PeriodicCall,
        calls: list[t.Any],
    ) -> None:
        """If now is False, the function is not run immediately."""
        periodic_call.start(5, now=False)
        await advance_time(1)
        assert calls == []

    async def test_start_later_run_after_interval(
        self,
        advance_time: AdvanceTime,
        periodic_call: PeriodicCall,
        calls: list[t.Any],
    ) -> None:
        """If now is False, the function is run after the interval."""
        periodic_call.start(5, now=False)
        await advance_time(6)
        assert calls == [5.0]
