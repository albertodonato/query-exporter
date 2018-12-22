import asyncio
from functools import wraps

import pytest


@pytest.fixture
async def advance_time(event_loop):
    """Replace the loop clock with a manually advanceable clock.

    Code is taken from `asynctest.ClockedTestCase`.

    """

    class Clocker:

        time = 0

        def __init__(self, loop):
            self.loop = loop
            self.loop.time = wraps(self.loop.time)(lambda: self.time)

        async def advance(self, seconds):
            await self._drain_loop()

            target_time = self.time + seconds
            while True:
                next_time = self._next_scheduled()
                if next_time is None or next_time > target_time:
                    break

                self.time = next_time
                await self._drain_loop()

            self.time = target_time
            await self._drain_loop()

        def _next_scheduled(self):
            try:
                return self.loop._scheduled[0]._when
            except IndexError:
                return None

        async def _drain_loop(self):
            while True:
                next_time = self._next_scheduled()
                if (not self.loop._ready
                        and (next_time is None or next_time > self.time)):
                    break
                await asyncio.sleep(0)

    yield Clocker(event_loop).advance
