"""
PyServe Server Utilities
"""
import asyncio
from enum import Enum
from typing import Coroutine, Dict, Optional, Tuple, TypeVar

#** Variables **#
__all__ = ['FlightSrc', 'SingleFlight']

T = TypeVar('T')

#** Classes **#

class FlightSrc(int, Enum):
    """
    Labels Single-Flight result based on source
    """
    LEADER   = 0
    FOLLOWER = 1
    RETRIED  = 2

class SingleFlight:
    """
    Single-Flight Pattern (https://go-patterns.dev/sync/singleflight)

    Reduces overhead when doing shared work. Instead, it links the same
    task to those requesting it and duplicates the result to each requester.
    """
    __slots__ = ('cache', 'lock')

    def __init__(self):
        self.cache: Dict[str, asyncio.Future] = {}
        self.lock = asyncio.Lock()

    async def _wait(self,
        call:    Coroutine[None, None, T],
        future:  asyncio.Future,
        timeout: Optional[float] = None,
    ) -> Tuple[T, FlightSrc]:
        """
        Wait for task to complete with optional timeout
        Should the timeout be reached, it results to spawning its own task
        """
        task = future if timeout is None else asyncio.wait_for(future, timeout)
        try:
            result = await task
            call.close() # cancel coroutine
            return result, FlightSrc.FOLLOWER
        except asyncio.TimeoutError:
            return await call, FlightSrc.RETRIED

    async def do_labeled(self,
        key:     str,
        call:    Coroutine[None, None, T],
        timeout: Optional[float] = None
    ) -> Tuple[T, FlightSrc]:
        """
        Spawn single-flight task or wait for an existing task to complete.

        Uses the key to determine if a job already exists and manage the cache.
        The timeout only applies for secondary tasks waiting on leader.
        """
        async with self.lock:
            future = self.cache.get(key)
            if future is None:
                leader = True
                future = asyncio.Future()
                self.cache[key] = future
            else:
                leader = False

        if not leader:
            return await self._wait(call, future, timeout)
        try:
            result = await call
            future.set_result(result)
        except BaseException as exc:
            future.set_exception(exc)
            raise exc
        finally:
            async with self.lock:
                self.cache.pop(key, None)
        return result, FlightSrc.LEADER

    async def do(self,
        key:     str,
        call:    Coroutine[None, None, T],
        timeout: Optional[float] = None
    ) -> T:
        """
        Spawn single-flight task or wait for an existing task to complete.

        Uses the key to determine if a job already exists and manage the cache.
        The timeout only applies for secondary tasks waiting on leader.
        """
        result, _ = await self.do_labeled(key, call, timeout)
        return result
