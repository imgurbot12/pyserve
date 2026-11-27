"""
PyServe Utils UnitTests
"""
import time
import asyncio
from unittest import IsolatedAsyncioTestCase

from ..utils import FlightSrc, SingleFlight

#** Variables **#
__all__ = ['UtilTests']

#** Classes **#

class UtilTests(IsolatedAsyncioTestCase):

    async def test_singleflight(self):
        """
        Ensure single-flight utility works as intended
        """
        async def task():
            await asyncio.sleep(0.1)
            return type('new_type', (), {})
        flight  = SingleFlight()
        start   = time.time()
        results = await asyncio.gather(
            flight.do_labeled('key', task()),
            flight.do_labeled('key', task()),
            flight.do_labeled('key', task()),
        )
        diff = time.time() - start
        self.assertLess(diff, 0.2)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0][1], FlightSrc.LEADER)
        self.assertEqual(results[1][1], FlightSrc.FOLLOWER)
        self.assertEqual(results[2][1], FlightSrc.FOLLOWER)
        self.assertIs(results[0][0], results[1][0])
        self.assertIs(results[1][0], results[2][0])
