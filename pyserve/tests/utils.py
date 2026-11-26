"""
PyServe Utils UnitTests
"""
import time
import asyncio
from unittest import IsolatedAsyncioTestCase

from ..utils import SingleFlight

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
            flight.do('key', task()),
            flight.do('key', task()),
            flight.do('key', task()),
        )
        diff = time.time() - start
        self.assertLess(diff, 0.2)
        self.assertEqual(len(results), 3)
        self.assertIs(results[0], results[1])
        self.assertIs(results[1], results[2])
