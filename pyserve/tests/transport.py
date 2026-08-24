"""
PyServe Async Transport Server UnitTests
"""
import asyncio
import json
import socket
from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union, cast
from unittest import IsolatedAsyncioTestCase

from . import ArgsSession, EchoSession, random_port
from ..abc import RawAddr, Session
from ..transport import TcpAsyncServer, UdpAsyncServer

#** Variables **#
__all__ = ['UdpAsyncTests', 'TcpAsyncTests']

Server = Union[UdpAsyncServer, TcpAsyncServer]
S      = TypeVar('S', bound=Server)

#** Classes **#

class AsyncTests(IsolatedAsyncioTestCase, Generic[S]):
    """
    Baseclass UnitTest implementation for async servers
    """
    __test__ = False

    server_cls: Type[S]

    addr:   RawAddr
    server: Server
    client: socket.socket

    async def spawn_async(self,
        addr:    RawAddr,
        factory: Type[Session],
        args:    tuple          = (),
        kwargs:  Dict[str, Any] = {},
        **kw,
    ) -> S:
        server = self.server_cls(addr, factory, args, kwargs, **kw)
        await server.start()
        self.addr   = addr
        self.server = server
        self.client = socket.socket(server.address_family, server.socket_type)
        self.client.setblocking(False)
        if isinstance(server, TcpAsyncServer):
            loop = asyncio.get_running_loop()
            await loop.sock_connect(self.client, addr)
        return cast(S, server)

    async def asyncSetUp(self) -> None:
        asyncio.get_running_loop().set_debug(False)

    async def asyncTearDown(self) -> None:
        client: Optional[socket.socket] = getattr(self, 'client', None)
        server: Optional[S] = getattr(self, 'server', None)
        if client is not None:
            setattr(self, 'client', None)
            client.close()
        if server is not None:
            setattr(self, 'server', None)
            server.stop()

    async def _recv(self, size: int) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.sock_recv(self.client, size)

    async def assertRespond(self, req: bytes, res: bytes):
        """
        """
        written = self.client.sendto(req, self.addr)
        read    = await self._recv(8192)
        self.assertLess(len(req), 8192)
        self.assertEqual(written, len(req))
        self.assertEqual(read, res)

    async def assertEcho(self, echo: bytes):
        """
        """
        await self.assertRespond(echo, echo)

    async def test_echo(self):
        await self.spawn_async(
            addr=('127.0.0.1', random_port()),
            factory=EchoSession)

        await self.assertEcho(b'hello')
        await self.assertEcho(b'world')
        await self.assertEcho(b'ayylmao')
        await self.assertEcho(b'\x00' * 8191)
        await self.assertEcho(b'bye')

    async def test_args(self):
        await self.spawn_async(
            addr=('127.0.0.1', random_port()),
            factory=ArgsSession,
            args=(1, 2, 3),
            kwargs={'a': 'A', 'b': 'B'})

        await self.assertRespond(b'hello', json.dumps({
            'args':   [1, 2, 3],
            'kwargs': {'a': 'A', 'b': 'B'},
            'data':   'hello',
        }).encode())

class UdpAsyncTests(AsyncTests[UdpAsyncServer]):
    __test__   = True
    server_cls = UdpAsyncServer

    async def test_closed(self):
        await self.spawn_async(
            addr=('127.0.0.1', random_port()),
            factory=EchoSession)

        self.client.settimeout(0.25)
        with self.assertRaises(socket.timeout):
            await self.assertEcho(b'bye')
            await self.assertEcho(b'test')

class TcpAsyncTests(AsyncTests[TcpAsyncServer]):
    __test__   = True
    server_cls = TcpAsyncServer

    async def test_closed(self):
        await self.spawn_async(
            addr=('127.0.0.1', random_port()),
            factory=EchoSession)

        await self.assertEcho(b'bye')
        try:
            await self.assertRespond(b'test', b'')
        except ConnectionResetError:
            pass
