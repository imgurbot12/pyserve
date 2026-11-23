"""
PyServe Async Transport UnitTests
"""
from typing import Optional, Type, Union
from unittest import IsolatedAsyncioTestCase

from . import EchoSession, AsyncEchoSession
from ..abc import SessFactory
from ..client import TcpAsyncClient, UdpAsyncClient
from ..transport import TcpAsyncServer, UdpAsyncServer

#** Variables **#
__all__ = ['TransportTests']

Client = Union[TcpAsyncClient, UdpAsyncClient]
Server = Union[UdpAsyncServer, TcpAsyncServer]

#** Classes **#

class TransportTests(IsolatedAsyncioTestCase):
    """
    """
    server: Server
    client: Client

    async def asyncTearDown(self) -> None:
        client: Optional[Client] = getattr(self, 'client', None)
        server: Optional[Server] = getattr(self, 'server', None)
        if client is not None:
            setattr(self, 'client', None)
            client.close()
        if server is not None:
            setattr(self, 'server', None)
            server.stop()

    async def assertRespond(self, req: bytes, res: bytes):
        """
        """
        await self.client.send(req)
        read = await self.client.recv(8192)
        self.assertLess(len(req), 8192)
        self.assertEqual(read, res)

    async def assertEcho(self, echo: bytes):
        """
        """
        await self.assertRespond(echo, echo)

    async def new_server(self,
        scls:        Type[Server],
        ccls:        Type[Client],
        factory:     SessFactory,
        force_dgram: bool = False
    ):
        self.server = scls(('127.0.0.1', 0), factory)
        self.client = ccls() \
            if issubclass(ccls, TcpAsyncClient) else \
            ccls(force_dgram)
        await self.server.start()
        await self.client.connect(self.server.sockname())

    async def test_udp_echo(self):
        """
        """
        await self.new_server(
            UdpAsyncServer, UdpAsyncClient,
            SessFactory(EchoSession), True)
        await self.assertEcho(b'hello')
        await self.assertEcho(b'world')
        await self.assertEcho(b'ayylmao')
        await self.assertEcho(b'\x00' * 8191)
        await self.assertEcho(b'bye')

    async def test_tcp_echo(self):
        """
        """
        await self.new_server(
            TcpAsyncServer, TcpAsyncClient, SessFactory(EchoSession))
        await self.assertEcho(b'hello')
        await self.assertEcho(b'world')
        await self.assertEcho(b'ayylmao')
        await self.assertEcho(b'\x00' * 8191)
        await self.assertEcho(b'bye')
        await self.assertRespond(b'test', b'')

    async def test_udp_echo_async(self):
        """
        """
        await self.new_server(
            UdpAsyncServer, UdpAsyncClient, SessFactory(AsyncEchoSession))
        await self.assertEcho(b'hello')
        await self.assertEcho(b'world')
        await self.assertEcho(b'ayylmao')
        await self.assertEcho(b'\x00' * 8191)
        await self.assertEcho(b'bye')

    async def test_tcp_echo_async(self):
        """
        """
        await self.new_server(
            TcpAsyncServer, TcpAsyncClient,
            SessFactory(AsyncEchoSession), True)
        await self.assertEcho(b'hello')
        await self.assertEcho(b'world')
        await self.assertEcho(b'ayylmao')
        await self.assertEcho(b'\x00' * 8191)
        await self.assertEcho(b'bye')
        await self.assertRespond(b'test', b'')

