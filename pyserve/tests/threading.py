"""
PyServe Threaded Transport UnitTests
"""
import socket
import threading
from typing import Optional, Type, Union
from unittest import TestCase

from . import EchoSession
from ..abc import SessFactory
from ..threading import TcpThreadServer, UdpThreadServer

#** Variables **#
__all__ = ['ThreadingTests']

Server = Union[TcpThreadServer, UdpThreadServer]

#** Classes **#

class ThreadingTests(TestCase):
    """
    """
    thread: threading.Thread
    server: Server
    client: socket.socket

    def tearDown(self) -> None:
        setattr(self, 'thread', None)
        client: Optional[socket.socket] = getattr(self, 'client', None)
        server: Optional[Server] = getattr(self, 'server', None)
        if client is not None:
            setattr(self, 'client', None)
            client.close()
        if server is not None:
            setattr(self, 'server', None)
            server.stop()

    def assertRespond(self, req: bytes, res: bytes):
        """
        """
        self.client.send(req)
        read = self.client.recv(8192)
        self.assertLess(len(req), 8192)
        self.assertEqual(read, res)

    def assertEcho(self, echo: bytes):
        """
        """
        self.assertRespond(echo, echo)

    def new_server(self,scls: Type[Server], factory: SessFactory):
        server = scls(('127.0.0.1', 0), factory)
        self.client = socket.socket(server.address_family, server.socket_type)
        self.client.settimeout(0.25)
        self.client.connect(server.sockname())

        self.server = server
        self.thread = threading.Thread(target=server.serve_forever)
        self.thread.daemon = True
        self.thread.start()

    def test_udp_echo(self):
        """
        """
        self.new_server(UdpThreadServer, SessFactory(EchoSession))
        self.assertEcho(b'hello')
        self.assertEcho(b'world')
        self.assertEcho(b'ayylmao')
        self.assertEcho(b'\x00' * 8191)
        self.assertEcho(b'bye')

    def test_tcp_echo(self):
        """
        """
        self.new_server(TcpThreadServer, SessFactory(EchoSession))
        self.assertEcho(b'hello')
        self.assertEcho(b'world')
        self.assertEcho(b'ayylmao')
        self.assertEcho(b'\x00' * 8191)
        self.assertEcho(b'bye')
        self.assertRespond(b'test', b'')
