"""
PyServe Threading Server UnitTests
"""
import json
import socket
import threading
from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union
from unittest import TestCase

from . import ArgsSession, EchoSession, random_port
from ..abc import RawAddr, Session
from ..threading import TcpThreadServer, UdpThreadServer

#** Variables **#
__all__ = ['UdpThreadTests', 'TcpThreadTests']

Server = Union[UdpThreadServer, TcpThreadServer]
S      = TypeVar('S', bound=Server)

#** Classes **#

class ThreadTests(TestCase, Generic[S]):
    """
    Baseclass UnitTest implementation for threading servers
    """
    __test__ = False

    server_cls: Type[S]

    addr:   RawAddr
    server: Server
    client: socket.socket
    thread: threading.Thread

    def spawn_threaded(self,
        addr:    RawAddr,
        factory: Type[Session],
        args:    tuple          = (),
        kwargs:  Dict[str, Any] = {},
        **kw,
    ) -> S:
        server      = self.server_cls(addr, factory, args, kwargs, **kw)
        self.addr   = addr
        self.thread = threading.Thread(target=server.serve_forever)
        self.thread.daemon = True
        self.thread.start()
        self.client = socket.socket(server.address_family, server.socket_type)
        self.client.settimeout(0.25)
        if isinstance(server, TcpThreadServer):
            self.client.connect(addr)
        return server

    def tearDown(self):
        client: Optional[socket.socket] = getattr(self, 'client', None)
        server: Optional[S] = getattr(self, 'server', None)
        thread: Optional[threading.Thread] = getattr(self, 'thread', None)
        if client is not None:
            setattr(self, 'client', None)
            client.close()
        if server is not None:
            setattr(self, 'server', None)
            server.stop()
        if thread:
            setattr(self, 'thread', None)

    def assertRespond(self, req: bytes, res: bytes):
        """
        """
        written = self.client.sendto(req, self.addr)
        read    = self.client.recv(8192)
        self.assertLess(len(req), 8192)
        self.assertEqual(written, len(req))
        self.assertEqual(read, res)

    def assertEcho(self, echo: bytes):
        """
        """
        self.assertRespond(echo, echo)

    def test_echo(self):
        self.spawn_threaded(
            addr=('127.0.0.1', random_port()), 
            factory=EchoSession)

        self.assertEcho(b'hello')
        self.assertEcho(b'world')
        self.assertEcho(b'ayylmao')
        self.assertEcho(b'\x00' * 8191)
        self.assertEcho(b'bye')

    def test_args(self):
        self.spawn_threaded(
            addr=('127.0.0.1', random_port()),
            factory=ArgsSession,
            args=(1, 2, 3),
            kwargs={'a': 'A', 'b': 'B'})

        self.assertRespond(b'hello', json.dumps({
            'args':   [1, 2, 3],
            'kwargs': {'a': 'A', 'b': 'B'},
            'data':   'hello',
        }).encode())

class UdpThreadTests(ThreadTests[UdpThreadServer]):
    __test__   = True
    server_cls = UdpThreadServer

    def test_closed(self):
        self.spawn_threaded(
            addr=('127.0.0.1', random_port()),
            factory=EchoSession)

        with self.assertRaises(socket.timeout):
            self.assertEcho(b'bye')
            self.assertEcho(b'test')

class TcpThreadTests(ThreadTests[TcpThreadServer]):
    __test__   = True
    server_cls = TcpThreadServer

    def test_closed(self):
        self.spawn_threaded(
            addr=('127.0.0.1', random_port()),
            factory=EchoSession)

        self.assertEcho(b'bye')
        try:
            self.assertRespond(b'test', b'')
        except ConnectionResetError:
            pass
