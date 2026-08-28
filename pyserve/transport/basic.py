"""
PyServe single-threaded Async implementations of session-based servers
"""
import asyncio
from ssl import SSLContext
from typing import Generic, Optional, cast

from . import T, wrap_ssl
from ..abc import (
    Address, AnyAddr, RawAddr, SessFactory, Session, UdpWriter, Writer)

#** Variables **#
__all__ = ['UdpProtocol', 'TcpProtocol']

#** Classes **#

class AsyncWriter(Writer, Generic[T]):
    """
    Baseclass for async socket writers
    """
    __slots__ = ('addr', 'transport')

    transport: T

    def __init__(self, addr: Address, transport: T):
        self.addr      = addr
        self.transport = transport

    def using_tls(self) -> bool:
        return 'ssl' in self.transport.__class__.__name__.lower()

    def close(self):
        self.transport.close()

    def is_closing(self) -> bool:
        return self.transport.is_closing()

class UdpAsyncWriter(UdpWriter, AsyncWriter[asyncio.DatagramTransport]):
    """
    Udp implementation for async socket writer
    """

    def write(self, data: bytes, addr: Optional[AnyAddr] = None) -> int:
        self.transport.sendto(data, addr or self.addr)
        return len(data)

class TcpAsyncWriter(AsyncWriter[asyncio.Transport]):
    """
    Tcp implementation for async socket writer
    """

    def start_tls(self, context: SSLContext):
        def callback(transport: asyncio.Transport):
            self.transport = transport
        wrap_ssl(context, self.transport, callback)

    def write(self, data: bytes) -> int:
        self.transport.write(data)
        return len(data)

class BaseProtocol(Generic[T]):
    """
    Baseclass async request handler implementation for UDP/TCP
    """
    __slots__ = ('factory', 'args', 'kwargs', 'session')

    factory: SessFactory
    session: Session

    def __init__(self, factory: SessFactory):
        self.factory = factory

    def set_timeout(self, timeout: float, tport: T):
        """
        implement timeout handling for async servers
        """
        async def expire():
            await asyncio.sleep(timeout)
            if not tport.is_closing():
                tport.abort()
        loop = asyncio.get_event_loop()
        loop.create_task(expire())

class UdpProtocol(BaseProtocol, asyncio.DatagramProtocol):
    """
    Udp async server protocol handler
    """
    transport: asyncio.DatagramTransport

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.DatagramTransport, transport)

    def datagram_received(self, data: bytes, addr: RawAddr) -> None:
        address      = Address(*addr)
        writer       = UdpAsyncWriter(address, self.transport)
        self.session = self.factory.build()
        self.session.connection_made(address, writer)
        self.session.data_received(data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.session.connection_lost(exc)

class TcpProtocol(BaseProtocol, asyncio.Protocol):
    """
    Tcp async server protocol handler
    """
    __slots__ = ('factory', 'args', 'kwargs', 'session', 'timeout')

    timeout: Optional[float]

    def __init__(self, factory: SessFactory, timeout: Optional[float] = None):
        super().__init__(factory)
        self.timeout = timeout

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        transport = cast(asyncio.Transport, transport)
        addr      = transport.get_extra_info('peername')
        address   = Address(*addr)
        if isinstance(transport, asyncio.Transport) \
            and self.timeout is not None:
            self.set_timeout(self.timeout, transport)
        writer       = TcpAsyncWriter(address, transport)
        self.session = self.factory.build()
        self.session.connection_made(address, writer)

    def data_received(self, data: bytes) -> None:
        self.session.data_received(data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.session.connection_lost(exc)
