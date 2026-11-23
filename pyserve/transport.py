"""
PyServe single-threaded Async implementations of session-based servers
"""
import socket
import asyncio
from ssl import SSLContext
from typing import (
    Any, Callable, ClassVar, Coroutine, Generic, Optional,
    Tuple, TypeVar, Union, cast)
from typing_extensions import Self

from .abc import (
    Address, AnyAddr, AsyncSession, BaseWriter, RawAddr, SessFactory, 
    Session, UdpWriter, Writer, modify_socket)

#** Variables **#
__all__ = ['UdpAsyncServer', 'TcpAsyncServer']

TcpSession = Union[Session[Writer], AsyncSession[Writer]]
UdpSession = Union[Session[UdpWriter], AsyncSession[UdpWriter]]

T    = TypeVar('T', bound=Union[asyncio.DatagramTransport, asyncio.Transport])
TcpS = TypeVar('TcpS', bound=TcpSession)
UdpS = TypeVar('UdpS', bound=UdpSession)

#** Functions **#

def wrap_ssl(
    ctx: SSLContext,
    transport: asyncio.Transport,
    callback:  Callable[[asyncio.Transport], None],
):
    """
    """
    loop   = asyncio.get_event_loop()
    future = loop.start_tls(
        transport=transport,
        protocol=transport.get_protocol(),
        sslcontext=ctx,
        server_side=True,
    )
    def ssl_callback(task: asyncio.Task):
        result: Optional[asyncio.Transport] = task.result()
        if result is not None:
            callback(result)
    task = loop.create_task(future)
    task.add_done_callback(ssl_callback)

#** Classes **#

class AsyncWriter(BaseWriter, Generic[T]):
    """
    Base-class for async socket writers
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
    UDP implementation for async socket writer
    """

    def write(self, data: bytes, addr: Optional[AnyAddr] = None) -> None:
        self.transport.sendto(data, addr or self.addr)

class TcpAsyncWriter(Writer, AsyncWriter[asyncio.Transport]):
    """
    TCP implementation for async socket writer
    """

    def start_tls(self, context: SSLContext):
        def callback(transport: asyncio.Transport):
            self.transport = transport
        wrap_ssl(context, self.transport, callback)

    def write(self, data: bytes):
        self.transport.write(data)

class BaseProtocol(Generic[T]):
    """
    Base-class async request handler implementation for UDP/TCP
    """
    factory: SessFactory

    def __init__(self, factory: SessFactory):
        self.factory = factory

    def set_timeout(self, timeout: float, tport: T):
        """
        Implement timeout handling for async servers
        """
        async def expire():
            await asyncio.sleep(timeout)
            if not tport.is_closing():
                tport.abort()
        loop = asyncio.get_event_loop()
        loop.create_task(expire())

class UdpProtocol(BaseProtocol, asyncio.DatagramProtocol):
    """
    UDP async server protocol handler
    """
    __slots__ = ('factory', 'session', 'transport', )

    factory:   SessFactory
    session:   UdpSession
    transport: asyncio.DatagramTransport

    def __init__(self, factory: SessFactory[UdpS]):
        self.factory = factory

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.DatagramTransport, transport)

    def datagram_received(self, data: bytes, addr: RawAddr) -> None:
        address      = Address(*addr)
        writer       = UdpAsyncWriter(address, self.transport)
        self.session = self.factory.build()
        task         = self.session._datagram_recieved(address, writer, data)
        if self.factory.is_async:
            asyncio.create_task(cast(Coroutine, task))

    def connection_lost(self, exc: Optional[Exception]) -> None:
        task = self.session.connection_lost(exc)
        if self.factory.is_async:
            asyncio.create_task(task)

class TcpProtocol(BaseProtocol, asyncio.Protocol):
    """
    TCP async server protocol handler
    """
    __slots__ = ('factory', 'session', 'timeout')

    session: TcpSession
    timeout: Optional[float]

    def __init__(self, factory: SessFactory[TcpS], timeout: Optional[float] = None):
        self.factory = factory
        self.timeout = timeout

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        addr      = transport.get_extra_info('peername')
        address   = Address(*addr)
        transport = cast(asyncio.Transport, transport)
        writer    = TcpAsyncWriter(address, transport)
        if isinstance(transport, asyncio.Transport) \
            and self.timeout is not None:
            self.set_timeout(self.timeout, transport)
        self.session = self.factory.build()
        task         = self.session.connection_made(address, writer)
        if self.factory.is_async:
            asyncio.create_task(task)

    def data_received(self, data: bytes) -> None:
        task = self.session.data_received(data)
        if self.factory.is_async:
            asyncio.create_task(task)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        task = self.session.connection_lost(exc)
        if self.factory.is_async:
            asyncio.create_task(task)

class UdpAsyncServer:
    """
    UDP async session-based server implementation
    """
    __slots__ = ('address', 'interface', 'handler',
        'allow_broadcast', 'allow_reuse_port', '_event', '_server')

    address_family: ClassVar[int] = socket.AF_INET
    socket_type:    ClassVar[int] = socket.SOCK_DGRAM

    address:          RawAddr
    interface:        Optional[str]
    handler:          Callable[[], UdpProtocol]
    allow_broadcast:  bool
    allow_reuse_port: bool

    _event:  asyncio.Event
    _server: Optional[Tuple[asyncio.DatagramTransport, UdpProtocol]]

    def __init__(self,
        address:         RawAddr,
        factory:         SessFactory,
        interface:       Optional[str] = None,
        reuse_port:      bool          = False,
        allow_broadcast: bool          = False,
    ):
        self.address          = address
        self.handler          = lambda: UdpProtocol(factory)
        self.interface        = interface
        self.allow_broadcast  = allow_broadcast
        self.allow_reuse_port = reuse_port
        self._event           = asyncio.Event()
        self._server          = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_):
        self.stop()

    def sockname(self) -> RawAddr:
        """
        Retrieve bind address from inner server socket
        """
        if self._server is None:
            raise RuntimeError('Server is not running')
        return self._server[0].get_extra_info('sockname')

    async def start(self) -> None:
        """
        Bind and activate server (if not already)
        """
        if self._server is not None:
            return
        self._event.clear()
        loop         = asyncio.get_running_loop()
        tport, proto = await loop.create_datagram_endpoint(
            self.handler, self.address,
            reuse_port=self.allow_reuse_port,
            allow_broadcast=self.allow_broadcast)
        if self.interface is not None:
            sock = proto.transport.get_extra_info('socket')
            modify_socket(sock, None, self.interface)
        self._server = (tport, proto)

    def stop(self) -> None:
        """
        Shut-down serve_forever loop and close transport
        """
        if self._server is None:
            return
        self._event.set()
        tport, _ = self._server
        tport.close()
        self._server = None

    async def serve_forever(self) -> None:
        """
        Spawn async server and wait forever (or until stopped)
        """
        await self.start()
        try:
            await self._event.wait()
        finally:
            self.stop()

class TcpAsyncServer:
    """
    TCP async session-based server implementation
    """
    __slots__ = ('address', 'interface', 'handler', 'backlog', 'ssl',
        'allow_reuse_port', '_server')

    address_family: ClassVar[int] = socket.AF_INET
    socket_type:    ClassVar[int] = socket.SOCK_STREAM

    address:          RawAddr
    interface:        Optional[str]
    handler:          Callable[[], TcpProtocol]
    backlog:          int
    ssl:              Optional[SSLContext]
    allow_reuse_port: bool

    _server: Optional[Any]

    def __init__(self,
        address:    RawAddr,
        factory:    SessFactory[TcpSession],
        interface:  Optional[str]        = None,
        reuse_port: bool                 = False,
        backlog:    int                  = 100,
        ssl:        Optional[SSLContext] = None,
        timeout:    Optional[int]        = None,
    ):
        self.address          = address
        self.handler          = lambda: TcpProtocol(factory, timeout)
        self.interface        = interface
        self.backlog          = backlog
        self.ssl              = ssl
        self.allow_reuse_port = reuse_port
        self._server          = None

    async def __aenter__(self) -> Self:
        server = await self._start()
        await server.__aenter__()
        return self

    async def __aexit__(self, *args):
        if self._server is not None:
            await self._server.__aexit__(*args)
        self.stop()

    def sockname(self):
        """
        Retrieve bind address from inner server socket
        """
        if self._server is None:
            raise RuntimeError('Server is not running')
        return self._server.sockets[0].getsockname()

    async def _start(self) -> Any:
        """
        Spawn async TCP server instance or return running one
        """
        if self._server is not None:
            return self._server
        loop       = asyncio.get_running_loop()
        host, port = self.address
        server     = await loop.create_server(self.handler, host, port,
            reuse_port=self.allow_reuse_port,
            backlog=self.backlog,
            ssl=self.ssl)
        if self.interface is not None:
            for sock in server.sockets:
                modify_socket(sock, None, self.interface)
        await server.start_serving()
        self._server = server
        return server

    async def start(self) -> None:
        """
        Bind and activate server (if not already)
        """
        await self._start()

    def stop(self) -> None:
        """
        Shut-down serve_forever loop and close transport
        """
        if self._server is None:
            return
        self._server.close()
        self._server = None

    async def serve_forever(self) -> None:
        """
        Spawn async server and wait forever (or until stopped)
        """
        server = await self._start()
        await server.serve_forever()
