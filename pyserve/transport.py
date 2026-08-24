"""
PyServe Async implementations of session-based servers
"""
import asyncio
import socket
from ssl import SSLContext
from typing import (
    Any, Callable, ClassVar, Dict, Generic, Optional, Tuple, Type, 
    TypeVar, Union, cast)
from typing_extensions import Self

from .abc import (
    Address, AnyAddr, RawAddr, Session, UdpWriter, Writer, modify_socket)

#** Variables **#
__all__ = [
    'UdpAsyncProtocol', 
    'TcpAsyncProtocol', 

    'UdpAsyncServer', 
    'TcpAsyncServer'
]

T = TypeVar('T', bound=Union[asyncio.DatagramTransport, asyncio.Transport])

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
        loop   = asyncio.get_event_loop()
        future = loop.start_tls(
            transport=self.transport,
            protocol=self.transport.get_protocol(),
            sslcontext=context,
            server_side=True,
        )

        def callback(task: asyncio.Task):
            self.transport = task.result()
        task = loop.create_task(future)
        task.add_done_callback(callback)

    def write(self, data: bytes) -> int:
        self.transport.write(data)
        return len(data)

class BaseProtocol(Generic[T]):
    """
    Baseclass async request handler implementation for UDP/TCP
    """
    __slots__ = ('factory', 'args', 'kwargs', 'session')

    factory: Type[Session]
    args:    tuple
    kwargs:  Dict[str, Any]
    session: Session

    def __init__(self,
        factory: Type[Session],
        args:    Optional[tuple] = None,
        kwargs:  Optional[Dict[str, Any]] = None,
    ):
        self.factory = factory
        self.args    = args or ()
        self.kwargs  = kwargs or {}
        # test factory to ensure it initializes with args/kwargs
        self._new_session()

    def _new_session(self) -> Session:
        """
        spawn new session instance from factory
        """
        return self.factory(*self.args, **self.kwargs)

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

class UdpAsyncProtocol(BaseProtocol, asyncio.DatagramProtocol):
    """
    Udp async server protocol handler
    """

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: RawAddr) -> None:
        address      = Address(*addr)
        writer       = UdpAsyncWriter(address, self.transport)
        self.session = self._new_session()
        self.session.connection_made(address, writer)
        self.session.data_recieved(data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.session.connection_lost(exc)

class TcpAsyncProtocol(BaseProtocol, asyncio.Protocol):
    """
    Tcp async server protocol handler
    """
    __slots__ = ('factory', 'args', 'kwargs', 'session', 'timeout')

    timeout: Optional[float]

    def __init__(self,
        factory: Type[Session],
        args:    Optional[tuple]          = None,
        kwargs:  Optional[Dict[str, Any]] = None,
        timeout: Optional[float]          = None,
    ):
        super().__init__(factory, args, kwargs)
        self.timeout = timeout

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        transport = cast(asyncio.Transport, transport)
        addr      = transport.get_extra_info('peername')
        address   = Address(*addr)
        if isinstance(transport, asyncio.Transport) \
            and self.timeout is not None:
            self.set_timeout(self.timeout, transport)
        writer       = TcpAsyncWriter(address, transport)
        self.session = self._new_session()
        self.session.connection_made(address, writer)

    def data_received(self, data: bytes) -> None:
        self.session.data_recieved(data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.session.connection_lost(exc)

class UdpAsyncServer:
    """
    Udp async session-based server implementation
    """
    __slots__ = ('address', 'interface', 'handler', 
        'allow_broadcast', 'allow_reuse_port', '_event', '_server')

    address_family: ClassVar[int] = socket.AF_INET
    socket_type:    ClassVar[int] = socket.SOCK_DGRAM

    address:          RawAddr
    interface:        Optional[str]
    handler:          Callable[[], UdpAsyncProtocol]
    allow_broadcast:  bool
    allow_reuse_port: bool

    _event:  asyncio.Event
    _server: Optional[Tuple[asyncio.DatagramTransport, UdpAsyncProtocol]]

    def __init__(self,
        address:         RawAddr,
        factory:         Type[Session],
        args:            Optional[tuple]          = None,
        kwargs:          Optional[Dict[str, Any]] = None,
        interface:       Optional[str]            = None,
        reuse_port:      bool                     = False,
        allow_broadcast: bool                     = False,
    ):
        self.address          = address
        self.handler          = lambda: UdpAsyncProtocol(factory, args, kwargs)
        self.interface        = interface
        self.allow_broadcast  = allow_broadcast
        self.allow_reuse_port = reuse_port
        self._event           = asyncio.Event()
        self._server          = None
        self.handler() # test that initialization works as intended

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_):
        self.stop()

    async def start(self) -> None:
        """
        bind and activate server (if not already)
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
        shutdown serve_forever loop and close transport
        """
        if self._server is None:
            return
        self._event.set()
        tport, _ = self._server
        tport.close()
        self._server = None

    async def serve_forever(self) -> None:
        """
        spawn async server and wait forever (or until stopped)
        """
        if self._server is None:
            return
        try:
            await self._event.wait()
        finally:
            self.stop()

class TcpAsyncServer:
    """
    Tcp async session-based server implementation
    """
    __slots__ = ('address', 'interface', 'handler', 'backlog', 'ssl',
        'allow_reuse_port', '_server')

    address_family: ClassVar[int] = socket.AF_INET
    socket_type:    ClassVar[int] = socket.SOCK_STREAM

    address:          RawAddr
    interface:        Optional[str]
    handler:          Callable[[], TcpAsyncProtocol]
    backlog:          int
    ssl:              Optional[SSLContext]
    allow_reuse_port: bool

    _server: Optional['asyncio.Server']

    def __init__(self,
        address:    RawAddr,
        factory:    Type[Session],
        args:       Optional[tuple]          = None,
        kwargs:     Optional[Dict[str, Any]] = None,
        interface:  Optional[str]            = None,
        reuse_port: bool                     = False,
        backlog:    int                      = 100,
        ssl:        Optional[SSLContext]     = None,
        timeout:    Optional[int]            = None,
    ):
        self.address          = address
        self.handler          = lambda: TcpAsyncProtocol(factory, args, kwargs, timeout)
        self.interface        = interface
        self.backlog          = backlog
        self.ssl              = ssl
        self.allow_reuse_port = reuse_port
        self._server          = None
        self.handler() # test that initialization works as intended

    async def __aenter__(self) -> Self:
        server = await self._start()
        await server.__aenter__()
        return self

    async def __aexit__(self, *args):
        if self._server is not None:
            await self._server.__aexit__(*args)
        self.stop()

    async def _start(self) -> 'asyncio.Server':
        """
        spawn asyncio tcp server instance or return running one
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
        bind and activate server (if not already)
        """
        await self._start()

    def stop(self) -> None:
        """
        shutdown serve_forever loop and close transport
        """
        if self._server is None:
            return
        self._server.close()
        self._server = None

    async def serve_forever(self) -> None:
        """
        spawn async server and wait forever (or until stopped)
        """
        server = await self._start()
        await server.serve_forever()
