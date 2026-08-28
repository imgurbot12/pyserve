"""

"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import socket
from ssl import SSLContext
from typing import Any, Callable, ClassVar, Dict, Optional, Tuple, Type, TypeVar, Union
from typing_extensions import Self

from ..abc import RawAddr, SessFactory, Session, modify_socket

#** Variables **#

T = TypeVar('T', bound=Union[asyncio.DatagramTransport, asyncio.Transport])

UdpP = Union['UdpProtocol', 'UdpThreadedProtocol']
TcpP = Union['TcpProtocol', 'TcpThreadedProtocol']

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
    handler:          Callable[[], UdpP]
    allow_broadcast:  bool
    allow_reuse_port: bool

    _event:  asyncio.Event
    _server: Optional[Tuple[asyncio.DatagramTransport, UdpP]]

    def __init__(self,
        address:         RawAddr,
        factory:         Type[Session],
        args:            Optional[Tuple[Any, ...]] = None,
        kwargs:          Optional[Dict[str, Any]]  = None,
        interface:       Optional[str]             = None,
        reuse_port:      bool                      = False,
        allow_broadcast: bool                      = False,
        threads:         Optional[int]             = None,
    ):
        sessfact = SessFactory(factory, args or (), kwargs or {})
        if threads is not None:
            threads = threads if threads is not None and threads > 0 else None
            pool    = ThreadPoolExecutor(threads)
            handler = lambda: UdpThreadedProtocol(sessfact, pool)
        else:
            handler = lambda: UdpProtocol(sessfact)
        self.address          = address
        self.handler          = handler
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
    handler:          Callable[[], TcpP]
    backlog:          int
    ssl:              Optional[SSLContext]
    allow_reuse_port: bool

    _server: Optional[Any]

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
        threads:    Optional[int]            = None,
    ):
        sessfact = SessFactory(factory, args or (), kwargs or {})
        if threads is not None:
            threads = threads if threads is not None and threads > 0 else None
            pool    = ThreadPoolExecutor(threads)
            handler = lambda: TcpThreadedProtocol(sessfact, pool, timeout)
        else:
            handler = lambda: TcpProtocol(sessfact, timeout)
        self.address          = address
        self.handler          = handler
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

    async def _start(self) -> Any:
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

#** Imports **#
from .basic import UdpProtocol, TcpProtocol
from .threaded import UdpThreadedProtocol, TcpThreadedProtocol
