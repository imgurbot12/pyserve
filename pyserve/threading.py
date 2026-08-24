"""
PyServe Threading implementations of session-based servers
"""
import socket
import socketserver
from ssl import SSLContext, SSLSocket
from typing import (
    Any, Callable, ClassVar, Dict, Generic, Optional, Type, TypeVar, cast)
from typing_extensions import override

from .abc import (
    Address, AnyAddr, RawAddr, Session, UdpWriter, Writer, modify_socket)

#** Variables **#
__all__ = ['UdpThreadServer', 'TcpThreadServer']

W = TypeVar('W', bound=Writer)
H = TypeVar('H', bound='RequestHandler')

#** Functions **#

def new_handler(
    base:      Type[H],
    factory:   Type[Session],
    args:      Optional[tuple]          = None,
    kwargs:    Optional[Dict[str, Any]] = None,
    blocksize: int                      = 8192
) -> Type[H]:
    """
    spawn new request-handler class w/ configured settings
    """
    # test factory generation
    args   = args or ()
    kwargs = kwargs or {}
    factory(*args, **kwargs)
    # generate new request handler
    name   = f'{base.__name__}Instance'
    newcls = type(name, (base, ), dict(
        factory=factory,
        args=args,
        kwargs=kwargs,
        blocksize=blocksize,
    ))
    return cast(Type[H], newcls)

#** Classes **#

class ThreadWriter(Writer):
    """
    Baseclass for threading socket writers
    """
    __slots__ = ('addr', 'sock', 'closing')

    addr:    Address
    sock:    socket.socket
    closing: bool

    def __init__(self, addr: Address, sock: socket.socket, closing: bool = False):
        self.addr    = addr
        self.sock    = sock
        self.closing = closing

    def using_tls(self) -> bool:
        return isinstance(self.sock, SSLSocket)

    def close(self):
        self.closing = True
        self.sock.close()

    def is_closing(self) -> bool:
        return self.closing

class UdpThreadWriter(UdpWriter, ThreadWriter):
    """
    Udp implementation for threaded socket writer
    """

    def write(self, data: bytes, addr: Optional[AnyAddr] = None) -> int:
        return self.sock.sendto(data, addr or self.addr)

class TcpThreadWriter(ThreadWriter):
    """
    Tcp implementation for threaded socket writer
    """

    def start_tls(self, context: SSLContext):
        self.sock = context.wrap_socket(self.sock, server_side=True)

    def write(self, data: bytes) -> int:
        self.sock.sendall(data)
        return len(data)

class RequestHandler(Generic[W], socketserver.BaseRequestHandler):
    """
    Baseclass request handler implementation for UDP/TCP
    """
    factory:   ClassVar[Type[Session]]
    args:      ClassVar[tuple]
    kwargs:    Dict[str, Any]
    blocksize: ClassVar[int]

    addr:   Address
    writer: W
    error:  Optional[Exception]

    @override
    def setup(self) -> None:
        self.error   = None
        self.session = self.factory(*self.args, **self.kwargs)
        self.session.connection_made(self.addr, self.writer)

    @override
    def finish(self) -> None:
        self.session.connection_lost(self.error)

class UdpHandler(RequestHandler[UdpThreadWriter]):
    """
    Udp server request handler implementation
    """
    sock: socket.socket

    @override
    def setup(self) -> None:
        self.addr   = Address(*self.client_address)
        self.sock   = self.request[1]
        self.writer = UdpThreadWriter(self.addr, self.sock)
        return super().setup()

    @override
    def handle(self) -> None:
        try:
            data = self.request[0]
            self.session.data_recieved(data)
        except socket.error as err:
            self.error = err
            if not self.writer.closing:
                self.writer.close()

class TcpHandler(RequestHandler[TcpThreadWriter]):
    """
    Tcp server request handler implementation
    """
    sock: socket.socket

    @override
    def setup(self) -> None:
        self.addr   = Address(*self.client_address)
        self.sock   = self.request
        self.writer = TcpThreadWriter(self.addr, self.sock)
        return super().setup()

    @override
    def handle(self) -> None:
        while not self.writer.closing:
            try:
                data = self.writer.sock.recv(self.blocksize)
                if not data:
                    break
                self.session.data_recieved(data)
            except socket.error as err:
                self.error = err
                if not self.writer.closing:
                    self.writer.close()
                break

class ThreadedMixin(Generic[H], socketserver.ThreadingMixIn):
    """
    Threaded server mixin tweaks
    """
    socket:         socket.socket
    address_family: int
    socket_type:    int
    handler:        Type[H]
   
    _cleanup:        Callable[[], None]
    shutdown:        Callable[[], None]
    server_bind:     Callable[[], None]
    server_activate: Callable[[], None]

    daemon_threads = True

    def _set_classvars(self, blocksize: int, reuse_port: bool):
        setattr(self, 'max_packet_size', blocksize)
        setattr(self, 'allow_reuse_port', reuse_port)

    def start(self):
        """
        shorthand for binding and activating the server (if not already)
        """
        try:
            self.server_bind()
            self.server_activate()
        except Exception as err:
            self.server_close()
            raise err

    def stop(self):
        """
        shutdown serve_forever loop and close socket
        """
        self.shutdown()
        self._cleanup()

    def rebuild_socket(self):
        """
        rebuild socket to recover from socket failure
        """
        self.socket.close()
        self.socket = socket.socket(self.address_family, self.socket_type)
        self.server_bind()

class UdpThreadServer(ThreadedMixin[UdpHandler], socketserver.UDPServer):
    """
    Udp threaded session-based server implementation
    """
    address:         RawAddr
    interface:       Optional[str]
    allow_broadcast: bool

    def __init__(self,
        address:           RawAddr,
        factory:           Type[Session],
        args:              Optional[tuple]          = None,
        kwargs:            Optional[Dict[str, Any]] = None,
        interface:         Optional[str]            = None,
        reuse_port:        bool                     = False,
        blocksize:         int                      = 8192,
        allow_broadcast:   bool                     = False,
        bind_and_activate: bool                     = True,
    ):
        self.address         = address
        self.handler         = new_handler(UdpHandler, factory, args, kwargs, blocksize)
        self.interface       = interface
        self.allow_broadcast = allow_broadcast
        self._set_classvars(blocksize, reuse_port)
        super().__init__(address, self.handler, bind_and_activate)

    def __exit__(self, *args):
        super().__exit__(*args)
        self.shutdown()

    def _cleanup(self):
        """
        custom server cleanup function
        """
        self.socket.close()
        self.server_close()

    @override
    def server_bind(self) -> None:
        """
        additional socket modification controls on server-bind
        """
        super().server_bind()
        modify_socket(self.socket, None, self.interface)
        if self.allow_broadcast:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    @override
    def get_request(self):
        """
        respawn socket after socket error to prevent infinite hanging loop
        """
        try:
            return super().get_request()
        except socket.error as e:
            self.rebuild_socket()
            raise e

    @override
    def serve_forever(self, poll_interval: float = 0.5) -> None:
        """
        polls server forever until its closed by something else

        :param poll_interval: how often server polls for close
        """
        try:
            return super().serve_forever(poll_interval)
        finally:
            self._cleanup()

class TcpThreadServer(ThreadedMixin[TcpHandler], socketserver.TCPServer):
    """
    Tcp threaded session-based server implementation
    """
    address:   RawAddr
    interface: Optional[str]
    ssl:       Optional[SSLContext]
    timeout:   Optional[float]

    def __init__(self,
        address:           RawAddr,
        factory:           Type[Session],
        args:              Optional[tuple]          = None,
        kwargs:            Optional[Dict[str, Any]] = None,
        interface:         Optional[str]            = None,
        reuse_port:        bool                     = False,
        blocksize:         int                      = 8192,
        ssl:               Optional[SSLContext]     = None,
        timeout:           Optional[float]          = None,
        bind_and_activate: bool                     = True,
    ):
        self.address   = address
        self.handler   = new_handler(TcpHandler, factory, args, kwargs, blocksize)
        self.interface = interface
        self.ssl       = ssl
        self.timeout   = timeout
        self._set_classvars(blocksize, reuse_port)
        super().__init__(address, self.handler, bind_and_activate)

    def __exit__(self, *args):
        super().__exit__(*args)
        self.shutdown()

    def _cleanup(self):
        """
        custom server cleanup function
        """
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.server_close()

    @override
    def server_bind(self):
        """
        additional socket modification controls on server-bind
        """
        super().server_bind()
        modify_socket(self.socket, None, self.interface)
        if self.ssl is not None:
            sock = self.ssl.wrap_socket(self.socket, server_side=True)
            self.socket = cast(socket.socket, sock)

    @override
    def get_request(self):
        """
        respawn socket after socket error to prevent infinite hanging loop
        """
        timeout = int(self.timeout) if self.timeout else None
        try:
            sock, addr = super().get_request()
            modify_socket(sock, timeout, None)
            return (sock, addr)
        except socket.timeout as e:
            raise e
        except socket.error as e:
            self.rebuild_socket()
            raise e

    @override
    def serve_forever(self, poll_interval: float = 0.5) -> None:
        """
        polls server forever until its closed by something else

        :param poll_interval: how often server polls for close
        """
        try:
            return super().serve_forever(poll_interval)
        finally:
            self._cleanup()
