"""
PyServe Common Abstractions
"""
import socket
from abc import abstractmethod
from ssl import SSLContext
from typing import (
    Callable, ClassVar, Generic, NamedTuple, Optional, ParamSpec, Protocol, Tuple,
    TypeVar, Union)

#** Variables **#
__all__ = [
    'P',
    'S',
    'W',
    'RawAddr',
    'AnyAddr',

    'modify_socket',

    'Address',
    'SessFactory',
    'Writer',
    'UdpWriter',
    'Session',
]

P       = ParamSpec('P')
S       = TypeVar('S', bound=Union['Session', 'AsyncSession'])
W       = TypeVar('W', bound='BaseWriter', contravariant=True)
RawAddr = Tuple[str, int]
AnyAddr = Union[RawAddr, 'Address']

#** Functions **#

def modify_socket(
    sock:      socket.socket,
    timeout:   Optional[int],
    interface: Optional[str]
) -> socket.socket:
    """
    Modify the socket object with the following options
    """
    if timeout is not None:
        sock.settimeout(timeout)
    if interface is not None:
        iface = interface.encode()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, iface)
    return sock

#** Classes **#

class Address(NamedTuple):
    host: str
    port: int

class SessFactory(Generic[S]):
    __slots__ = ('factory', 'args', 'kwargs', 'is_async')

    def __init__(self, factory: Callable[P, S], *args: P.args, **kwargs: P.kwargs):
        self.factory  = factory
        self.args     = args
        self.kwargs   = kwargs
        self.is_async = hasattr(self.build(), '_IS_ASYNC')

    def build(self) -> S:
        return self.factory(*self.args, **self.kwargs)

class BaseWriter(Protocol):

    @abstractmethod
    def using_tls(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def is_closing(self) -> bool:
        raise NotImplementedError

class Writer(BaseWriter):
    """
    Abstract socket writer implementation for a single server connection
    """

    @abstractmethod
    def start_tls(self, context: SSLContext):
        raise NotImplementedError

    @abstractmethod
    def write(self, data: bytes) -> None:
        raise NotImplementedError

class UdpWriter(BaseWriter):
    """
    Abstract socket writer implementation for a single UDP server connection
    """

    def using_tls(self) -> bool:
        return False

    @abstractmethod
    def write(self, data: bytes, addr: Optional[AnyAddr] = None) -> None:
        raise NotImplementedError

class Session(Protocol[W]):
    """
    Abstract session-manager implementation for single server connection
    """

    def _datagram_recieved(self, addr: Address, writer: W, data: bytes):
        self.connection_made(addr, writer)
        self.data_received(data)

    @abstractmethod
    def connection_made(self, addr: Address, writer: W):
       raise NotImplementedError

    @abstractmethod
    def data_received(self, data: bytes):
        raise NotImplementedError

    @abstractmethod
    def connection_lost(self, err: Optional[Exception]):
        raise NotImplementedError

class AsyncSession(Protocol[W]):
    """
    Abstract async session-manager implementation for single server connection
    """
    _IS_ASYNC: ClassVar[bool] = True

    async def _datagram_recieved(self, addr: Address, writer: W, data: bytes):
        await self.connection_made(addr, writer)
        await self.data_received(data)

    @abstractmethod
    async def connection_made(self, addr: Address, writer: W):
       raise NotImplementedError

    @abstractmethod
    async def data_received(self, data: bytes):
        raise NotImplementedError

    @abstractmethod
    async def connection_lost(self, err: Optional[Exception]):
        raise NotImplementedError
