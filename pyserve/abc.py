"""
PyServe Common Abstractions
"""
from abc import abstractmethod
import socket
from ssl import SSLContext
from typing import (
    Any, Dict, Generic, NamedTuple, Optional, Protocol, Tuple,
    Type, TypeVar, Union)

#** Variables **#
__all__ = [
    'S',
    'RawAddr',
    'AnyAddr',

    'modify_socket',

    'Address',
    'SessFactory',
    'Writer',
    'UdpWriter',
    'Session',
]

S       = TypeVar('S', bound='Session')
RawAddr = Tuple[str, int]
AnyAddr = Union[RawAddr, 'Address']

#** Functions **#

def modify_socket(
    sock:      socket.socket,
    timeout:   Optional[int],
    interface: Optional[str]
) -> socket.socket:
    """
    modify the socket object w/ the following options
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

class SessFactory(Generic[S], NamedTuple):
    factory: Type[S]
    args:    Tuple[Any, ...]
    kwargs:  Dict[str, Any]

    def build(self) -> S:
        return self.factory(*self.args, **self.kwargs)

    def test(self) -> None:
        self.build()

class Writer(Protocol):
    """
    Abstract socket writer implementation for a single server connection
    """

    @abstractmethod
    def using_tls(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def start_tls(self, context: SSLContext):
        raise NotImplementedError

    @abstractmethod
    def write(self, data: bytes) -> int:
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def is_closing(self) -> bool:
        raise NotImplementedError

class UdpWriter(Writer):
    """
    Abstract socket writer implementation for a single udp server connection
    """

    def start_tls(self, context: SSLContext):
        raise NotImplementedError('Cannot Use SSL over UDP')

    @abstractmethod
    def write(self, data: bytes, addr: Optional[AnyAddr] = None) -> int:
        raise NotImplementedError

class Session(Protocol):
    """
    Abstract session-manager implementation for single server connection
    """

    @abstractmethod
    def connection_made(self, addr: Address, writer: Writer):
       raise NotImplementedError

    @abstractmethod
    def data_received(self, data: bytes):
        raise NotImplementedError

    @abstractmethod
    def connection_lost(self, err: Optional[Exception]):
        raise NotImplementedError
