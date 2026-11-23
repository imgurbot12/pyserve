"""
PyServe Socket Client Utilities
"""
import socket
import asyncio
from abc import abstractmethod
from typing import Optional, Protocol, Tuple, Union

from .abc import RawAddr

#** Variables **#
__all__ = ['AbstractClient', 'TcpAsyncClient', 'UdpAsyncClient']

Bytes = Union[bytes, bytearray, memoryview]

#** Classes **#

class AbstractClient(Protocol):
    """
    Abstract async socket client
    """
    socket: socket.socket

    @abstractmethod
    async def connect(self, addr: RawAddr):
        raise NotImplementedError

    @abstractmethod
    async def recv(self, nbytes: int) -> bytes:
        raise NotImplementedError

    @abstractmethod
    async def send(self, data: Bytes) -> None:
        raise NotImplementedError

class TcpAsyncClient(AbstractClient):
    """
    Simple async TCP client implementation
    """
    __slots__ = ('loop', 'socket', )

    socket: socket.socket

    def __init__(self):
        self.loop   = asyncio.get_running_loop()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setblocking(False)

    async def connect(self, addr: RawAddr):
        """
        Connect to the TCP server at the specified address
        """
        await self.loop.sock_connect(self.socket, addr)

    async def recv(self, nbytes: int) -> bytes:
        """
        Receive a response from the TCP server
        """
        return await self.loop.sock_recv(self.socket, nbytes)

    async def send(self, data: Bytes) -> None:
        """
        Send data to the TCP server
        """
        await self.loop.sock_sendall(self.socket, data)

    def close(self):
        """
        Close socket connection and client
        """
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except:
            pass
        self.socket.close()

class UdpAsyncClient(AbstractClient):
    """
    Simple async UDP client implementation
    """
    __slots__ = ('loop', 'socket', '_addr', '_dgram')

    def __init__(self, _force_dgram: bool = False):
        self.loop   = asyncio.get_running_loop()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self._addr  = None
        self._dgram = None \
            if hasattr(self.loop, 'sock_recvfrom') and not _force_dgram else \
            _UdpClientBackport(self.loop, self.socket)

    def _getaddr(self, addr: Optional[RawAddr]) -> RawAddr:
        """
        Retrieve the address of the UDP server or raise an error
        """
        addr = addr or self._addr
        if addr is not None:
            return addr
        raise RuntimeError(f'UDP socket is not connected. No address')

    async def connect(self, addr: RawAddr) -> None:
        """
        Configure a default address for UDP client to use
        """
        self._addr = addr

    async def send(self, data: Bytes) -> None:
        """
        Send data to default UDP server configured with `connect`
        """
        await self.sendto(data)

    async def recv(self, nbytes: int) -> bytes:
        """
        Receive data from UDP server at the address configured with `connect`
        """
        expected   = self._getaddr(None)
        data, addr = await self.recvfrom(nbytes)
        if addr is not None and addr != expected:
            raise RuntimeError(f'Unexpected response from address {addr}')
        return data

    async def recvfrom(self, nbytes: int) -> Tuple[bytes, RawAddr]:
        """
        Wait for an inbound message over UDP socket connection
        """
        task = self.loop.sock_recvfrom(self.socket, nbytes) \
            if self._dgram is None else \
            self._dgram.recvfrom()
        return await task

    async def sendto(self, data: Bytes, addr: Optional[RawAddr] = None) -> None:
        """
        Send a message over the UDP socket with the given bytes and address
        """
        addr = self._getaddr(addr)
        task = self.loop.sock_sendto(self.socket, data, addr) \
            if self._dgram is None else \
            self._dgram.sendto(data, addr)
        await task

    def close(self):
        """
        Close the client connection to server
        """
        self.socket.close()
        if self._dgram is not None:
            self._dgram.close()

class _UdpClientBackport:
    """
    UDP client backported support system.

    `loop.sock_sendto` and `loop.sock_recvfrom` were not introduced until py3.11
    and so this method uses the old `asyncio.create_datagram_endpoint` method
    to handle socket interactions safely and asyncly.
    """
    __slots__ = ('loop', 'socket', 'tport', 'proto')

    socket: socket.socket
    tport:  Optional[asyncio.DatagramTransport]
    proto:  Optional[_UdpProtocol]

    def __init__(self, loop: asyncio.AbstractEventLoop, socket: socket.socket):
        self.loop   = loop
        self.socket = socket
        self.tport  = None
        self.proto  = None

    async def _connect(self) -> Tuple[asyncio.DatagramTransport, _UdpProtocol]:
        """
        Establish UDP datagram endpoint used to send and receive data
        """
        if self.tport is None or self.proto is None:
            self.tport, self.proto = await self.loop.create_datagram_endpoint(
                _UdpProtocol, sock=self.socket)
        return (self.tport, self.proto)

    async def recvfrom(self) -> Tuple[bytes, RawAddr]:
        """
        Wait for message to be passed over datagram protocol queue
        """
        _, protocol = await self._connect()
        message = await protocol.queue.get()
        if isinstance(message, Exception):
            raise message
        return message

    async def sendto(self, data: Bytes, addr: RawAddr) -> None:
        """
        Send message via datagram transport
        """
        transport, _ = await self._connect()
        transport.sendto(data, addr)

    def close(self):
        """
        Close transport and protocol
        """
        if self.tport is not None:
            self.tport.close()

class _UdpProtocol(asyncio.DatagramProtocol):
    """
    DatagramProtocol implementation used for _UdpClientBackport
    """
    __slots__ = ('queue', 'closed', )

    def __init__(self):
        self.queue  = asyncio.Queue()
        self.closed = asyncio.Event()

    def datagram_received(self, data: bytes, addr: RawAddr) -> None:
        self.queue.put_nowait((data, addr))

    def error_received(self, exc: Exception) -> None:
        self.queue.put_nowait(exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.closed.set()
        if exc is not None:
            self.queue.put_nowait(exc)
