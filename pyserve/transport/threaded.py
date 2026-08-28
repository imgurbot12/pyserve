"""
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import queue
from ssl import SSLContext
from threading import Event
from typing import Generic, NamedTuple, Optional, Union, cast

from . import T, wrap_ssl
from ..abc import Address, AnyAddr, RawAddr, SessFactory, Session, UdpWriter, Writer

#** Variables **#
__all__ = []

ServerAction = Union['Connected', 'Recieved', 'Disconnected', None]
ClientAction = Union['WrapSSL', 'WriteAction', None]

#** Classes **#

class Connected(NamedTuple):
    addr:   Address
    writer: Writer

class Recieved(NamedTuple):
    data: bytes

class Disconnected(NamedTuple):
    error: Optional[Exception]

class WrapSSL(NamedTuple):
    context: SSLContext

class WriteAction(NamedTuple):
    data: bytes
    addr: Optional[RawAddr]

class QueuedWriter(Writer):
    __slots__ = ('addr', 'queue', 'tls', 'closing')

    addr:    Address
    queue:   queue.Queue
    closing: bool

    def __init__(self, addr: Address, queue: queue.Queue, using_tls: Event):
        self.addr    = addr
        self.queue   = queue
        self.tls     = using_tls
        self.closing = False

    def using_tls(self) -> bool:
        return self.tls.is_set()

    def start_tls(self, context: SSLContext):
        self.queue.put(WrapSSL(context))

    def write(self, data: bytes) -> int:
        self.queue.put(WriteAction(data, self.addr))
        return len(data)

    def close(self):
        self.queue.put(None)
        self.closing = True

    def is_closing(self) -> bool:
        return self.closing

class UdpQueuedWriter(UdpWriter, QueuedWriter):
    def write(self, data: bytes, addr: Optional[AnyAddr] = None) -> int:
        self.queue.put(WriteAction(data, addr or self.addr))
        return len(data)

class BaseProtocol(Generic[T]):
    __slots__ = ('transport', 'task',
        'factory', 'inbound', 'outbound', 'using_tls', 'pool')

    transport: T

    factory:   SessFactory
    inbound:   queue.Queue
    outbound:  queue.Queue
    using_tls: Event
    pool:      ThreadPoolExecutor
    task:      Optional[asyncio.Task]

    def __init__(self, factory: SessFactory, pool: ThreadPoolExecutor):
        self.factory   = factory
        self.inbound   = queue.Queue()
        self.outbound  = queue.Queue()
        self.using_tls = Event()
        self.pool      = pool
        self.task      = None

    def wrap_tls(self, context: SSLContext):
        """
        """
        if not isinstance(self.transport, asyncio.Transport):
            raise RuntimeError()
        def callback(transport: asyncio.Transport):
            self.transport = cast(T, transport)
            self.using_tls.set()
        wrap_ssl(context, self.transport, callback)

    def close(self):
        """
        """
        self.transport.close()
        self.outbound.put_nowait(None)

    async def reader_task(self):
        """
        """
        while not self.transport.is_closing():
            try:
                message: ClientAction = self.outbound.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
            if isinstance(message, WrapSSL):
                self.wrap_tls(message.context)
            elif isinstance(message, WriteAction):
                if hasattr(self.transport, 'sendto'):
                    transport = cast(asyncio.DatagramTransport, self.transport)
                    transport.sendto(message.data, message.addr)
                else:
                    transport = cast(asyncio.Transport, self.transport)
                    transport.write(message.data)
            else:
                self.transport.close()
                break

class UdpThreadedProtocol(BaseProtocol, asyncio.DatagramProtocol):
    transport: asyncio.DatagramTransport

    @staticmethod
    def writer_thread(f: SessFactory, writer: QueuedWriter, data: bytes):
        """
        """
        session: Session = f.build()
        session.connection_made(writer.addr, writer)
        session.data_received(data)
        session.connection_lost(None)

    def spawn_tasks(self, addr: RawAddr, data: bytes):
        """
        """
        loop = asyncio.get_event_loop()
        if self.task is None:
            self.task = loop.create_task(self.reader_task())
        address = Address(*addr)
        writer  = QueuedWriter(address, self.outbound, self.using_tls)
        args    = (self.factory, writer, data)
        loop.run_in_executor(self.pool, self.writer_thread, *args)

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: RawAddr) -> None:
        self.spawn_tasks(addr, data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.close()

class TcpThreadedProtocol(BaseProtocol, asyncio.Protocol):
    __slots__ = ('timeout', )

    transport: asyncio.Transport
    timeout:   Optional[float]

    def __init__(self,
        factory: SessFactory,
        pool:    ThreadPoolExecutor,
        timeout: Optional[float] = None
    ):
        super().__init__(factory, pool)
        self.timeout = timeout

    def set_timeout(self, timeout: float):
        """
        implement timeout handling for async servers
        """
        async def expire():
            await asyncio.sleep(timeout)
            if not self.transport.is_closing():
                self.transport.abort()
                self.close()

        loop = asyncio.get_event_loop()
        loop.create_task(expire())

    @staticmethod
    def writer_thread(f: SessFactory, inb: queue.Queue, out: queue.Queue):
        """
        """
        session: Session = f.build()
        while True:
            action: ServerAction = inb.get()
            if isinstance(action, Connected):
                session.connection_made(action.addr, action.writer)
            elif isinstance(action, Recieved):
                session.data_received(action.data)
            elif isinstance(action, Disconnected):
                session.connection_lost(action.error)
                out.put_nowait(None)
                break
            else:
                break

    def spawn_tasks(self, addr: RawAddr):
        """
        """
        if self.task is not None:
            return
        address = Address(*addr)
        writer  = QueuedWriter(address, self.outbound, self.using_tls)
        args    = (self.factory, self.inbound, self.outbound)
        loop    = asyncio.get_event_loop()
        loop.run_in_executor(self.pool, self.writer_thread, *args)
        self.task = loop.create_task(self.reader_task())
        self.inbound.put_nowait(Connected(address, writer))

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.Transport, transport)
        addr = self.transport.get_extra_info('peername')
        self.spawn_tasks(addr)
        if self.timeout is not None:
            self.set_timeout(self.timeout)

    def data_received(self, data: bytes) -> None:
        self.inbound.put_nowait(Recieved(data))

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.inbound.put_nowait(Disconnected(exc))
        self.close()
