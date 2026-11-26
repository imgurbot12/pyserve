"""
PyServe UnitTests
"""
from typing import Optional, Union

from ..abc import Address, AsyncSession, Session, UdpWriter, Writer

#** Variables **#
__all__ = ['TransportTests', 'ThreadingTests', 'UtilTests']

#** Classes **#

class EchoSession(Session):
    def connection_made(self, addr: Address, writer: Union[Writer, UdpWriter]):
        self.addr   = addr
        self.writer = writer
    def data_received(self, data: bytes):
        self.writer.write(data)
        if data == b'bye':
            self.writer.close()
    def connection_lost(self, err: Optional[Exception]):
        pass

class AsyncEchoSession(AsyncSession):
    async def connection_made(self, addr: Address, writer: Writer):
        self.addr   = addr
        self.writer = writer
    async def data_received(self, data: bytes):
        self.writer.write(data)
        if data == b'bye':
            self.writer.close()
    async def connection_lost(self, err: Optional[Exception]):
        pass

#** Imports **#
from .transport import TransportTests
from .threading import ThreadingTests
from .utils import UtilTests
