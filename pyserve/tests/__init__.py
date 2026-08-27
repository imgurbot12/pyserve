"""
PyServe Server UnitTests
"""
import json
import random
from typing import Optional

from .. import Address, Session, Writer

#** Variables **#
__all__ = [
    'UdpThreadTests', 
    'TcpThreadTests',
    'UdpAsyncTests', 
    'TcpAsyncTests',
]

#** Functions **#

def random_port() -> int:
    return random.randint(10_000, 20_000)

#** Classes **#

class EchoSession(Session):
    def connection_made(self, addr: Address, writer: Writer):
        self.addr   = addr
        self.writer = writer
    def data_received(self, data: bytes):
        self.writer.write(data)
        if data == b'bye':
            self.writer.close()
    def connection_lost(self, err: Optional[Exception]):
        pass

class ArgsSession(Session):
    def __init__(self, *args, **kwargs):
        self.args   = args
        self.kwargs = kwargs
    def connection_made(self, addr: Address, writer: Writer):
        self.addr   = addr
        self.writer = writer
    def data_received(self, data: bytes):
        message = json.dumps({
            'args':   self.args,
            'kwargs': self.kwargs,
            'data':   data.decode(),
        }).encode()
        self.writer.write(message)
    def connection_lost(self, err: Optional[Exception]):
        pass

#** Imports **#
from .threading import UdpThreadTests, TcpThreadTests
from .transport import UdpAsyncTests, TcpAsyncTests


