"""
Unify socket server implementations using session-based model
"""
from ssl import SSLContext
from typing import Any, Dict, Optional, Tuple, Type

from .abc import Address, RawAddr, Session, UdpWriter, Writer
from .threading import UdpThreadServer, TcpThreadServer
from .transport import UdpAsyncServer, TcpAsyncServer

#** Variables **#
__all__ = [
    'listen_udp_async',
    'listen_tcp_async',
    'listen_udp_threaded',
    'listen_tcp_threaded',

    'Address',
    'RawAddr',
    'Session',
    'UdpWriter',
    'Writer',

    'UdpThreadServer',
    'TcpThreadServer',
    'UdpAsyncServer',
    'TcpAsyncServer',
]

#** Functions **#

async def listen_udp_async(
    address:         RawAddr,
    factory:         Type[Session],
    args:            Optional[Tuple[Any, ...]] = None,
    kwargs:          Optional[Dict[str, Any]]  = None,
    interface:       Optional[str]             = None,
    reuse_port:      bool                      = False,
    allow_broadcast: bool                      = False,
) -> None:
    """
    """
    async with UdpAsyncServer(
        address=address,
        factory=factory,
        args=args,
        kwargs=kwargs,
        interface=interface,
        reuse_port=reuse_port,
        allow_broadcast=allow_broadcast,
    ) as server:
        await server.serve_forever()

async def listen_tcp_async(
    address:    RawAddr,
    factory:    Type[Session],
    args:       Optional[Tuple[Any, ...]] = None,
    kwargs:     Optional[Dict[str, Any]]  = None,
    interface:  Optional[str]             = None,
    reuse_port: bool                      = False,
    backlog:    int                       = 100,
    ssl:        Optional[SSLContext]      = None,
    timeout:    Optional[int]             = None,
) -> None:
    """
    """
    async with TcpAsyncServer(
        address=address,
        factory=factory,
        args=args,
        kwargs=kwargs,
        interface=interface,
        reuse_port=reuse_port,
        backlog=backlog,
        ssl=ssl,
        timeout=timeout,
    ) as server:
        await server.serve_forever()

def listen_udp_threaded(
    address:         RawAddr,
    factory:         Type[Session],
    args:            Optional[Tuple[Any, ...]] = None,
    kwargs:          Optional[Dict[str, Any]]  = None,
    interface:       Optional[str]             = None,
    reuse_port:      bool                      = False,
    allow_broadcast: bool                      = False,
    blocksize:       int                       = 8192
) -> None:
    """
    """
    with UdpThreadServer(
        address=address,
        factory=factory,
        args=args,
        kwargs=kwargs,
        interface=interface,
        blocksize=blocksize,
        reuse_port=reuse_port,
        allow_broadcast=allow_broadcast,
    ) as server:
        server.serve_forever()

def listen_tcp_threaded(
    address:    RawAddr,
    factory:    Type[Session],
    args:       Optional[Tuple[Any, ...]] = None,
    kwargs:     Optional[Dict[str, Any]]  = None,
    interface:  Optional[str]             = None,
    reuse_port: bool                      = False,
    blocksize:  int                       = 8192,
    ssl:        Optional[SSLContext]      = None,
    timeout:    Optional[int]             = None,
) -> None:
    """
    """
    with TcpThreadServer(
        address=address,
        factory=factory,
        args=args,
        kwargs=kwargs,
        interface=interface,
        reuse_port=reuse_port,
        blocksize=blocksize,
        ssl=ssl,
        timeout=timeout,
    ) as server:
        server.serve_forever()
