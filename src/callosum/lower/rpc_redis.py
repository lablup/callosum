from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Mapping, Optional, Tuple, Union

import attrs
import redis
import redis.asyncio

from ..abc import RawHeaderBody
from . import (
    AbstractAddress,
    AbstractBinder,
    AbstractConnection,
    AbstractConnector,
    BaseTransport,
)
from .redis_common import redis_addr_to_url


@attrs.define(auto_attribs=True, slots=True)
class RedisStreamAddress(AbstractAddress):
    redis_server: Union[str, Tuple[str, int]]
    stream_key: str
    group: Optional[str] = None
    consumer: Optional[str] = None


class RPCRedisConnection(AbstractConnection):
    __slots__ = ("transport",)

    transport: RPCRedisTransport
    addr: RedisStreamAddress
    direction_keys: Optional[Tuple[str, str]]

    def __init__(
        self,
        transport: RPCRedisTransport,
        addr: RedisStreamAddress,
        direction_keys: Optional[Tuple[str, str]] = None,
    ):
        self.transport = transport
        self.addr = addr
        self.direction_keys = direction_keys

    async def recv_message(self) -> AsyncGenerator[Optional[RawHeaderBody], None]:
        assert self.transport._redis is not None
        assert self.addr.group is not None
        assert self.addr.consumer is not None
        if not self.direction_keys:
            stream_key = self.addr.stream_key
        else:
            stream_key = f"{self.addr.stream_key}.{self.direction_keys[0]}"

        try:
            per_key_fetch_list: list[Any] = []
            while not per_key_fetch_list:
                per_key_fetch_list = await self.transport._redis.xreadgroup(
                    self.addr.group,
                    self.addr.consumer,
                    {stream_key: ">"},
                    block=1000,
                )
            for fetch_info in per_key_fetch_list:
                if fetch_info[0].decode() != stream_key:
                    continue
                for item in fetch_info[1]:
                    item_id: bytes = item[0]
                    item_data: dict[bytes, bytes] = item[1]
                    try:
                        if b"meta" in item_data:
                            continue
                        yield RawHeaderBody(
                            item_data[b"hdr"],
                            item_data[b"msg"],
                            None,
                        )
                    finally:
                        await self.transport._redis.xack(
                            stream_key, self.addr.group, item_id
                        )
        except asyncio.CancelledError:
            raise
        except redis.asyncio.ConnectionError:
            yield None

    async def send_message(self, raw_msg: RawHeaderBody) -> None:
        assert self.transport._redis is not None
        if not self.direction_keys:
            stream_key = self.addr.stream_key
        else:
            stream_key = f"{self.addr.stream_key}.{self.direction_keys[1]}"
        await self.transport._redis.xadd(
            stream_key, {b"hdr": raw_msg[0], b"msg": raw_msg[1]}
        )


class RPCRedisBinder(AbstractBinder):
    """
    This class is binder for RPCRedisTransport.
    Usually, the RPC server is supposed to provide
    the connect address during Peer instantiation,
    which will result in the Peer class using
    the RPCRedisBinder for establishing
    the connection.
    """

    __slots__ = ("transport", "addr", "_addr_url")

    transport: RPCRedisTransport
    addr: RedisStreamAddress
    _addr_url: str

    async def __aenter__(self):
        assert self.addr.group is not None
        assert self.addr.consumer is not None
        self._addr_url = redis_addr_to_url(self.addr.redis_server)
        self.transport._redis = await redis.asyncio.from_url(
            self._addr_url, **self.transport._redis_opts
        )
        key = f"{self.addr.stream_key}.bind"
        await self.transport._redis.xadd(key, {b"meta": b"create-stream"})
        groups = await self.transport._redis.xinfo_groups(key)
        print(groups)
        if not any(map(lambda g: g["name"] == self.addr.group.encode(), groups)):
            await self.transport._redis.xgroup_create(
                key,
                self.addr.group,
                mkstream=True,
            )
        return RPCRedisConnection(self.transport, self.addr, ("bind", "conn"))

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        assert self.addr.group is not None
        assert self.addr.consumer is not None
        # we need to create a new Redis connection for cleanup
        # because self.transport._redis gets corrupted upon
        # cancellation of Peer._recv_loop() task.
        _redis = await redis.asyncio.from_url(
            self._addr_url, **self.transport._redis_opts
        )
        try:
            await asyncio.shield(
                _redis.xgroup_delconsumer(
                    f"{self.addr.stream_key}.bind",
                    self.addr.group,
                    self.addr.consumer,
                )
            )
        finally:
            await _redis.close()


class RPCRedisConnector(AbstractConnector):
    """
    This class is connector for RPCRedisTransport.
    Usually, the RPC invoker (client) is supposed to provide
    the connect address during Peer instantiation,
    which will result in the Peer class using
    the RPCRedisConnector for establishing
    the connection.
    """

    __slots__ = ("transport", "addr", "_addr_url")

    transport: RPCRedisTransport
    addr: RedisStreamAddress

    async def __aenter__(self):
        assert self.addr.group is not None
        assert self.addr.consumer is not None
        self._addr_url = redis_addr_to_url(self.addr.redis_server)
        self.transport._redis = redis.asyncio.from_url(
            self._addr_url, **self.transport._redis_opts
        )
        key = f"{self.addr.stream_key}.conn"
        await self.transport._redis.xadd(key, {b"meta": b"create-stream"})
        groups = await self.transport._redis.xinfo_groups(key)
        if not any(map(lambda g: g["name"] == self.addr.group.encode(), groups)):
            await self.transport._redis.xgroup_create(
                key,
                self.addr.group,
                mkstream=True,
            )
        return RPCRedisConnection(self.transport, self.addr, ("conn", "bind"))

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        assert self.addr.group is not None
        assert self.addr.consumer is not None
        # we need to create a new Redis connection for cleanup
        # because self.transport._redis gets corrupted upon
        # cancellation of Peer._recv_loop() task.
        _redis = await redis.asyncio.from_url(
            self._addr_url, **self.transport._redis_opts
        )
        try:
            await asyncio.shield(
                _redis.xgroup_delconsumer(
                    f"{self.addr.stream_key}.conn",
                    self.addr.group,
                    self.addr.consumer,
                )
            )
        finally:
            await _redis.close()


class RPCRedisTransport(BaseTransport):

    """
    Implementation for bidirectional transport backend by Redis Streams.
    """

    __slots__ = BaseTransport.__slots__ + (
        "_redis_opts",
        "_redis",
    )

    _redis_opts: Mapping[str, Any]
    _redis: Optional[redis.asyncio.Redis]

    binder_cls = RPCRedisBinder
    connector_cls = RPCRedisConnector

    def __init__(
        self,
        authenticator,
        **kwargs,
    ) -> None:
        super().__init__(authenticator, **kwargs)
        self._redis_opts = self.transport_opts.get("redis_opts", {})
        self._redis = None

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.close()
