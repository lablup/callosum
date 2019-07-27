from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Mapping, Optional, Tuple, Union

import aioredis
import attr

from . import (
    AbstractAddress,
    AbstractBinder, AbstractConnector,
    AbstractConnection,
    BaseTransport,
)
from ..exceptions import ParamError
# from ..auth import Identity
# from ..compat import current_loop


@attr.s(auto_attribs=True, slots=True)
class RedisStreamAddress(AbstractAddress):
    '''
    Group and consumer are only required by receiver-side.
    Thus they must not be specified for CommonStreamBinder.
    '''

    redis_server: Union[str, Tuple[str, int]]
    stream_key: str
    group: Optional[str] = None
    consumer: Optional[str] = None


class RedisStreamConnection(AbstractConnection):

    __slots__ = ('transport', )

    transport: RedisStreamTransport
    addr: RedisStreamAddress
    direction_keys: Tuple[str, str]

    def __init__(self, transport: RedisStreamTransport,
                 addr: RedisStreamAddress,
                 direction_keys: Optional[Tuple[str, str]] = None):
        self.transport = transport
        self.addr = addr
        self.direction_keys = direction_keys

    async def recv_message(self) -> AsyncGenerator[
            Optional[Tuple[bytes, bytes]], None]:
        # assert not self.transport._redis.closed
        if not self.direction_keys:
            stream_key = self.addr.stream_key
        else:
            stream_key = f'{self.addr.stream_key}.{self.direction_keys[0]}'
        _s = asyncio.shield

        async def _xack(raw_msg):
            await self.transport._redis.xack(
                raw_msg[0], self.addr.group, raw_msg[1])
        try:
            raw_msgs = await _s(self.transport._redis.xread_group(
                self.addr.group,
                self.addr.consumer,
                [stream_key],
                latest_ids=['>']))
            for raw_msg in raw_msgs:
                # [0]: stream key, [1]: item ID
                if b'meta' in raw_msg[2]:
                    await _s(_xack(raw_msg))
                    continue
                yield raw_msg[2][b'hdr'], raw_msg[2][b'msg']
                await _s(_xack(raw_msg))
        except asyncio.CancelledError:
            raise
        except aioredis.errors.ConnectionForcedCloseError:
            yield None

    async def send_message(self, raw_msg: Tuple[bytes, bytes]) -> None:
        # assert not self.transport._redis.closed
        if not self.direction_keys:
            stream_key = self.addr.stream_key
        else:
            stream_key = f'{self.addr.stream_key}.{self.direction_keys[1]}'
        _s = asyncio.shield
        await _s(self.transport._redis.xadd(
            stream_key, {b'hdr': raw_msg[0], b'msg': raw_msg[1]}))


class CommonStreamBinder(AbstractBinder):
    '''
    CommonStreamBinder is for use with Publisher class.
    All Publishers using CommonStreamBinder are supposed
    to provide the same stream key for the purposes
    of subsequent message load-balancing among those,
    who read messages from the stream (Subscribers).
    '''

    __slots__ = ('transport', 'addr')

    transport: RedisStreamTransport
    addr: RedisStreamAddress

    async def __aenter__(self):
        self.transport._redis = await aioredis.create_redis(
            self.addr.redis_server,
            **self.transport._redis_opts)
        key = self.addr.stream_key
        # If there were no stream with the specified key before,
        # it is created as a side effect of adding the message.
        await self.transport._redis.xadd(key, {b'meta': b'create-or-join-to-stream'})
        if self.addr.group:
            raise ParamError("group")
        if self.addr.consumer:
            raise ParamError("consumer")
        return RedisStreamConnection(self.transport, self.addr)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        pass


class CommonStreamConnector(AbstractConnector):
    '''
    CommonStreamConnector is for ise with Subscriber class.
    All Subscribers using CommonStreamConnector are supposed
    to provide the same stream key and same group name
    for the purposes of subsequent load-balancing.
    Based on the consumer group name, RedisStream will make sure
    that each subscriber from the group gets distinct set of messages.
    '''

    __slots__ = ('transport', 'addr')

    transport: RedisStreamTransport
    addr: RedisStreamAddress

    async def __aenter__(self):
        pool = await aioredis.create_connection(
            self.addr.redis_server,
            **self.transport._redis_opts)
        self.transport._redis = aioredis.Redis(pool)
        key = self.addr.stream_key
        # If there were no stream with the specified key before,
        # it is created as a side effect of adding the message.
        await self.transport._redis.xadd(key, {b'meta': b'create-or-join-to-stream'})
        groups = await self.transport._redis.xinfo_groups(key)
        if not any(map(lambda g: g[b'name'] == self.addr.group.encode(), groups)):
            await self.transport._redis.xgroup_create(
                key, self.addr.group)
        return RedisStreamConnection(self.transport, self.addr)

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        # we need to create a new Redis connection for cleanup
        # because self.transport._redis gets corrupted upon
        # cancellation of Peer._recv_loop() task.
        _redis = await aioredis.create_redis(
            self.addr.redis_server,
            **self.transport._redis_opts)
        try:
            await asyncio.shield(_redis.xgroup_delconsumer(
                self.addr.stream_key,
                self.addr.group,
                self.addr.consumer))
        finally:
            _redis.close()
            await _redis.wait_closed()


class RedisStreamBinder(AbstractBinder):

    __slots__ = ('transport', 'addr')

    transport: RedisStreamTransport
    addr: RedisStreamAddress

    async def __aenter__(self):
        self.transport._redis = await aioredis.create_redis(
            self.addr.redis_server,
            **self.transport._redis_opts)
        key = f'{self.addr.stream_key}.bind'
        await self.transport._redis.xadd(key, {b'meta': b'create-stream'})
        groups = await self.transport._redis.xinfo_groups(key)
        if not any(map(lambda g: g[b'name'] == self.addr.group.encode(), groups)):
            await self.transport._redis.xgroup_create(
                key, self.addr.group)  # TODO: mkstream=True in future aioredis
        return RedisStreamConnection(self.transport, self.addr,
                                     ('bind', 'conn'))

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        # we need to create a new Redis connection for cleanup
        # because self.transport._redis gets corrupted upon
        # cancellation of Peer._recv_loop() task.
        _redis = await aioredis.create_redis(
            self.addr.redis_server,
            **self.transport._redis_opts)
        try:
            await asyncio.shield(_redis.xgroup_delconsumer(
                f'{self.addr.stream_key}.bind',
                self.addr.group, self.addr.consumer))
        finally:
            _redis.close()
            await _redis.wait_closed()


class RedisStreamConnector(AbstractConnector):

    __slots__ = ('transport', 'addr')

    transport: RedisStreamTransport
    addr: RedisStreamAddress

    async def __aenter__(self):
        pool = await aioredis.create_connection(
            self.addr.redis_server,
            **self.transport._redis_opts)
        self.transport._redis = aioredis.Redis(pool)
        key = f'{self.addr.stream_key}.conn'
        await self.transport._redis.xadd(key, {b'meta': b'create-stream'})
        groups = await self.transport._redis.xinfo_groups(key)
        if not any(map(lambda g: g[b'name'] == self.addr.group.encode(), groups)):
            await self.transport._redis.xgroup_create(
                key, self.addr.group)  # TODO: mkstream=True in future aioredis
        return RedisStreamConnection(self.transport, self.addr,
                                     ('conn', 'bind'))

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        # we need to create a new Redis connection for cleanup
        # because self.transport._redis gets corrupted upon
        # cancellation of Peer._recv_loop() task.
        _redis = await aioredis.create_redis(
            self.addr.redis_server,
            **self.transport._redis_opts)
        try:
            await asyncio.shield(_redis.xgroup_delconsumer(
                f'{self.addr.stream_key}.conn',
                self.addr.group, self.addr.consumer))
        finally:
            _redis.close()
            await _redis.wait_closed()


class RedisStreamTransport(BaseTransport):

    '''
    Implementation for the transport backend by Redis Streams.
    '''

    __slots__ = BaseTransport.__slots__ + (
        '_redis_opts',
        '_redis',
    )

    _redis_opts: Mapping[str, Any]
    _redis: aioredis.RedisConnection

    binder_cls = RedisStreamBinder
    connector_cls = RedisStreamConnector

    def __init__(self, authenticator, **kwargs):
        self._redis_opts = kwargs.pop('redis_opts', {})
        super().__init__(authenticator, **kwargs)
        self._redis = None

    async def close(self):
        if self._redis is not None and not self._redis.closed:
            self._redis.close()
            await self._redis.wait_closed()
