import asyncio
import logging

import async_timeout
from aiothrift.connection import ThriftConnection
from aiothrift.processor import TProcessor
from aiothrift.protocol import TBinaryProtocol

from . import BaseClientAdaptor, BaseServerAdaptor
from ..io import AsyncBytesIO

logger = logging.getLogger(__name__)


class TBinaryProtocolHook(TBinaryProtocol):

    def __init__(self, *args, **kwargs):
        self.msg_begin_hook = kwargs.pop('msg_begin_hook', None)
        self.msg_end_hook = kwargs.pop('msg_end_hook', None)
        super().__init__(*args, **kwargs)

    async def read_message_begin(self):
        ret = await super().read_message_begin()
        if self.msg_begin_hook is not None:
            await self.msg_begin_hook()
        return ret

    async def read_message_end(self):
        ret = await super().read_message_end()
        if self.msg_end_hook is not None:
            await self.msg_end_hook()
        return ret

    def write_message_begin(self, *args):
        ret = super().write_message_begin(*args)
        if self.msg_begin_hook is not None:
            self.trans.add_drain_hook(self.msg_begin_hook)
        return ret

    def write_message_end(self):
        ret = super().write_message_end()
        if self.msg_end_hook is not None:
            self.trans.add_drain_hook(self.msg_end_hook)
        return ret


class ThriftServerAdaptor(BaseServerAdaptor):

    __slots__ = BaseServerAdaptor.__slots__ + \
        ('_processor', '_protocol_cls', '_exec_timeout')

    def __init__(self, peer, service, handler, *, exec_timeout=None):
        super().__init__(peer)
        self._processor = TProcessor(service, handler)
        self._protocol_cls = TBinaryProtocolHook
        self._exec_timeout = exec_timeout

    async def handle_function(self, request):
        reader = AsyncBytesIO(request.body)
        writer = AsyncBytesIO()
        iproto = self._protocol_cls(reader)
        oproto = self._protocol_cls(writer)
        try:
            with async_timeout.timeout(self._exec_timeout):
                await self._processor.process(iproto, oproto)
        except (asyncio.IncompleteReadError, ConnectionError):
            logger.debug('client has closed the connection')
            writer.close()
        except asyncio.TimeoutError:
            logger.debug('timeout when processing the client request')
            writer.close()
            raise
        except asyncio.CancelledError:
            logger.debug('service cancelled')
            writer.close()
            raise
        except Exception:
            writer.close()
            raise
        response_body = writer.getvalue()
        writer.close()
        return response_body


class ThriftClientAdaptor(BaseClientAdaptor):

    __slots__ = BaseClientAdaptor.__slots__ + \
        ('_service', '_protocol_cls', '_invoke_timeout')

    def __init__(self, service, *, invoke_timeout=None):
        super().__init__()
        self._service = service
        self._protocol_cls = TBinaryProtocolHook
        self._invoke_timeout = invoke_timeout

    async def _call(self, reader, writer, send_hook, method, args, kwargs):
        loop = asyncio.get_event_loop()
        iprotocol = self._protocol_cls(reader)
        oprotocol = self._protocol_cls(writer, msg_end_hook=send_hook)
        conn = ThriftConnection(self._service,
                                iprot=iprotocol, oprot=oprotocol,
                                address='(callosum-peer)',
                                loop=loop, timeout=self._invoke_timeout)
        try:
            response = await conn.execute(method, *args, **kwargs)
            return response
        finally:
            conn.close()
