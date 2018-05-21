import asyncio
import io
import logging

import async_timeout
from aiothrift.connection import ThriftConnection as AIOThriftConnection
from aiothrift.processor import TProcessor
from aiothrift.protocol import TBinaryProtocol
from aiothrift.util import args2kwargs
from thriftpy.thrift import TMessageType

from . import BaseClientAdaptor, BaseServerAdaptor
from ..io import AsyncBytesIO

logger = logging.getLogger(__name__)


class ThriftServerAdaptor(BaseServerAdaptor):

    __slots__ = BaseServerAdaptor.__slots__ + \
        ('_processor', '_protocol_cls', '_exec_timeout')

    def __init__(self, peer, service, handler, *, exec_timeout=None):
        super().__init__(peer)
        self._processor = TProcessor(service, handler)
        self._protocol_cls = TBinaryProtocol
        self._exec_timeout = exec_timeout

    async def handle_function(self, request) -> bytes:
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
        # Even with oneway requests, Callosum will respond with empty body.
        response_body = writer.getvalue()
        writer.close()
        return response_body


class ThriftConnection(AIOThriftConnection):
    '''
    aiothrift.connection.ThriftConnection modified for Callosum.
    '''

    async def execute(self, api, *args, **kwargs):
        '''
        This is the strippted down version of the original execute(),
        with addition of yield statements.
        '''

        kw = args2kwargs(getattr(self.service, api + "_args").thrift_spec, *args)
        kwargs.update(kw)
        result_cls = getattr(self.service, api + "_result")

        self._seqid += 1
        self._oprot.write_message_begin(api, TMessageType.CALL, self._seqid)
        args = getattr(self.service, api + '_args')()
        for k, v in kwargs.items():
            setattr(args, k, v)
        args.write(self._oprot)
        self._oprot.write_message_end()
        await self._oprot.trans.drain()

        # Switch over the control to Callosum so that it can perform send/recv
        # with its own lower transport layer.
        yield

        if not getattr(result_cls, "oneway"):
            result = await self._recv(api)
            yield result
        else:
            yield None


class ThriftClientAdaptor(BaseClientAdaptor):

    __slots__ = BaseClientAdaptor.__slots__ + \
        ('_service', '_protocol_cls', '_invoke_timeout')

    def __init__(self, service):
        super().__init__()
        self._service = service
        self._protocol_cls = TBinaryProtocol

    async def _call(self, method, args, kwargs):
        loop = asyncio.get_event_loop()
        reader = AsyncBytesIO()
        writer = AsyncBytesIO()
        iprotocol = self._protocol_cls(reader)
        oprotocol = self._protocol_cls(writer)
        conn = ThriftConnection(self._service,
                                iprot=iprotocol,
                                oprot=oprotocol,
                                address='(callosum-peer)',
                                loop=loop, timeout=None)
        try:
            execute_agen = conn.execute(method, *args, **kwargs)
            await execute_agen.asend(None)
            raw_request_body = writer.getvalue()
            raw_response_body = yield raw_request_body
            reader.write(raw_response_body)
            reader.seek(0, io.SEEK_SET)
            result = await execute_agen.asend(None)
            try:
                await execute_agen.asend(None)
            except StopAsyncIteration:
                pass
            yield result
        finally:
            conn.close()
