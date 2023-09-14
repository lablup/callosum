import asyncio
import io
import logging
from typing import Any, Optional, Sequence

from thriftpy2.contrib.aio.processor import TAsyncProcessor
from thriftpy2.contrib.aio.protocol.binary import TAsyncBinaryProtocol
from thriftpy2.thrift import TApplicationException, TMessageType, args_to_kwargs

from ..io import AsyncBytesIO
from ..rpc import Peer, RPCMessage
from . import BaseRPCClientAdaptor, BaseRPCServerAdaptor

logger = logging.getLogger(__spec__.name)  # type: ignore[name-defined]


class ThriftServerAdaptor(BaseRPCServerAdaptor):
    __slots__ = BaseRPCServerAdaptor.__slots__ + (
        "_processor",
        "_protocol_cls",
        "_exec_timeout",
    )

    def __init__(
        self, peer: Peer, service, handler, *, exec_timeout: Optional[float] = None
    ) -> None:
        super().__init__(peer)
        self._processor = TAsyncProcessor(service, handler)
        self._protocol_cls = TAsyncBinaryProtocol
        self._exec_timeout = exec_timeout

    async def handle_function(self, request: RPCMessage) -> bytes:
        assert request.body is not None
        reader_trans = AsyncBytesIO(request.body)
        writer_trans = AsyncBytesIO()
        iproto = self._protocol_cls(reader_trans)
        oproto = self._protocol_cls(writer_trans)
        try:
            async with asyncio.timeout(self._exec_timeout):
                await self._processor.process(iproto, oproto)
        except (asyncio.IncompleteReadError, ConnectionError):
            logger.debug("client has closed the connection")
            writer_trans.close()
        except asyncio.TimeoutError:
            logger.debug("timeout when processing the client request")
            writer_trans.close()
            raise
        except asyncio.CancelledError:
            logger.debug("service cancelled")
            writer_trans.close()
            raise
        except Exception:
            writer_trans.close()
            raise
        finally:
            reader_trans.close()
        # Even with oneway requests,
        # Callosum will respond with an empty body.
        response_body = writer_trans.getvalue()
        writer_trans.close()
        return response_body


class ThriftClientAdaptor(BaseRPCClientAdaptor):
    __slots__ = BaseRPCClientAdaptor.__slots__ + (
        "_service",
        "_protocol_cls",
        "_seqid",
    )

    def __init__(self, service):
        super().__init__()
        self._service = service
        self._protocol_cls = TAsyncBinaryProtocol
        self._seqid = 0

    async def _call(self, method: str, args: Sequence[Any], kwargs):
        reader_trans = AsyncBytesIO()
        writer_trans = AsyncBytesIO()
        iproto = self._protocol_cls(reader_trans)
        oproto = self._protocol_cls(writer_trans)
        try:
            self._seqid += 1

            # Write to the output buffer with Thrift.
            kw = args_to_kwargs(
                getattr(self._service, method + "_args").thrift_spec, *args
            )
            kwargs.update(kw)
            oproto.write_message_begin(method, TMessageType.CALL, self._seqid)
            api_args = getattr(self._service, method + "_args")()
            for k, v in kwargs.items():
                setattr(api_args, k, v)
            api_args.write(oproto)
            oproto.write_message_end()

            # Switch over the control to Callosum so that it can perform send/recv
            # with its own lower transport layer.
            raw_request_body = writer_trans.getvalue()
            raw_response_body = yield raw_request_body
            if raw_response_body is None:
                yield None
                return
            reader_trans.write(raw_response_body)
            reader_trans.seek(0, io.SEEK_SET)

            # Now read the response buffer with Thrift.
            fname, mtype, rseqid = await iproto.read_message_begin()
            if rseqid != self._seqid:
                raise TApplicationException(
                    TApplicationException.BAD_SEQUENCE_ID,
                    fname + " failed: out of sequence response",
                )

            if mtype == TMessageType.EXCEPTION:
                x = TApplicationException()
                await iproto.read_struct(x)
                await iproto.read_message_end()
                raise x
            result = getattr(self._service, method + "_result")()
            await iproto.read_struct(result)
            await iproto.read_message_end()

            if hasattr(result, "success") and result.success is not None:
                yield result.success
            if len(result.thrift_spec) == 0:
                yield None
            for k, v in result.__dict__.items():
                if k != "success" and v:
                    raise v
        finally:
            writer_trans.close()
            reader_trans.close()
