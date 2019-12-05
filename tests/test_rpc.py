import json

from callosum.rpc.message import (
    NullMetadata, ErrorMetadata,
    RPCMessage, RPCMessageTypes,
)


def test_metadata_serialization():
    orig = NullMetadata()
    data = orig.encode()
    out = NullMetadata.decode(data)
    assert out == orig

    orig = ErrorMetadata('MyError', 'this is a long traceback')
    data = orig.encode()
    out = ErrorMetadata.decode(data)
    assert out == orig


def test_rpcmessage_exception_serialization():
    request = RPCMessage(
        peer_id=None,
        msgtype=RPCMessageTypes.FUNCTION,
        method='dummy_function',
        order_key='x',
        seq_id=1000,
        metadata=NullMetadata(),
        body=b'{}',
    )
    try:
        raise ZeroDivisionError('oops')
    except Exception:
        failure_msg = RPCMessage.failure(request)
        assert failure_msg.msgtype == RPCMessageTypes.FAILURE
        data = failure_msg.encode(json.dumps)
        decoded_failure_msg = RPCMessage.decode(data, json.loads)
        assert decoded_failure_msg == failure_msg
