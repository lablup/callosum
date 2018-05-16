import pytest

from callosum import Peer


def test_init():
    p = Peer(connect='tcp://127.0.0.1:5000')

    assert p._connect == 'tcp://127.0.0.1:5000'
    assert p._max_concurrency > 0
    assert p._invoke_timeout is None or p._invoke_timeout > 0
    assert p._exec_timeout is None or p._exec_timeout > 0
    assert len(p._func_registry) == 0
    assert len(p._stream_registry) == 0


def test_func_registry():
    p = Peer(connect='tcp://127.0.0.1:5000')

    def dummy():
        pass

    assert 'dummy' not in p._func_registry
    assert 'dummy' not in p._stream_registry
    p.handle_function('dummy', dummy)
    assert 'dummy' in p._func_registry
    assert 'dummy' not in p._stream_registry
    assert p._func_registry['dummy'] is dummy


def test_stream_registry():
    p = Peer(connect='tcp://127.0.0.1:5000')

    def dummy():
        pass

    assert 'dummy' not in p._func_registry
    assert 'dummy' not in p._stream_registry
    p.handle_stream('dummy', dummy)
    assert 'dummy' not in p._func_registry
    assert 'dummy' in p._stream_registry
    assert p._stream_registry['dummy'] is dummy
