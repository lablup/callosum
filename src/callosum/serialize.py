from typing import Any, Optional

import msgpack


def mpackb(v: Any) -> bytes:
    return msgpack.packb(v, use_bin_type=True)


def munpackb(b: bytes) -> Any:
    return msgpack.unpackb(b, raw=False, use_list=False)


def noop_serializer(o: Optional[Any]) -> bytes:
    assert isinstance(o, bytes)
    return o


def noop_deserializer(b: bytes) -> Optional[Any]:
    return b
