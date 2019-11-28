from typing import Any

import msgpack


def mpackb(v: Any) -> bytes:
    return msgpack.packb(v, use_bin_type=True)


def munpackb(b: bytes) -> Any:
    return msgpack.unpackb(b, raw=False, use_list=False)
