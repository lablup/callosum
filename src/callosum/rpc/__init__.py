from typing import Any, Protocol

from .channel import Peer
from .exceptions import RPCError, RPCInternalError, RPCUserError
from .message import RPCMessage


class FunctionHandler(Protocol):
    def __call__(self, request: RPCMessage, /) -> Any:  # noqa: E225
        ...


__all__ = (
    "RPCError",
    "RPCUserError",
    "RPCInternalError",
    "RPCMessage",
    "Peer",
    "FunctionHandler",
)
