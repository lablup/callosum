from typing import Any, Protocol

from .exceptions import RPCError, RPCUserError, RPCInternalError
from .message import RPCMessage
from .channel import Peer


class FunctionHandler(Protocol):
    def __call__(self, request: RPCMessage, /) -> Any:  # noqa: E225
        ...


__all__ = (
    'RPCError',
    'RPCUserError',
    'RPCInternalError',
    'RPCMessage',
    'Peer',
    'FunctionHandler',
)
