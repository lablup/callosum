from __future__ import annotations

from typing import TYPE_CHECKING, Self

from ..exceptions import CallosumError

if TYPE_CHECKING:
    from .message import ErrorMetadata


class RPCError(CallosumError):
    """
    A base exception for all RPC-specific errors.
    """

    name: str
    repr: str
    traceback: str
    exceptions: tuple

    def __init__(self, name: str, repr_: str, tb: str, exceptions: tuple, *args):
        super().__init__(name, repr_, tb, *args)
        self.name = name
        self.repr = repr_
        self.traceback = tb
        self.exceptions = exceptions

    @classmethod
    def from_err_metadata(cls, metadata: ErrorMetadata) -> Self:
        return cls(
            metadata.name,
            metadata.repr,
            metadata.traceback,
            tuple(cls.from_err_metadata(err) for err in metadata.sub_errors),
        )


class RPCUserError(RPCError):
    """
    Represents an error caused in user-defined handlers.
    """


class RPCInternalError(RPCError):
    """
    Represents an error caused in Calloum's internal logic.
    """
