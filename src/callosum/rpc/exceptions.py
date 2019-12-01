from ..exceptions import CallosumError


class RPCError(CallosumError):
    '''
    A base exception for all RPC-specific errors.
    '''
    pass


class RPCUserError(RPCError):
    '''
    Represents an error caused in user-defined handlers.
    '''

    name: str
    traceback: str

    def __init__(self, name: str, tb: str, *args):
        super().__init__(name, tb, *args)
        self.name = name
        self.traceback = tb


class RPCInternalError(RPCError):
    '''
    Represents an error caused in Calloum's internal logic.
    '''

    name: str
    traceback: str

    def __init__(self, name: str, tb: str, *args):
        super().__init__(name, tb, *args)
        self.name = name
        self.traceback = tb
