import abc


class AbstractMessagingMixin(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def recv_message(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def send_message(self, msg):
        raise NotImplementedError


class AbstractStreamingMixin(metaclass=abc.ABCMeta):

    # TODO: define
    pass


class AbstractConnection(AbstractMessagingMixin, AbstractStreamingMixin,
                         metaclass=abc.ABCMeta):
    '''
    An abstract interface for communication operations, except its lifecycle
    management operations which are responsible to binder and connector.
    '''
    pass


class AbstractBinder(metaclass=abc.ABCMeta):

    __slots__ = ('transport', 'addr')

    def __init__(self, transport, addr):
        self.transport = transport
        self.addr = addr

    @abc.abstractmethod
    async def __aenter__(self) -> AbstractConnection:
        '''
        Create a listening connection bound on self.addr.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        '''
        Close the listening connection.
        '''
        pass


class AbstractConnector(metaclass=abc.ABCMeta):

    __slots__ = ('transport', 'addr')

    def __init__(self, transport, addr):
        self.transport = transport
        self.addr = addr

    @abc.abstractmethod
    async def __aenter__(self) -> AbstractConnection:
        '''
        Return a connection to self.addr.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        '''
        Close/release the connection.
        '''
        pass


class BaseTransport(metaclass=abc.ABCMeta):

    __slots__ = ('authenticator', )

    binder_cls = None
    connector_cls = None

    def __init__(self, authenticator, **kwargs):
        self.authenticator = authenticator

    def bind(self, bind_addr):
        return self.binder_cls(self, bind_addr)

    def connect(self, connect_addr):
        return self.connector_cls(self, connect_addr)

    async def close(self):
        '''
        Close all open connections and release system resources.
        This may be left empty for transports without connection pooling or
        persistent connections.
        '''
        pass
