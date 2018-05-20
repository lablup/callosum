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


class BaseBinder(AbstractMessagingMixin, AbstractStreamingMixin,
                 metaclass=abc.ABCMeta):

    def __init__(self, transport, addr):
        self.transport = transport
        self.addr = addr

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        pass


class BaseConnector(AbstractMessagingMixin, AbstractStreamingMixin,
                    metaclass=abc.ABCMeta):

    def __init__(self, transport, addr):
        self.transport = transport
        self.addr = addr

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        pass


class BaseTransport(metaclass=abc.ABCMeta):

    binder_cls = None
    connector_cls = None

    def bind(self, bind_addr):
        return self.binder_cls(self, bind_addr)

    def connect(self, connect_addr):
        return self.connector_cls(self, connect_addr)

    async def close(self):
        pass
