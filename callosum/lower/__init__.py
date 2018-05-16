import abc


class BaseTransport(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def connect(self, connect_addr):
        raise NotImplementedError

    @abc.abstractmethod
    async def bind(self, bind_addr):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def closed(self):
        return True

    async def close(self):
        pass

    @abc.abstractmethod
    async def recv_message(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def send_message(self, msg):
        raise NotImplementedError
