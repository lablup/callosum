import functools


class BaseServerAdaptor:

    __slots__ = ('peer', )

    def __init__(self, peer):
        self.peer = peer

    async def handle_function(self, request):
        raise NotImplementedError

    async def handle_stream(self, request):
        raise NotImplementedError


class BaseClientAdaptor:

    __slots__ = ()

    def __init__(self):
        pass

    async def _call(self, reader, writer, send_hook, method, args, kwargs):
        raise NotImplementedError

    def __getattr__(self, name):
        def _caller(*args, **kwargs):
            return functools.partial(self._call,
                                     method=name,
                                     args=args,
                                     kwargs=kwargs)
        return _caller
