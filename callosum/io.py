import io


class AsyncBytesIO(io.BytesIO):

    '''
    An async wrapper for in-memory bytes stream.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._drain_hooks = []

    async def readexactly(self, n):
        return self.read(n)

    async def drain(self):
        for hook in self._drain_hooks:
            await hook()
        self._drain_hooks.clear()

    def add_drain_hook(self, hook):
        self._drain_hooks.append(hook)
