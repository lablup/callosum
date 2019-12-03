import io


class AsyncBytesIO:
    '''
    An async wrapper for in-memory bytes stream.
    '''

    def __init__(self, initial_data: bytes = b'') -> None:
        self._buf = io.BytesIO(initial_data)

    async def open(self) -> None:
        pass

    def close(self) -> None:
        self._buf.close()

    async def flush(self) -> None:
        pass

    async def readexactly(self, n: int) -> bytes:
        return self._buf.read(n)

    async def read(self, n: int) -> bytes:
        return self._buf.read(n)

    def write(self, val: bytes) -> None:
        self._buf.write(val)

    def getvalue(self) -> bytes:
        return self._buf.getvalue()

    def seek(self, pos: int, flag: int) -> None:
        self._buf.seek(pos, flag)
