from typing import Any, Awaitable, Callable

ConsumerCallback = Callable[[Any], Awaitable[None]]
