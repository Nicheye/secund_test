from collections.abc import Awaitable, Callable

Sleeper = Callable[[float], Awaitable[None]]
