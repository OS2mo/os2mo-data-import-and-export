import asyncio
from functools import wraps
from typing import Awaitable, Callable, TypeVar

CallableReturnType = TypeVar("CallableReturnType")


def async_to_sync(
    func: Callable[..., Awaitable[CallableReturnType]]
) -> Callable[..., CallableReturnType]:
    """Decorator to run an async function to completion.

    Example:

        @async_to_sync
        async def sleepy(seconds):
            await asyncio.sleep(seconds)
            return seconds

        print(sleepy(5))  # --> 5

    Args:
        func (async function): The asynchronous function to wrap.

    Returns:
        :obj:`sync function`: The synchronous function wrapping the async one.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> CallableReturnType:
        return asyncio.run(func(*args, **kwargs))

    return wrapper
