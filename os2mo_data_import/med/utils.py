import asyncio
from functools import wraps
import collections

def async_to_sync(f):
    """Decorator to run an async function to completion.

    Example:

        @async_to_sync
        async def sleepy(seconds):
            await sleep(seconds)

        sleepy(5)
    
    Args:
        f (async function): The async function to wrap and make synchronous.

    Returns:
        :obj:`sync function`: The syncronhous function wrapping the async one.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(f(*args, **kwargs))
        return loop.run_until_complete(future)

    return wrapper


def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is None, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)


def dict_map(func, dicty):
    def unfold_call(entry):
        return func(entry[0], entry[1])

    return dict(map(unfold_call, dicty.items()))
