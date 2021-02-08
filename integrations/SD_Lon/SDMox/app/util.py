import asyncio
from functools import wraps
from functools import lru_cache

from os2mo_helpers.mora_helpers import MoraHelper

from config import get_settings


def get_mora_helper(mora_url=None):
    mora_url = mora_url or get_settings().mora_url
    return MoraHelper(hostname=mora_url, use_cache=False)


@lru_cache
def get_organisation_uuid(mora_url=None):
    mora_helper = get_mora_helper(mora_url=mora_url)
    org_uuid = mora_helper.read_organisation()


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
