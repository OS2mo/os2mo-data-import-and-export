from typing import List, Optional, Tuple

import asyncio
from collections import ChainMap
from functools import partial
from operator import itemgetter

from aiohttp import ClientSession, TCPConnector
from more_itertools import chunked, unzip

from ra_utils.async_to_sync import async_to_sync


def ensure_session(func):
    async def _decorator(*args, **kwargs):
        if "client" in kwargs:
            return await func(*args, **kwargs)
        else:
            # DAWA only accepts:
            # * 30 requests per second per IP
            # * 10 concurrent connections per IP
            # Thus we limit our connections to 10 here.
            connector = TCPConnector(limit=10)
            async with ClientSession(connector=connector) as client:
                return await func(*args, **kwargs, client=client)

    return _decorator


@ensure_session
async def dar_fetch_non_chunked(uuids, addrtype, client=None):
    """Lookup uuids in DAR (without chunking).

    Args:
        uuids: List of DAR UUIDs to lookup.
        addr_type: The address type to lookup.
        client (optional): aiohttp.ClientSession to use for connecting.

    Returns:
        (dict, set):
            dict: Map from UUID to DAR reply.
            set: Set of UUIDs of entries which were not found.
    """
    url = "https://dawa.aws.dk/" + addrtype
    params = {"id": "|".join(uuids), "struktur": "mini"}

    async with client.get(url, params=params) as response:
        response.raise_for_status()
        body = await response.json()

        result = {addr['id']: addr for addr in body}

        found_uuids = set(map(itemgetter("id"), body))
        missing = set(uuids) - found_uuids

        return result, missing


@ensure_session
async def dar_fetch_chunked(uuids, addrtype, chunk_size, client=None):
    """Lookup uuids in DAR (chunked).

    Args:
        uuids: List of DAR UUIDs.
        addr_type: The address type to lookup.
        chunk_size: Number of UUIDs per block, sent to DAR.
        client (optional): aiohttp.ClientSession to use for connecting.

    Returns:
        (dict, set):
            dict: Map from UUID to DAR reply.
            set: Set of UUIDs of entries which were not found.
    """

    def create_task(uuid_chunk):
        return asyncio.ensure_future(
            dar_fetch_non_chunked(uuid_chunk, addrtype=addrtype, client=client)
        )

    # Chunk our UUIDs into blocks of chunk_size
    uuid_chunks = chunked(uuids, chunk_size)
    # Convert chunks into a list of asyncio.tasks
    tasks = list(map(create_task, uuid_chunks))
    # Here 'result' is a list of tuples (dict, set) => (result, missing)
    result = await asyncio.gather(*tasks)
    # First we unzip 'result' to get a list of results and a list of missing
    result_dicts, missing_sets = unzip(result)
    # Then we union the dicts and sets before returning
    combined_result = dict(ChainMap(*result_dicts))
    combined_missing = set.union(*missing_sets)
    return combined_result, combined_missing


@ensure_session
async def dar_fetch(uuids, addrtype="adresser", chunk_size=150, client=None):
    """Lookup uuids in DAR (chunked if required).

    Args:
        uuids: List of DAR UUIDs.
        addr_type: The address type to lookup.
        chunk_size: Number of UUIDs per block, sent to DAR.
        client (optional): aiohttp.ClientSession to use for connecting.

    Returns:
        (dict, set):
            dict: Map from UUID to DAR reply.
            set: Set of UUIDs of entries which were not found.
    """
    num_uuids = len(uuids)
    if num_uuids == 0:
        return dict(), set()
    elif num_uuids <= 150:
        return await dar_fetch_non_chunked(uuids, addrtype, client=client)
    else:
        return await dar_fetch_chunked(uuids, addrtype, chunk_size, client=client)


@async_to_sync
async def sync_dar_fetch(uuids, addrtype="adresser", chunk_size=150):
    """Syncronized version of dar_fetch."""
    return await dar_fetch(uuids, addrtype, chunk_size)


@ensure_session
async def dar_datavask(address: str, client: ClientSession) -> Tuple[str, Optional[dict]]:
    """
    Perform search in DAR using the 'datavask' API
    """
    url = "https://api.dataforsyningen.dk/datavask/adresser"
    params = {"struktur": "mini", "betegnelse": address}
    async with client.get(url, params=params) as response:
        response.raise_for_status()
        result = await response.json()
        # A and B are safe matches, and will only have one result in the array
        # https://dawadocs.dataforsyningen.dk/dok/api/adresse#datavask
        if result['kategori'] in ['A', 'B']:
            return address, result['resultater'][0]['adresse']['id']

    return address, None


@ensure_session
async def dar_datavask_multiple(addresses: List[str], client: ClientSession) -> List[Tuple[str, Optional[dict]]]:
    """Perform search in DAR on multiple address strings"""
    tasks = map(partial(dar_datavask, client=client), addresses)
    results = await asyncio.gather(*tasks)
    return results
