import asyncio
from datetime import date
from functools import lru_cache
from operator import itemgetter
from typing import cast
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import config
from aiohttp import ClientResponse
from aiohttp import ClientSession
from aiohttp import ClientTimeout
from aiohttp import TCPConnector
from more_itertools import chunked
from mox_helpers.mox_helper import create_mox_helper
from mox_helpers.mox_helper import MoxHelper
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.headers import TokenSettings
from ra_utils.tqdm_wrapper import tqdm


def get_tcp_connector():
    settings = config.get_config()
    return TCPConnector(limit=settings.max_concurrent_requests)


def get_client_session():
    return ClientSession(
        connector=get_tcp_connector(), timeout=ClientTimeout(total=None)
    )


async def create_details(
    session: ClientSession, detail_payloads: Iterable[dict]
) -> None:
    """Helper function for submitting create detail payloads"""
    url = "/service/details/create"
    await submit_payloads(session, url, detail_payloads, "create details")


async def edit_details(session: ClientSession, detail_payloads: Iterable[dict]) -> None:
    """Helper function for submitting edit detail payloads"""
    url = "/service/details/edit"
    await submit_payloads(session, url, detail_payloads, "edit details")


async def terminate_details(
    session: ClientSession,
    detail_payloads: Iterable[dict],
    ignored_http_statuses: Optional[Tuple[int]] = (404,),
) -> None:
    """Helper function for submitting terminate detail payloads"""
    url = "/service/details/terminate"
    await submit_payloads(
        session,
        url,
        detail_payloads,
        "terminate details",
        ignored_http_statuses=ignored_http_statuses,
    )


async def create_it(payload: dict, obj_uuid: str, mox_helper: MoxHelper) -> None:
    """Helper function for reating an IT system"""
    await mox_helper.insert_organisation_itsystem(payload, obj_uuid)


async def create_klasse(payload: dict, obj_uuid: str, mox_helper: MoxHelper) -> None:
    """Helper function for creating a Klasse object"""
    await mox_helper.insert_klassifikation_klasse(payload, obj_uuid)


async def raise_on_unhandled_mo_error(
    endpoint: str,
    doc: Iterable[dict],
    response: ClientResponse,
    ignored_mo_error_keys: Tuple[str] = ("V_DUPLICATED_IT_USER",),
) -> None:
    """Raise an exception on any MO errors that we do not know to handle.

    :param endpoint: The MO endpoint that was called
    :param doc: The payload used when calling the MO endpoint
    :param response: The MO response returned by the endpoint
    :param ignored_mo_error_keys: Zero or more MO error keys to ignore, if encountered
    """
    if not response.ok:
        # Print out the MO error response to ease debugging
        mo_error = await response.json()
        print(
            f'Posting "{doc}" to "{endpoint}" resulted in MO error '
            f'response "{mo_error}"'
        )
        # Examine the MO error to see if it should be ignored
        mo_error_key = mo_error.get("error_key")
        needs_check = mo_error_key and ignored_mo_error_keys
        if needs_check and mo_error_key in ignored_mo_error_keys:
            print(f'Continuing on ignored MO error "{mo_error_key}"')
            return

    response.raise_for_status()


async def submit_payloads(
    session: ClientSession,
    endpoint: str,
    payloads: Iterable[dict],
    description: str,
    ignored_http_statuses: Optional[Tuple[int]] = None,
) -> None:
    """
    Send a list of payloads to OS2mo. The payloads are chunked based on preset variable
    and submitted concurrently.

    :param session: A aiohttp session
    :param endpoint: Which endpoint to send the payloads to
    :param payloads: An iterable of dict payloads
    :param description: A description to print as part of the output
    :param ignored_http_statuses: A tuple of HTTP status codes to ignore, if encountered
    """
    settings = config.get_config()
    base_url = settings.mora_base
    headers = TokenSettings().get_headers()

    async def submit(data: List[dict]) -> None:
        doc = list(data)
        # Use semaphore to throttle the amount of concurrent requests
        async with session.post(
            base_url + endpoint,
            params={"force": 1},
            json=doc,
            headers=headers,
        ) as response:
            if ignored_http_statuses and response.status in ignored_http_statuses:
                print(f"{endpoint} returned status {response.status}, ignoring")
            else:
                await raise_on_unhandled_mo_error(endpoint, doc, response)

    chunks = chunked(payloads, settings.os2mo_chunk_size)
    tasks = list(map(submit, chunks))
    if len(tasks) == 0:
        return

    for f in tqdm(
        asyncio.as_completed(tasks), total=len(tasks), unit="chunk", desc=description
    ):
        await f


async def lookup_organisationfunktion():
    """Helper function for fetching all available 'organisationfunktion' objects."""
    settings = config.get_config()
    mox = await create_mox_helper(settings.mox_base)
    return await mox.search_organisation_organisationfunktion(params={"bvn": "%"})


def lookup_employees():
    """Helper function for fetching all available 'employee' objects."""
    settings = config.get_config()
    mh = MoraHelper(hostname=settings.mora_base, export_ansi=True)
    return mh.read_all_users()


@lru_cache(maxsize=0)
def build_cpr_map() -> Dict[str, str]:
    employees = lookup_employees()
    cache = cast(
        Iterator[Tuple[str, str]],
        map(
            itemgetter("cpr_no", "uuid"),
            filter(lambda employee: employee.get("cpr_no"), employees),
        ),
    )
    return dict(cache)


def convert_validities(from_time: date, to_time: date) -> Tuple[str, Optional[str]]:
    from_time_str = from_time.isoformat()
    to_time_str = to_time.isoformat()
    return from_time_str, to_time_str if to_time_str != "9999-12-31" else None
