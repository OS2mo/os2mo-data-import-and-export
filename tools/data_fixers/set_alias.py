import asyncio
import json
import random
import string
from datetime import date
from functools import wraps
from operator import itemgetter

import aiohttp
import click


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


async def find_root_org_uuid(client, mo_url):
    url = "/service/o/"
    async with client.get(mo_url + url) as response:
        response.raise_for_status()
        payloads = await response.json()
        if len(payloads) != 1:
            raise ValueError("Unexpected payload")
        payload = payloads[0]
        if "uuid" not in payload:
            raise ValueError("Unexpected payload")
        return payload["uuid"]


async def search_employees(org_uuid, query, client, mo_url):
    url = "/service/o/" + org_uuid + "/e/?query=" + query
    async with client.get(mo_url + url) as response:
        response.raise_for_status()
        payloads = await response.json()
        return payloads["items"]


def construct_edit_payload(employee_uuids):
    from_time = date.today().isoformat()

    VOWELS = "aeiou"
    CONSONANTS = "".join(set(string.ascii_lowercase) - set(VOWELS))

    def generate_word(length):
        def pick_letter(i):
            return random.choice(CONSONANTS if (i % 2 == 0) else VOWELS)

        return "".join(list(map(pick_letter, range(length))))

    def generate_alias():
        return generate_word(5).capitalize(), generate_word(10).capitalize()

    def map_edit_payload(employee_uuid):
        nickname_givenname, nickname_surname = generate_alias()
        return {
            "type": "employee",
            "data": {
                "validity": {"from": from_time, "to": None},
                "nickname_givenname": nickname_givenname,
                "nickname_surname": nickname_surname,
            },
            "uuid": employee_uuid,
        }

    payloads = map(map_edit_payload, employee_uuids)
    return list(payloads)


async def set_aliases(edit_payload, client, mo_url):
    url = "/service/details/edit"
    async with client.post(mo_url + url, json=edit_payload) as response:
        response.raise_for_status()
        payloads = await response.json()
        return payloads


@click.command()
@click.option(
    "--query",
    help="Search query to find employees.",
    show_default=True,
)
@click.option(
    "--uuid",
    help="Find employees with the given uuids.",
    multiple=True,
    type=click.UUID,
)
@click.option(
    "--count",
    default=0,
    help="Max number of people to alias. 0 => All.",
    show_default=True,
)
@click.option(
    "--mo-url",
    default="http://localhost:5000",
    help="Host URL to MO.",
    show_default=True,
)
@click.option(
    "--saml-token",
    help="SAML Token to send with MO requests.",
    show_default=True,
    type=click.UUID,
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dry-run without changes.",
    show_default=True,
    type=click.BOOL,
)
@async_to_sync
async def alias(query, uuid, count, mo_url, saml_token, dry_run):
    """Update a number of employees in MO with random aliases."""
    by_query = query is not None
    by_uuid = len(uuid) != 0
    # Ensure one, and only one is set
    if not by_query and not by_uuid:
        raise click.ClickException("Either query or uuid must be set!")
    if by_query and by_uuid:
        raise click.ClickException("Cannot use both query and uuid at once!")
    # Validate query parameter
    if by_query:
        if len(query) < 2:
            raise click.ClickException("Query must be atleast 2 characters!")
    # count == 0, we want all, which means no slicing (achieved with None)
    if count == 0:
        count = None

    # Prepare headers
    headers = {}
    if saml_token:
        headers["Session"] = saml_token

    try:
        async with aiohttp.ClientSession(headers=headers) as client:
            found_users = map(str, uuid)
            if by_query:
                # Fetch root org uuid
                root_org_uuid = await find_root_org_uuid(client, mo_url)
                # Fetch list of users
                found_users = await search_employees(
                    root_org_uuid, query, client, mo_url
                )
                # Limit found_users to 'count'
                found_users = found_users[:count]
                found_users = map(itemgetter("uuid"), found_users)
            # Convert found_users to one big edit_payload
            edit_payload = construct_edit_payload(found_users)
            # If dry-run, print payload and exit
            if dry_run:
                print(json.dumps(edit_payload, indent=2))
                return
            # Fire payload, and make changes in MO
            print(await set_aliases(edit_payload, client, mo_url))
    except aiohttp.client_exceptions.InvalidURL:
        raise click.ClickException("mo_url seems malformed!")
    except aiohttp.client_exceptions.ClientResponseError:
        raise click.ClickException("mo_url does not behave like a MO server")


if __name__ == "__main__":
    alias()
