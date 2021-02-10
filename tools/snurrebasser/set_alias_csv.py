import asyncio
import json
import random
import string
import csv
from tqdm import tqdm
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


def construct_edit_payload(mo_user_uuid, nickname_givenname, nickname_surname):
    from_time = date.today().isoformat()

    return {
        "type": "employee",
        "data": {
            "validity": {"from": from_time, "to": None},
            "nickname_givenname": nickname_givenname,
            "nickname_surname": nickname_surname,
        },
        "uuid": mo_user_uuid,
    }


async def set_aliases(edit_payload, client, mo_url):
    url = "/service/details/edit"
    async with client.post(mo_url + url, json=edit_payload) as response:
        response.raise_for_status()
        payloads = await response.json()
        return payloads


@click.command()
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
    "--verify-name",
    default=False,
    is_flag=True,
    help="Verify name.",
    show_default=True,
    type=click.BOOL,
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dry-run without changes.",
    show_default=True,
    type=click.BOOL,
)
@click.argument('csv_file', type=click.Path(exists=True))
@async_to_sync
async def alias(mo_url, saml_token, verify_name, dry_run, csv_file):
    """Update a number of employees in MO with random aliases."""
    rows = []
    with open(csv_file, "r", encoding='UTF-16') as csv_file:
        csvreader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        rows = [row for row in csvreader]

    # Prepare headers
    headers = {}
    if saml_token:
        headers["Session"] = str(saml_token)

    try:
        async with aiohttp.ClientSession(headers=headers) as client:
            # Fetch root org uuid
            root_org_uuid = await find_root_org_uuid(client, mo_url)
            # Fetch list of users
            edit_payloads = []
            for row in tqdm(rows):
                csv_cpr = row['extensionattribute3']
                # givenname, surname, displayname, cpr, samaccountname = row
                found_users = await search_employees(
                    root_org_uuid, csv_cpr, client, mo_url
                )
                if len(found_users) == 0:
                    print("Unable to find user:", csv_cpr)
                    continue
                elif len(found_users) > 1:
                    print("Found multiple users:", csv_cpr)
                    continue
                mo_user = found_users[0]
                csv_givenname, csv_surname = itemgetter('givenname', 'sn')(row)
                mo_givenname, mo_surname = itemgetter('givenname', 'surname')(mo_user)
                if verify_name and csv_givenname != mo_givenname:
                    print("Givenname mismatch", csv_givenname, "!=", mo_givenname)
                    continue
                if verify_name and csv_surname != mo_surname:
                    print("Surname mismatch", csv_surname, "!=", mo_surname)
                    continue
                csv_displayname = row['displayname']
                nickname_givenname, nickname_surname = csv_displayname.rsplit(' ', 1)
                edit_payload = construct_edit_payload(mo_user['uuid'], nickname_givenname, nickname_surname)
                edit_payloads.append(edit_payload)
            print(len(edit_payloads), "payloads prepared!")
            # If dry-run, print payload and exit
            if dry_run:
                print(json.dumps(edit_payloads, indent=2))
                return
            ## Fire payload, and make changes in MO
            # print(await set_aliases(edit_payloads, client, mo_url))
    except aiohttp.client_exceptions.InvalidURL:
        raise click.ClickException("mo_url seems malformed!")
    except aiohttp.client_exceptions.ClientResponseError:
        raise click.ClickException("mo_url does not behave like a MO server")


if __name__ == "__main__":
    alias()
