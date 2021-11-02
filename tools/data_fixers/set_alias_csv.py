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
        payloads = await response.json()
        if response.status == 400:
            if "description" not in payloads:
                response.raise_for_status()
            if "raise to a new registration." not in payloads["description"]:
                response.raise_for_status()
            return False
        response.raise_for_status()
        return True


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
@click.argument("csv_file", type=click.Path(exists=True))
@async_to_sync
async def alias(mo_url, saml_token, verify_name, dry_run, csv_file):
    """Update a number of employees in MO with random aliases."""
    rows = []
    with open(csv_file, "r", encoding="iso-8859-1") as csv_file:
        csvreader = csv.DictReader(csv_file, delimiter=";", quotechar='"')
        rows = [row for row in csvreader]

    # Prepare headers
    headers = {}
    if saml_token:
        headers["Session"] = str(saml_token)

    # CPR;Firstname;Lastname;Displayname;MO.cpr;MO.firstName;MO.lastName

    status = {
        "unable_to_find_user": 0,
        "found_multiple_users": 0,
        "cpr_mismatch": 0,
        "mo_name_mismatch_givenname": 0,
        "mo_name_mismatch_surname": 0,
        "csv_name_mismatch_givenname": 0,
        "csv_name_mismatch_surname": 0,
        "succes": 0,
        "no_change": 0,
    }

    try:
        async with aiohttp.ClientSession(headers=headers) as client:
            # Fetch root org uuid
            root_org_uuid = await find_root_org_uuid(client, mo_url)
            # Fetch list of users
            for row in tqdm(rows):
                csv_cpr = row["CPR"].zfill(10)
                mo_cpr = row["MO.cpr"].zfill(10)
                # givenname, surname, displayname, cpr, samaccountname = row
                found_users = await search_employees(
                    root_org_uuid, csv_cpr, client, mo_url
                )
                if len(found_users) == 0:
                    print("Unable to find user:", csv_cpr)
                    status["unable_to_find_user"] += 1
                    continue
                elif len(found_users) > 1:
                    print("Found multiple users:", csv_cpr)
                    status["found_multiple_users"] += 1
                    continue
                mo_user = found_users[0]
                csv_givenname, csv_surname = itemgetter("Firstname", "Lastname")(row)
                csv_mo_givenname, csv_mo_surname = itemgetter(
                    "MO.firstName", "MO.lastName"
                )(row)
                mo_givenname, mo_surname = itemgetter("givenname", "surname")(mo_user)
                if verify_name and csv_cpr != mo_cpr:
                    print("CPR mismatch", csv_cpr, "!=", mo_cpr)
                    status["cpr_mismatch"] += 1
                if verify_name and csv_mo_givenname != mo_givenname:
                    print("MO Givenname mismatch", csv_mo_givenname, "!=", mo_givenname)
                    status["mo_name_mismatch_givenname"] += 1
                if verify_name and csv_mo_surname != mo_surname:
                    print("MO Surname mismatch", csv_mo_surname, "!=", mo_surname)
                    status["mo_name_mismatch_surname"] += 1
                if verify_name and csv_givenname not in mo_givenname:
                    print("Givenname mismatch", csv_givenname, "!=", mo_givenname)
                    status["csv_name_mismatch_givenname"] += 1
                if verify_name and csv_surname not in mo_surname:
                    print("Surname mismatch", csv_surname, "!=", mo_surname)
                    status["csv_name_mismatch_surname"] += 1
                csv_displayname = row["Displayname"]
                nickname_givenname, nickname_surname = csv_displayname.rsplit(" ", 1)
                edit_payload = construct_edit_payload(
                    mo_user["uuid"], nickname_givenname, nickname_surname
                )
                # If dry-run, print payload and exit
                if dry_run:
                    print(json.dumps(edit_payload, indent=2))
                    continue
                update_successful = await set_aliases([edit_payload], client, mo_url)
                status["succes" if update_successful else "no_change"] += 1
    except aiohttp.client_exceptions.InvalidURL:
        raise click.ClickException("mo_url seems malformed!")
    except aiohttp.client_exceptions.ClientResponseError as exp:
        print(exp)
        raise click.ClickException("mo_url does not behave like a MO server")
    print(json.dumps(status, indent=4))


if __name__ == "__main__":
    alias()
