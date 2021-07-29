import asyncio
import csv
import hashlib
import uuid
from datetime import datetime
from ftplib import FTP
from functools import lru_cache
from operator import itemgetter
from typing import Callable
from typing import Iterable
from typing import List

import tqdm
from aiohttp import ClientSession
from more_itertools import chunked
from more_itertools import one
from mox_helpers.mox_helper import create_mox_helper
from mox_helpers.mox_helper import MoxHelper
from os2mo_helpers.mora_helpers import MoraHelper

import config
# The amount of concurrent requests made to OS2mo

SEM_SIZE = 16
# Size of payload chunks sent to OS2mo
DEFAULT_CHUNK_SIZE = 100

sem = asyncio.Semaphore(SEM_SIZE)


@lru_cache(maxsize=None)
def generate_uuid(seed):
    """
    Generate an UUID based on a seed in a deterministic way
    This allows us generate the same uuid for objects across different imports,
    without having to maintain a separate list of UUIDs, or fetch the relevant uuids
    from MO
    """
    m = hashlib.md5()
    m.update(seed.encode("utf-8"))
    return str(uuid.UUID(m.hexdigest()))


def read_csv(filename: str, model: Callable):
    """Read CSV file from FTP into list of model objects"""
    print(f"Processing {filename}")

    ftp = get_ftp_connector()

    lines: List[str] = []
    ftp.retrlines(f"RETR {filename}", lines.append)

    reader = csv.DictReader(lines, delimiter=";")
    return [model(**row) for row in reader]


async def create_details(session: ClientSession, detail_payloads: Iterable[dict]):
    """Helper function for submitting create detail payloads"""
    url = "/service/details/create"
    await submit_payloads(session, url, detail_payloads, "create details")


async def edit_details(session: ClientSession, detail_payloads: Iterable[dict]):
    """Helper function for submitting edit detail payloads"""
    url = "/service/details/edit"
    await submit_payloads(session, url, detail_payloads, "edit details")


async def terminate_details(session: ClientSession, detail_payloads: Iterable[dict]):
    """Helper function for submitting terminate detail payloads"""
    url = "/service/details/terminate"
    await submit_payloads(session, url, detail_payloads, "terminate details")


async def create_it(payload: dict, obj_uuid: str, mox_helper: MoxHelper):
    """Helper function for reating an IT system"""
    await mox_helper.insert_organisation_itsystem(payload, obj_uuid)


async def create_klasse(payload: dict, obj_uuid: str, mox_helper: MoxHelper):
    """Helper function for creating a Klasse object"""
    await mox_helper.insert_klassifikation_klasse(payload, obj_uuid)


async def submit_payloads(
    session: ClientSession, endpoint: str, payloads: Iterable[dict], description: str
):
    """
    Send a list of payloads to OS2mo. The payloads are chunked based on preset variable
    and submitted concurrently.

    :param session: A aiohttp session
    :param endpoint: Which endpoint to send the payloads to
    :param payloads: An iterable of dict payloads
    :param description: A description to print as part of the output
    """
    settings = config.get_config()
    base_url = settings.mora_base
    headers = {"session": settings.saml_token}

    async def submit(data):
        # Use semaphore to throttle the amount of concurrent requests
        async with sem:
            async with session.post(
                base_url + endpoint,
                params={"force": 1},
                json=list(data),
                headers=headers,
            ) as response:
                response.raise_for_status()

    chunks = chunked(payloads, DEFAULT_CHUNK_SIZE)
    tasks = list(map(submit, chunks))
    if len(tasks) == 0:
        return

    for f in tqdm.tqdm(
        asyncio.as_completed(tasks), total=len(tasks), unit="chunk", desc=description
    ):
        await f


def parse_filenames(filenames: Iterable[str], prefix: str, last_import: datetime):
    """
    Get valid filenames matching a prefix and date newer than last_import

    All valid filenames are on the form: {{prefix}}_20210131_221600.csv
    """

    def parse_filepath(filepath):
        filepath = str(filepath)

        date_part = filepath[-19:-4]
        parsed_datetime = datetime.strptime(date_part, "%Y%m%d_%H%M%S")

        return filepath, parsed_datetime

    filtered_names = filter(lambda x: x.startswith(prefix), filenames)
    parsed_names = map(parse_filepath, filtered_names)
    # Only use files that are newer than last import
    new_files = filter(lambda x: x[1] > last_import, parsed_names)
    sorted_filenames = sorted(new_files, key=itemgetter(1))
    return sorted_filenames


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


def get_modified_datetime_for_file(filename: str) -> datetime:
    """Read the 'modified' field from an FTP file"""
    ftp = get_ftp_connector()
    files = ftp.mlsd()
    found_file = one(filter(lambda x: x[0] == filename, files))
    filename, facts = found_file
    # String is on the form: "20210323153241.448"
    modify_string = facts["modify"][:-4]
    return datetime.strptime(modify_string, "%Y%m%d%H%M%S")


def get_ftp_connector() -> FTP:
    """Helper function for fetching an FTP connector for the configured ftp server"""
    settings = config.get_config()
    ftp = FTP(settings.ftp_url)
    ftp.encoding = "utf-8"
    ftp.login(user=settings.ftp_user, passwd=settings.ftp_pass)
    return ftp
