import asyncio
import csv
import hashlib
import uuid
from datetime import datetime
from datetime import date
from ftplib import FTP
from functools import lru_cache
from operator import itemgetter
from typing import Iterable
from typing import List
from typing import Tuple

import pydantic
import tqdm
from aiohttp import ClientSession, ClientTimeout
from aiohttp import TCPConnector
from more_itertools import chunked
from more_itertools import one
from pydantic import parse_obj_as
from ra_utils.apply import apply

from mox_helpers.mox_helper import create_mox_helper
from mox_helpers.mox_helper import MoxHelper
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.headers import TokenSettings

import config


def get_tcp_connector():
    settings = config.get_config()
    return TCPConnector(limit=settings.max_concurrent_requests)


def get_client_session():
    return ClientSession(
        connector=get_tcp_connector(), timeout=ClientTimeout(total=None)
    )


def read_csv(filename: str, model: pydantic.BaseModel) -> List[pydantic.BaseModel]:
    """Read CSV file from FTP into list of model objects"""
    print(f"Processing {filename}")

    ftp = get_ftp_connector()

    lines: List[str] = []
    ftp.retrlines(f"RETR {filename}", lines.append)

    def strip_empty(val: dict):
        return {k: v for k, v in val.items() if v != ""}

    reader = csv.DictReader(lines, delimiter=";")
    parsed = map(strip_empty, reader)
    return parse_obj_as(List[model], list(parsed))


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
    session: ClientSession, detail_payloads: Iterable[dict]
) -> None:
    """Helper function for submitting terminate detail payloads"""
    url = "/service/details/terminate"
    await submit_payloads(session, url, detail_payloads, "terminate details")


async def create_it(payload: dict, obj_uuid: str, mox_helper: MoxHelper) -> None:
    """Helper function for reating an IT system"""
    await mox_helper.insert_organisation_itsystem(payload, obj_uuid)


async def create_klasse(payload: dict, obj_uuid: str, mox_helper: MoxHelper) -> None:
    """Helper function for creating a Klasse object"""
    await mox_helper.insert_klassifikation_klasse(payload, obj_uuid)


async def submit_payloads(
    session: ClientSession, endpoint: str, payloads: Iterable[dict], description: str
) -> None:
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
    headers = TokenSettings().get_headers()

    async def submit(data: List[dict]) -> None:
        # Use semaphore to throttle the amount of concurrent requests
        async with session.post(
            base_url + endpoint,
            params={"force": 1},
            json=list(data),
            headers=headers,
        ) as response:
            response.raise_for_status()

    chunks = chunked(payloads, settings.os2mo_chunk_size)
    tasks = list(map(submit, chunks))
    if len(tasks) == 0:
        return

    for f in tqdm.tqdm(
        asyncio.as_completed(tasks), total=len(tasks), unit="chunk", desc=description
    ):
        await f


def parse_filenames(
    filenames: Iterable[str], prefix: str, last_import: datetime
) -> List[Tuple[str, datetime]]:
    """
    Get valid filenames matching a prefix and date newer than last_import

    All valid filenames are on the form: {{prefix}}_20210131_221600.csv
    """

    def parse_filepath(filepath: str) -> Tuple[str, datetime]:
        date_part = filepath[-19:-4]
        parsed_datetime = datetime.strptime(date_part, "%Y%m%d_%H%M%S")

        return filepath, parsed_datetime

    filtered_names = filter(lambda x: x.startswith(prefix), filenames)
    parsed_names = map(parse_filepath, filtered_names)
    # Only use files that are newer than last import
    new_files = filter(
        apply(lambda filepath, filedate: filedate > last_import), parsed_names
    )
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
    ftp.cwd(settings.ftp_folder)
    return ftp


def convert_validities(from_time: date, to_time: date) -> Tuple[str, str]:
    from_time_str = from_time.isoformat()
    to_time_str = to_time.isoformat()
    return from_time_str, to_time_str if to_time_str != "9999-12-31" else None
