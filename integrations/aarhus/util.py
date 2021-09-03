import asyncio
import codecs
import csv
from datetime import date
from datetime import datetime
from ftplib import FTP
from io import BytesIO
from io import StringIO
from operator import itemgetter
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar

import config
import tqdm
from aiohttp import ClientSession
from aiohttp import ClientTimeout
from aiohttp import TCPConnector
from more_itertools import chunked
from more_itertools import one
from mox_helpers.mox_helper import create_mox_helper
from mox_helpers.mox_helper import MoxHelper
from os2mo_helpers.mora_helpers import MoraHelper
from pydantic import parse_obj_as
from ra_utils.apply import apply
from ra_utils.headers import TokenSettings


T = TypeVar("T")


def get_tcp_connector():
    settings = config.get_config()
    return TCPConnector(limit=settings.max_concurrent_requests)


def get_client_session():
    return ClientSession(
        connector=get_tcp_connector(), timeout=ClientTimeout(total=None)
    )


def get_ftp_file(filename: str) -> List[str]:
    ftp = get_ftp_connector()
    lines: List[str] = []
    ftp.retrlines(f"RETR {filename}", lines.append)
    return lines


def parse_csv(lines: List[str], model: T) -> List[T]:
    def strip_empty(val: dict):
        return {k: v for k, v in val.items() if v != ""}

    reader = csv.DictReader(lines, delimiter=";")
    parsed = map(strip_empty, reader)
    return parse_obj_as(List[model], list(parsed))  # type: ignore


def read_csv(filename: str, model: T) -> List[T]:
    """Read CSV file from FTP into list of model objects"""
    print(f"Processing {filename}")
    lines = get_ftp_file(filename)
    return parse_csv(lines, model)


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
            if ignored_http_statuses and response.status in ignored_http_statuses:
                print(f"{endpoint} returned status {response.status}, ignoring")
            else:
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


def convert_validities(from_time: date, to_time: date) -> Tuple[str, Optional[str]]:
    from_time_str = from_time.isoformat()
    to_time_str = to_time.isoformat()
    return from_time_str, to_time_str if to_time_str != "9999-12-31" else None


def convert_stringio_to_bytesio(output: StringIO, encoding: str = "utf-8") -> BytesIO:
    """Convert StringIO object `output` to a BytesIO object using `encoding`"""
    output.seek(0)
    bytes_output = BytesIO()
    bytes_writer = codecs.getwriter(encoding)(bytes_output)
    bytes_writer.write(output.getvalue())
    bytes_output.seek(0)
    return bytes_output


def write_csv_to_ftp(filename: str, csv_stream: StringIO, folder: str = "DARfejlliste"):
    """Write CSV data in `csv_stream` to FTP file `filename`"""
    ftp = get_ftp_connector()
    ftp.cwd(folder)
    result = ftp.storlines(f"STOR {filename}", convert_stringio_to_bytesio(csv_stream))
    ftp.close()
    return result
