# Script for getting all changes in SD for a given person based on the persons
# employmentID or CPR number. The script extracts these info from the .tar.gz
# files in /opt/dipex/backup on the servers by parsing the files
# opt/dipex/os2mo-data-import-and-export/mo_integrations.log in the tar
# archives
import re
import sys
import tarfile
from enum import Enum
from enum import unique
from functools import partial
from pathlib import Path
from typing import List
from typing import Optional

import click
from lxml import etree
from lxml.etree import _Element  # as Element
from more_itertools import only
from pydantic import BaseModel

LOG_FILE = "opt/dipex/os2mo-data-import-and-export/mo_integrations.log"
BACKUP_DIR = "/opt/dipex/backup"
OUTPUT_FILE = "/tmp/analysis.txt"


@unique
class IdType(Enum):
    CPR = "PersonCivilRegistrationIdentifier"
    EMPLOYMENT_ID = "EmploymentIdentifier"


class SdPersonChange(BaseModel):
    # We use strings instead of datetimes since we only need the string
    # output anyway
    start_date: str
    end_date: str
    change: Optional[_Element] = None

    class Config:
        arbitrary_types_allowed = True


def get_tar_gz_archive_files(path: Path) -> List[Path]:
    """
    Get all .tar.gz files in the provided folder path.

    Args:
        path: The folder containing the tar.gz files

    Returns:
        List of tar.gz files in the provided folder
    """

    tars = list(filter(lambda f: f.is_file() and f.suffix == ".gz", path.iterdir()))
    tars.sort()
    return tars


def extract_log_file_lines(tar_gz_file: Path) -> List[str]:
    """
    Extract list of lines from the mo_integrations.log file.

    Args:
        tar_gz_file: The tar.gz file to extract the lines from.

    Returns:
        List of log files lines as UTF-8 strings.
    """
    with tarfile.open(str(tar_gz_file), "r:gz") as tar:
        try:
            log_file = tar.extractfile(LOG_FILE)
        except KeyError:
            return []

        if not log_file:
            return []

        # List of byte strings
        lines = log_file.readlines()
        uft8_lines = map(lambda line: line.decode("utf-8"), lines)
        lines_without_newline = map(lambda line: line.rstrip("\n"), uft8_lines)

        return list(lines_without_newline)


def get_sd_xml_responses(log_file_lines: List[str]) -> List[_Element]:
    """
    Get SD XML responses from list of log file lines.

    Args:
        log_file_lines: list of raw lines from log file.

    Returns:
         XML responses from SD
    """

    sd_response_lines = filter(lambda line: "sdCommon Response" in line, log_file_lines)

    # Only use the part of the string that is after
    # <?xml version="1.0" encoding="UTF-8" ?>
    xml_strings = map(lambda s: s.partition("?>")[2], sd_response_lines)

    # Convert for XML strings to XML element
    xml_element = map(lambda s: etree.fromstring(s), xml_strings)

    return list(xml_element)


def get_sd_person_changed_at_date_responses(
    sd_xml_responses: List[_Element],
) -> List[_Element]:
    return list(
        filter(lambda e: e.tag == "GetPersonChangedAtDate20111201", sd_xml_responses)
    )


def get_sd_person_changed(
    identifier_type: IdType, identifier: str, xml_root: _Element
) -> SdPersonChange:

    # Get the request date timestamps
    request_structure = xml_root.find("RequestStructure")
    activation_date = request_structure.find("ActivationDate").text.strip()
    deactivation_date = request_structure.find("DeactivationDate").text.strip()

    def id_match(id_type: IdType, id_: str, person: _Element) -> bool:
        if id_type == IdType.CPR:
            id_element = person.find(id_type.value)
        else:
            employment_element = person.find("Employment")
            id_element = employment_element.find(id_type.value)

        if id_ == id_element.text.strip():
            return True
        return False

    person_elements = xml_root.findall("Person")
    persons = filter(partial(id_match, identifier_type, identifier), person_elements)

    sd_person_changed = SdPersonChange(
        start_date=activation_date, end_date=deactivation_date, change=only(persons)
    )

    return sd_person_changed


def get_all_sd_person_changes(
    identifier_type: IdType, identifier: str, backup_folder: Path
) -> List[SdPersonChange]:
    tar_gz_files = get_tar_gz_archive_files(backup_folder)
    sd_person_changes = []
    for file in tar_gz_files:
        print(f"Analyzing {str(file.name)}...")
        log_file_lines = extract_log_file_lines(file)
        sd_xml_responses = get_sd_xml_responses(log_file_lines)
        changed_at_date_responses = get_sd_person_changed_at_date_responses(
            sd_xml_responses
        )
        sd_person_changes += [
            get_sd_person_changed(identifier_type, identifier, sd_response)
            for sd_response in changed_at_date_responses
        ]
    return sd_person_changes


def output_to_file(all_changes: List[SdPersonChange], file: Path) -> None:
    with open(file, "w") as fp:
        non_empty_changes = filter(
            lambda person: person.change is not None, all_changes
        )
        for sd_person_change in non_empty_changes:
            fp.write(40 * "-" + "\n")
            fp.write(f"Start date: {sd_person_change.start_date}\n")
            fp.write(f"End date: {sd_person_change.end_date}\n\n")
            fp.write(
                etree.tostring(sd_person_change.change, pretty_print=True).decode(
                    "utf-8"
                )
            )


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--folder",
    type=click.Path(exists=True, readable=True),
    default=Path(BACKUP_DIR),
    help="Folder containing the .tar.gz backup files",
)
@click.option("--cpr", type=click.STRING, help="The CPR number of the SD person")
@click.option(
    "--eid", type=click.STRING, help="The SD employment identifier of the person"
)
@click.option(
    "--output",
    type=click.Path(writable=True, file_okay=True),
    default=Path(OUTPUT_FILE),
    help="The output file containing the changes for the given person",
)
def analyze(folder, cpr, eid, output):
    if not ((cpr is None) ^ (eid is None)):
        sys.exit("You must provide exactly one of CPR or employmentID")

    if cpr:
        cpr_regex = re.compile("[0-9]{10}")
        if not cpr_regex.match(cpr):
            sys.exit("CPR must be exactly 10 digits (no hyphen)")
        all_changes = get_all_sd_person_changes(IdType.CPR, cpr, Path(folder))
    else:
        eid_regex = re.compile("[0-9]{5}")
        if not eid_regex.match(eid):
            sys.exit("CPR must be exactly 5 digits (no hyphen)")
        all_changes = get_all_sd_person_changes(IdType.EMPLOYMENT_ID, eid, Path(folder))
    output_to_file(all_changes, Path(output))


if __name__ == "__main__":
    cli()
