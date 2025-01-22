"""Generates two reports for RSD containing information on org_units, managers and engagements"""

import datetime
from collections import defaultdict
from datetime import date
from typing import Iterator

import click
import xlsxwriter
from dateutil.relativedelta import relativedelta
from fastramqpi.ra_utils.job_settings import JobSettings
from fastramqpi.raclients.upload import file_uploader
from more_itertools import first
from more_itertools import one
from more_itertools import prepend

from reports.graphql import get_mo_client
from reports.graphql import paginated_query
from reports.query_actualstate import XLSXExporter
from tools.log import LogLevel
from tools.log import get_logger
from tools.log import setup_logging

setup_logging(LogLevel.DEBUG)
logger = get_logger()


# Query to find engagements and managers in all administrative org_units.
# The administrative organisation means all units below "Region Syddanmark" which is why the uuid  "00923955-db6e-49fc-a191-ec36ff151ec7" can be hardcoded into the filter
QUERY = """
query EngagementManagers($limit: int, $cursor: Cursor = null) {
  org_units(limit: $limit, cursor: $cursor, filter: { ancestor: { uuids: "00923955-db6e-49fc-a191-ec36ff151ec7" } }) {
    objects {
      current {
        uuid
        name
        managers(inherit: false) {
          user_key
          responsibilities {
            name
          }
          person {
            name
            uuid
          }
          manager_type {
            name
          }
        }
        ancestors {
          name
        }
        unit_type {
          name
        }
        engagements {
          person {
            name
            cpr_number
            uuid
            addresses(filter: { address_type: { scope: "EMAIL" } }) {
              name
            }
          }
          user_key
          job_function {
            name
            user_key
          }
          extension_1
          engagement_type {
            name
          }
        }
      }
    }
    page_info {
      next_cursor
    }
  }
}
"""


class RSDReportsCommon:
    def __init__(self, settings, graphql_response, engagement_map):
        self.settings = settings
        self.engagement_map = engagement_map
        # Extract data from graphql response
        self.data = []
        for o in graphql_response:
            self.data.extend(self.parse_orgunit_data_to_report(o))

        self.sort_result()

        self.upload_excel()

    def sort_result(self):
        """Sort data by employee name, unit name and path"""
        employee_row = self.headers.index("Medarbejder")
        self.data.sort(key=lambda _: _[employee_row])
        for i in range(7, -1, -1):
            self.data.sort(key=lambda _: _[i])

    def upload_excel(self):
        """Write data as excel file and upload it to MO"""
        logger.info(f"uploading file {self.filename} to MO reports")
        with file_uploader(self.settings, self.filename) as filename:
            workbook = xlsxwriter.Workbook(filename)
            excel = XLSXExporter(filename)
            excel.add_sheet(
                workbook, "Personer", list(prepend(self.headers, self.data))
            )
            workbook.close()

    def parse_orgunit_data_to_report(self, org_unit: dict) -> Iterator[tuple[str]]:
        raise NotImplementedError()


class RSDReportsEngagementManagers(RSDReportsCommon):
    filename = "engagements_managers.xlsx"
    headers = (
        "Niveau 1",
        "Niveau 2",
        "Niveau 3",
        "Niveau 4",
        "Niveau 5",
        "Niveau 6",
        "Niveau 7",
        "Enhedsnavn",
        "Leder",
        "Medleder 1",
        "Medleder 2",
        "Administrator",
        "Administrativ ansvarlig",
        "Personaleledelse",
        "Uddannelsesansvarlig",
        "BVN (k)",
        "Tjenestenummer",
        "Medarbejder",
        "E-mail",
        "Stillingskode nuværende",
        "Engagementstype",
    )
    # For units with no engagements or managers we need a row containing the first 8 columns and the rest should be empty
    empty_rows = (len(headers) - 8) * [""]

    def parse_orgunit_data_to_report(self, org_unit: dict) -> Iterator[tuple[str]]:
        """Extract relevant data from Graphql-response and return as a list of tuples"""
        ancestors = extract_path(org_unit)
        managers = extract_manager(org_unit["managers"])
        if not (org_unit["engagements"] or org_unit["managers"]):
            yield (*ancestors, org_unit["name"], *self.empty_rows)
        for e in org_unit["engagements"]:
            person = one(e["person"])
            name = person["name"]

            email = first(person["addresses"], default=None)
            email = email["name"] if email else ""
            job_function = (
                f"{e['job_function']['name']} ({e['job_function']['user_key']})"
            )
            yield (
                *ancestors,
                org_unit["name"],
                *managers,
                e["user_key"],
                e["user_key"][3:],
                name,
                email,
                job_function,
                e["engagement_type"]["name"],
            )

        for m in find_managers_with_no_engagement_here(org_unit):
            engagement = first(self.engagement_map[m], default=None)
            if not engagement:
                continue
            person = one(engagement["person"])
            email = first(person["addresses"], default=None)
            email = email["name"] if email else ""
            job_function = f"{engagement['job_function']['name']} ({engagement['job_function']['user_key']})"
            yield (
                *ancestors,
                org_unit["name"],
                *managers,
                engagement["user_key"],
                engagement["user_key"][3:],
                one(engagement["person"])["name"],
                email,
                job_function,
                engagement["engagement_type"]["name"],
            )


class RSDReportsEngagementManagersWithCPR(RSDReportsCommon):
    filename = "engagements_managers_with_cpr.xlsx"
    headers = [
        "Niveau 1",
        "Niveau 2",
        "Niveau 3",
        "Niveau 4",
        "Niveau 5",
        "Niveau 6",
        "Niveau 7",
        "Enhedsnavn",
        "Enhedstype",
        "Medarbejder",
        "Stilling",
        "Lederbetegnelse",
        "Lederansvar",
        "Tjenestenummer",
        "CPR-nummer",
        "E-mail",
        "Alder",
        "Leder",
        "BVN (k)",
        "Engagementstype",
    ]
    # For units with no engagements or managers we need a row containing the first 9 columns and the rest should be empty
    empty_rows = (len(headers) - 9) * [""]

    def parse_orgunit_data_to_report(self, org_unit: dict) -> Iterator[tuple[str]]:
        """For 2. report Extract relevant data from Graphql-response and return as a list of tuples"""
        ancestors = extract_path(org_unit)
        managers = org_unit["managers"]
        manager = first(find_managers_of_type(managers, "Leder"), default=None)
        manager_name = (
            one(manager["person"])["name"] if manager and manager["person"] else ""
        )
        if not (org_unit["engagements"] or org_unit["managers"]):
            yield (
                *ancestors,
                org_unit["name"],
                org_unit["unit_type"]["name"],
                *self.empty_rows,
            )
        for e in org_unit["engagements"]:
            person = one(e["person"])

            email = first(person["addresses"], default=None)
            email = email["name"] if email else ""

            manager_role = find_manager_role_for_person(person, managers)
            manager_type = manager_role["manager_type"]["name"] if manager_role else ""
            # Select "Personaleledelse" if it exists, else pick any other responsibility
            responsibility = (
                max(
                    manager_role["responsibilities"],
                    key=lambda _: _["name"] == "Personaleledelse",
                )["name"]
                if manager_role
                else ""
            )

            yield (
                *ancestors,
                org_unit["name"],
                org_unit["unit_type"]["name"],
                person["name"],
                e["extension_1"],
                manager_type,
                responsibility,
                e["user_key"][3:],
                person["cpr_number"],
                email,
                str(get_age(person["cpr_number"])),
                manager_name,
                e["user_key"],
                e["engagement_type"]["name"],
            )

        for m in find_managers_with_no_engagement_here(org_unit):
            engagement = first(self.engagement_map[m], default=None)
            if not engagement:
                continue
            person = one(engagement["person"])
            manager_role = find_manager_role_for_person(person, managers)
            manager_type = manager_role["manager_type"]["name"] if manager_role else ""
            # Select "Personaleledelse" if it exists, else pick any other responsibility
            responsibility = (
                max(
                    manager_role["responsibilities"],
                    key=lambda _: _["name"] == "Personaleledelse",
                )["name"]
                if manager_role
                else ""
            )
            email = first(person["addresses"], default=None)
            email = email["name"] if email else ""

            yield (
                *ancestors,
                org_unit["name"],
                org_unit["unit_type"]["name"],
                person["name"],
                engagement["extension_1"],
                manager_type,
                responsibility,
                engagement["user_key"][3:],
                person["cpr_number"],
                email,
                str(get_age(person["cpr_number"])),
                manager_name,
                engagement["user_key"],
                engagement["engagement_type"]["name"],
            )


def find_managers_with_no_engagement_here(org_unit: dict) -> set[str]:
    engagement_persons = {one(e["person"])["uuid"] for e in org_unit["engagements"]}
    manager_persons = {
        one(m["person"])["uuid"] for m in org_unit["managers"] if m["person"]
    }
    return manager_persons - engagement_persons


def extract_path(org_unit: dict) -> list[str]:
    """Find the path to the org_unit.

    Returns a list of names of every ancestor unit, including the org_unit itself.
    Always returns 7 values to conform with the report scheme. For paths with less than 7 ancestors
    empty strings are returned. Any path longer than 7 ancestors is cropped.

    """
    ancestors = org_unit["ancestors"]

    # Reverse ancestors list to start at root
    ancestor_names = [a["name"] for a in ancestors[::-1]]

    ancestor_names.append(org_unit["name"])
    # Max level is 7 ancestors as pr. request from RSD
    ancestor_names = ancestor_names[:7]
    # Append empty strings to conform to report schema of up to 7 ancestors
    for _ in range(7 - len(ancestor_names)):
        ancestor_names.append("")
    return ancestor_names


def find_managers_of_type(managers: list[dict], manager_type: str) -> list[dict]:
    return [m for m in managers if m["manager_type"]["name"] == manager_type]


def has_responsibility(manager: dict, responsibility_name: str) -> bool:
    return any(r["name"] == responsibility_name for r in manager["responsibilities"])


def find_manager_role_for_person(person: dict, managers: dict) -> dict | None:
    return first(
        (
            e
            for e in managers
            if e["person"] and one(e["person"])["uuid"] == person["uuid"]
        ),
        default=None,
    )


def extract_manager(managers: list[dict]) -> list[str]:
    """extract names of managers based on responsibility and return for the following:
    * "Leder"
    * "Medleder 1"
    * "Medleder 2"
    * "Administrator"
    * "Administrativ ansvarlig"
    * "Personaleledelse"
    * "Uddannelsesansvarlig"
    """

    manager = first(find_managers_of_type(managers, "Leder"), default=None)
    co_managers = find_managers_of_type(managers, "Medleder")
    co_manager_1 = co_managers[0] if len(co_managers) > 0 else None
    co_manager_2 = co_managers[1] if len(co_managers) > 1 else None
    administrator = first(
        find_managers_of_type(managers, "Administrator"), default=None
    )
    admin_responsibility = first(
        (m for m in managers if has_responsibility(m, "Administrativ ansvarlig")),
        default=None,
    )
    personel_management = first(
        (m for m in managers if has_responsibility(m, "Personaleledelse")), default=None
    )
    udd = first(
        (m for m in managers if has_responsibility(m, "Uddannelsesansvarlig")),
        default=None,
    )

    def extract_manager_name(manager: dict | None) -> str:
        if manager is None:
            return ""
        try:
            return one(manager["person"])["name"]
        except TypeError:
            return ""

    return [
        extract_manager_name(manager),
        extract_manager_name(co_manager_1),
        extract_manager_name(co_manager_2),
        extract_manager_name(administrator),
        extract_manager_name(admin_responsibility),
        extract_manager_name(personel_management),
        extract_manager_name(udd),
    ]


def get_cpr_birthdate(number: int | str) -> datetime.datetime:
    """Copied from MO"""
    if isinstance(number, str):
        number = int(number)

    rest, code = divmod(number, 10000)
    rest, year = divmod(rest, 100)
    rest, month = divmod(rest, 100)
    rest, day = divmod(rest, 100)

    if rest:
        raise ValueError(f"invalid CPR number {number}")

    # see https://da.wikipedia.org/wiki/CPR-nummer :(
    if code < 4000:
        century = 1900
    elif code < 5000:
        century = 2000 if year <= 36 else 1900
    elif code < 9000:
        century = 2000 if year <= 57 else 1800
    else:
        century = 2000 if year <= 36 else 1900

    try:
        return datetime.datetime(century + year, month, day)
    except ValueError:
        raise ValueError(f"invalid CPR number {number}")


def get_age(cpr_number: str) -> int:
    birthdate = get_cpr_birthdate(cpr_number)

    # From "Approach #5"  https://www.geeksforgeeks.org/python-program-to-calculate-age-in-year/
    today = date.today()
    age = relativedelta(today, birthdate)
    return age.years


@click.command()
@click.option("--mora_base", envvar="MORA_BASE", default="http://mo-service:5000")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option("--client-secret", envvar="CLIENT_SECRET")
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option(
    "--auth-server", envvar="AUTH_SERVER", default="http://keycloak-service:8080/auth"
)
def main(*args, **kwargs):
    settings = JobSettings(**kwargs)
    client = get_mo_client(
        auth_server=settings.auth_server,
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        mo_base_url=settings.mora_base,
        gql_version=22,
    )
    res = paginated_query(graphql_client=client, query=QUERY, page_size=10)
    res = [r["current"] for r in res]

    # Create a map of all org_unit_uuids to the set of engagement and managers user_keys in each
    engagement_map = defaultdict(list)

    for o in res:
        for e in o["engagements"]:
            engagement_map[one(e["person"])["uuid"]].append(e)

    RSDReportsEngagementManagers(
        settings=settings, graphql_response=res, engagement_map=engagement_map
    )
    RSDReportsEngagementManagersWithCPR(
        settings=settings, graphql_response=res, engagement_map=engagement_map
    )


if __name__ == "__main__":
    logger.info("starting engagement_report")
    main()
    logger.info("done")
