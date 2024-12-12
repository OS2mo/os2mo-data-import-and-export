"""Generates a report for RSD containing information on org_units, managers and engagements"""
import datetime
from datetime import date

import click
import xlsxwriter
from dateutil.relativedelta import relativedelta
from more_itertools import first
from more_itertools import one
from more_itertools import prepend
from ra_utils.job_settings import JobSettings
from raclients.upload import file_uploader

from reports.graphql import get_mo_client
from reports.graphql import paginated_query
from reports.query_actualstate import XLSXExporter
from tools.log import get_logger
from tools.log import LogLevel
from tools.log import setup_logging

setup_logging(LogLevel.DEBUG)
logger = get_logger()

HEADERS = (
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
)
HEADERS_2 = [
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
]


QUERY = """
query EngagementManagers($limit: int, $cursor: Cursor = null) {
  engagements(limit: $limit, cursor: $cursor) {
    objects {
      current {
        person {
          name
          cpr_number
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
        org_unit {
          name
          managers(inherit: false) {
            responsibilities {
              name
            }
            person {
              name
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
        }
      }
    }
    page_info {
      next_cursor
    }
  }
}

"""


def extract_ancestors(
    ancestors: list[dict],
) -> list[str]:
    # Reverse ancestors list to start at root
    ancestor_names = [a["name"] for a in ancestors[::-1]]
    # Max level is 7 ancestors.
    # Todo: consider a dynamic number of columns
    ancestor_names = ancestor_names[:7]
    # Append empty strings to conform to report schema of up to 7 ancestors
    for _ in range(7 - len(ancestor_names)):
        ancestor_names.append("")
    return ancestor_names


def extract_manager(managers: dict) -> list[str]:
    """extract names of managers based on responsibility and return for the following:
    * "Leder"
    * "Medleder 1"
    * "Medleder 2"
    * "Administrator"
    * "Administrativ ansvarlig"
    * "Personaleledelse"
    * "Uddannelsesansvarlig"
    """

    def has_responsibility(manager, responsibility_name):
        return any(
            r["name"] == responsibility_name for r in manager["responsibilities"]
        )

    manager = first((m for m in managers if has_responsibility(m, "Leder")), default="")
    co_managers = [m for m in managers if has_responsibility(m, "Medleder")]
    co_manager_1 = co_managers[0] if len(co_managers) > 0 else ""
    co_manager_2 = co_managers[1] if len(co_managers) > 1 else ""
    administrator = first(
        (m for m in managers if has_responsibility(m, "Administrator")), default=""
    )
    admin_responsibility = first(
        (m for m in managers if has_responsibility(m, "Administrativ ansvarlig")),
        default="",
    )
    personel_management = first(
        (m for m in managers if has_responsibility(m, "Personaleledelse")), default=""
    )
    udd = first(
        (m for m in managers if has_responsibility(m, "Uddannelsesansvarlig")),
        default="",
    )

    def extract_manager_name(manager: dict | str) -> str:
        if not manager:
            return ""
        try:
            return one(manager["person"])["name"]  # type: ignore
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


def extract_list_format_1(engagement: dict) -> list[str]:
    """Extract relevant data from Graphql-response and return as a list of tuples"""

    name = one(engagement["person"])["name"]
    org_unit = one(engagement["org_unit"])

    email = first(one(engagement["person"])["addresses"], default=None)
    email = email["name"] if email else ""
    job_function = f'{engagement["job_function"]["name"]} ({engagement["job_function"]["user_key"]})'
    row = [
        *extract_ancestors(org_unit["ancestors"]),
        org_unit["name"],
        *extract_manager(org_unit["managers"]),
        engagement["user_key"],
        engagement["user_key"][3:],
        name,
        email,
        job_function,
    ]
    # Add org_unit name as first ancestor if no ancestors
    if not row[0]:
        row[0] = org_unit["name"]
    return row


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


def extract_list_format_2(engagement: dict) -> list[str]:
    """For 2. report Extract relevant data from Graphql-response and return as a list of tuples"""

    person = one(engagement["person"])
    org_unit = one(engagement["org_unit"])

    email = first(one(engagement["person"])["addresses"], default=None)
    email = email["name"] if email else ""
    # TODO: Add org_unit name as first ancestor if no ancestors

    row = [
        *extract_ancestors(org_unit["ancestors"]),
        org_unit["name"],
        org_unit["unit_type"]["name"],
        person["name"],
        engagement["extension_1"],
        "Lederbetegnelse",
        "Lederansvar",
        engagement["user_key"][3:],
        person["cpr_number"],
        email,
        str(get_age(person["cpr_number"])),
        "Leder",
        engagement["user_key"],
    ]
    # Add org_unit name as first ancestor if no ancestors
    if not row[0]:
        row[0] = org_unit["name"]
    return row


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
    res = list(paginated_query(graphql_client=client, query=QUERY))
    # Extract data from graphql response
    data = [extract_list_format_1(e["current"]) for e in res]
    data_2 = [extract_list_format_2(e["current"]) for e in res]

    # Sort data by unit name and path
    for i in range(7, -1, -1):
        data.sort(key=lambda l: l[i])
        data_2.sort(key=lambda l: l[i])

    logger.info("uploading files to MO reports")
    with file_uploader(settings, "managers.xlsx") as filename:
        # write data as excel file
        workbook = xlsxwriter.Workbook(filename)
        excel = XLSXExporter(filename)
        excel.add_sheet(workbook, "ledere", list(prepend(HEADERS, data)))
        workbook.close()
    with file_uploader(settings, "engagements.xlsx") as filename:
        # write data as excel file
        workbook = xlsxwriter.Workbook(filename)
        excel = XLSXExporter(filename)
        excel.add_sheet(workbook, "engagementer", list(prepend(HEADERS_2, data_2)))
        workbook.close()


if __name__ == "__main__":
    logger.info("starting engagement_report")
    main()
    logger.info("done")
