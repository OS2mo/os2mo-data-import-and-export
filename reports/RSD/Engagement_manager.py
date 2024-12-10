import click
import xlsxwriter
from gql import gql
from more_itertools import first
from more_itertools import one
from more_itertools import only
from more_itertools import prepend
from ra_utils.job_settings import JobSettings
from raclients.upload import file_uploader

from reports.graphql import get_mo_client
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
query = """query EngagementManagers {
  engagements(limit: "100") {
    objects {
      current {
        person {
          name
          addresses(filter: { address_type: { scope: "EMAIL" } }) {
            name
          }
        }
        user_key
        job_function{
          name
          user_key
        }
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
        }
      }
    }
  }
}
"""


def extract_ancestors(
    ancestors: dict,
) -> list[str]:
    # Reverse ancestors list to start at root
    ancestor_names = [a["name"] for a in ancestors[::-1]]
    # Max level is 7 ancestors.
    # Todo: consider a dynamic number of columns
    if len(ancestor_names) > 7:
        ancestor_names = ancestor_names[:7]
    # Append empty strings to conform to report schema of up to 7 ancestors
    for _ in range(7 - len(ancestor_names)):
        ancestor_names.append("")
    return ancestor_names


def extract_manager(managers: dict) -> list:
    """extract managers based on responsibility and return for the following:
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

    manager = only(m for m in managers if has_responsibility(m, "Leder"))
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

    def extract_manager_name(manager: dict) -> str:
        if not manager:
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


def extract_list_format(res: dict):
    """Extract relevant data from Graphql-response and return as a list of tuples"""
    engagements = res["engagements"]["objects"]
    for e in engagements:
        current = e["current"]

        name = one(current["person"])["name"]
        org_unit = one(current["org_unit"])

        email = first(one(current["person"])["addresses"], default=None)
        email = email["name"] if email else ""
        job_function = (
            f'{current["job_function"]["name"]} ({current["job_function"]["user_key"]})'
        )
        yield [
            *extract_ancestors(org_unit["ancestors"]),
            org_unit["name"],
            *extract_manager(org_unit["managers"]),
            current["user_key"],
            current["user_key"][3:],
            name,
            email,
            job_function,
        ]


@click.command()
@click.option("--mora_base", envvar="MORA_BASE", default="http://localhost:5000")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option("--client-secret", envvar="CLIENT_SECRET")
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option(
    "--auth-server", envvar="AUTH_SERVER", default="http://localhost:5000/auth"
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
    # TODO: add pagination
    res = client.execute(gql(query))
    # TODO Transform res to data :ez:
    data = list(extract_list_format(res))

    # Sort data by unit path
    for i in range(7)[::-1]:
        data.sort(key=lambda l: l[i])

    logger.info("uploading file to MO reports")
    with file_uploader(settings, "managers.xlsx") as filename:
        # write data as excel file
        workbook = xlsxwriter.Workbook(filename)
        excel = XLSXExporter(filename)
        excel.add_sheet(workbook, "page", list(prepend(HEADERS, data)))
        workbook.close()


if __name__ == "__main__":
    logger.info("starting engagement_report")
    main()
    logger.info("done")
