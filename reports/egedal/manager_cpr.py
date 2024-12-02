import csv

from gql import gql
from more_itertools import first
from more_itertools import one
from ra_utils.job_settings import JobSettings
from raclients.graph.client import GraphQLClient
from raclients.upload import file_uploader

from reports.graphql import get_mo_client
from tools.log import get_logger
from tools.log import LogLevel
from tools.log import setup_logging

CPR_QUERY = """
query FindCPR {
  engagements {
    objects {
      current {
        is_primary
        person {
          cpr_number
        }
        org_unit {
          managers(inherit: true) {
            person {
              cpr_number
            }
          }
          parent {
            uuid
          }
        }
      }
    }
  }
}
"""

ORG_UNIT_MANAGER = """
query FindUnitManager($uuid: UUID!) {
  org_units(filter: { uuids: [$uuid] }) {
    objects {
      current {
        parent {
          uuid
        }
        managers(inherit: true) {
          person {
            cpr_number
          }
        }
      }
    }
  }
}

"""
setup_logging(LogLevel.DEBUG)
logger = get_logger()


def upload_to_mo(settings: JobSettings, contents_of_file: str, name_of_file: str):
    """Upload a file to OS2mo reports."""
    logger.info("uploading file to MO reports")
    with file_uploader(settings, name_of_file) as filename:
        with open(filename, "w", encoding="utf-8") as file:
            writer = csv.writer(file)

            writer.writerow(("CPR", "CPR Leder"))  # Header
            writer.writerows(contents_of_file)  # Data


def find_cpr_manager_cpr(client: GraphQLClient):
    """Find all employee cpr_numbers and the cpr_number of their manager

    An employee must not be manager for him/her-self. If an employee is manager in the same organisation unit as the engagement,
    we look for a manager in the unit above until the next manager is found.
    Each employee should only appear once this list with one manager. This is done by showing only primary engagement.
    """

    # TODO: consider pagination
    # Lookup engagements
    res = client.execute(gql(CPR_QUERY))
    engagements = res["engagements"]["objects"]
    # Filter primary engagements
    only_primary = filter(lambda e: e["current"]["is_primary"], engagements)
    result = []

    # For each primary engagement find the manager and the parent unit in case the person is manager for the same unit
    for e in only_primary:
        cpr = one(e["current"]["person"])["cpr_number"]

        org_unit = one(e["current"]["org_unit"])
        parent_unit_uuid = org_unit["parent"]["uuid"] if org_unit["parent"] else None

        managers = org_unit["managers"]
        manager_cprs = set(one(m["person"])["cpr_number"] for m in managers)

        # If this person is manager we need to look at the parent-orgunit - if there is one.
        while cpr in manager_cprs and parent_unit_uuid:
            res = client.execute(
                gql(ORG_UNIT_MANAGER), variable_values={"uuid": parent_unit_uuid}
            )
            current = one(res["org_units"]["objects"])["current"]
            # Overwrite the variables manager_cprs and parent_unit_uuid that are used in the "while" loop
            # If we found a unit where the person is not manager or a top level unit the loop is stopped.
            manager_cprs = set(
                one(m["person"])["cpr_number"] for m in current["managers"]
            )
            parent_unit_uuid = current["parent"]["uuid"] if current["parent"] else None

        # There are usually only one manager on each organisational unit, but in case there are more than one we select one at random
        manager_cpr = first(manager_cprs) if manager_cprs else None

        result.append((cpr, manager_cpr))
    return result


def main():
    """Report for Egedal kommune containing a list of cpr-numbers for employees and cpr-numbers for each employees manager.

    The report is uploaded to MO where Egedal can download it and upload it into XFlow.

    """
    settings = JobSettings()
    client = get_mo_client(
        auth_server=settings.auth_server,
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        mo_base_url=settings.mora_base,
        gql_version=22,
    )
    manager_cpr_map = find_cpr_manager_cpr(client)
    upload_to_mo(settings, manager_cpr_map, "Leder mapping til XFlow.csv")


if __name__ == "__main__":
    logger.info("starting manager_cpr.py")
    main()
    logger.info("done")
