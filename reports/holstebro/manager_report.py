from typing import Any
from uuid import UUID

from gql import gql
from more_itertools import first
from more_itertools import one
from more_itertools import only
from pydantic.main import BaseModel
from structlog import get_logger
import xlsxwriter.worksheet

from raclients.upload import file_uploader
from raclients.graph.client import GraphQLClient
from ra_utils.job_settings import JobSettings, LogLevel

from reports.holstebro.graphql import get_mo_client
from reports.query_actualstate import XLSXExporter


logger = get_logger()

GET_EMPLOYEE_QUERY = gql(
    """
    query GetEmployees($cursor: Cursor, $limit: int, $email_addr_type: [UUID!]) {
      employees(cursor: $cursor, limit: $limit) {
        page_info {
          next_cursor
        }
        objects {
          current {
            given_name
            name
            cpr_number
            addresses(filter: {address_type: {uuids: $email_addr_type}}) {
              name
            }
            manager_roles {
              uuid
              org_unit {
                uuid
              }
            }
            engagements {
              user_key
              org_unit {
                uuid
                name
                user_key
              }
              is_primary
            }
          }
        }
      }
    }
    """
)

GET_ORG_UNITS_QUERY = gql(
    """
    query GetOrgUnits {
      org_units {
        objects {
          current {
            name
            user_key
            uuid
            parent {
              uuid
              user_key
            }
          }
        }
      }
    }
    """
)

GET_EMAIL_ADDR_TYPE_QUERY = gql(
    """
    query GetEmailAddrType {
      classes(filter: {user_keys: "EmailEmployee"}) {
        objects {
          current {
            uuid
          }
        }
      }
    }
    """
)


class XLSXRow(BaseModel):
    employment_id: str
    first_name: str
    last_name: str
    email: str | None
    cpr: str | None
    org_unit_user_key: str | None
    is_manager: bool


def get_email_addr_type(gql_client: GraphQLClient) -> UUID:
    r = gql_client.execute(GET_EMAIL_ADDR_TYPE_QUERY)
    return UUID(one(r["classes"]["objects"])["current"]["uuid"])


def get_employees(
    gql_client: GraphQLClient,
    email_addr_type: UUID,
    limit: int
) -> list[dict[str, Any]]:
    employees = []
    next_cursor = None
    while True:
        r = gql_client.execute(
            GET_EMPLOYEE_QUERY,
            variable_values={
                "cursor": next_cursor,
                "limit": limit,
                "email_addr_type": str(email_addr_type),
            }
        )
        employees.extend(r["employees"]["objects"])
        next_cursor = r["employees"]["page_info"]["next_cursor"]

        logger.info("Number of employees fetched", n=len(employees))

        if next_cursor is None:
            break

    return employees


def get_org_units(gql_client: GraphQLClient) -> list[dict[str, Any]]:
    r = gql_client.execute(GET_ORG_UNITS_QUERY)
    return r["org_units"]["objects"]


def employees_to_xlsx_rows(employees: list[dict[str, Any]]) -> list[XLSXRow]:
    def get_last_name(current: dict[str, Any]) -> str:
        return current["name"].split()[-1]

    def get_org_unit_user_key(engagement: dict[str, Any]) -> str:
        return one(engagement["org_unit"])["user_key"]

    def is_manager(current: dict[str, Any], eng: dict[str, Any]) -> bool:
        manager_roles = current["manager_roles"]

        if not manager_roles:
            return False

        manager_ou_uuids = [
            only(manager_role["org_unit"], dict()).get("uuid")
            for manager_role in manager_roles
        ]
        eng_ou_uuid = one(eng["org_unit"])["uuid"]
        return eng_ou_uuid in manager_ou_uuids

    def get_email(current: dict[str, Any]) -> str:
        """
        Get employee email (or the empty string if no email is found).
        """
        address = first(current["addresses"], None)
        return address["name"] if address is not None else ""

    def get_cpr(current: dict[str, Any]) -> str:
        """
        This function only returns the CPR number, if the employee
        does not have an email address. If an email address exists for
        the employee, the empty string will be returned.
        """
        address = first(current["addresses"], None)
        return current["cpr_number"] if address is None else ""

    return [
        XLSXRow(
            employment_id=eng.get("user_key", ""),
            first_name=emp["current"]["given_name"],
            last_name=get_last_name(emp["current"]),
            email=get_email(emp["current"]),
            cpr=get_cpr(emp["current"]),
            org_unit_user_key=get_org_unit_user_key(eng),
            is_manager=is_manager(emp["current"], eng),
        )
        for emp in employees
        for eng in emp["current"]["engagements"]
    ]


def employee_to_xlsx_exporter_format(xlsx_rows: list[XLSXRow]) -> list[list[str]]:
    data = [
        [
            "Medarbejdernummer",
            "Fornavn",
            "Efternavn",
            "Mail",
            "CPR",
            "Afdelingskode",
            "ErLeder"
        ]
    ]
    for row in xlsx_rows:
        data.append(
            [
                row.employment_id,
                row.first_name,
                row.last_name,
                row.email,
                row.cpr,
                row.org_unit_user_key,
                "Ja" if row.is_manager else "Nej",
            ]
        )
    return data


def org_units_to_xlsx_exporter_format(units: list[dict[str, Any]]) -> list[list[str]]:
    data = [["Afdelingskode", "Afdelingsnavn", "Forældreafdelingskode"]]
    for unit in units:
        parent_obj = unit["current"]["parent"]
        parent = parent_obj["user_key"] if parent_obj is not None else ""
        data.append(
            [
                unit["current"]["user_key"],
                unit["current"]["name"],
                parent,
            ]
        )
    return data


def upload_report(
    settings: JobSettings,
    xlsx_exporter_data: list[list[str]],
    filename: str,
    sheet_name: str,
) -> None:
    # Hack - we need to convert the JobSettings
    settings_dict = {
        "crontab.CLIENT_ID": settings.client_id,
        "crontab.CLIENT_SECRET": settings.crontab_CLIENT_SECRET,
        "crontab.AUTH_SERVER": settings.crontab_AUTH_SERVER,
        "mora.base": settings.mora_base,
    }
    with file_uploader(settings_dict, filename) as report_file:
        workbook = xlsxwriter.Workbook(report_file)
        excel = XLSXExporter(report_file)
        excel.add_sheet(workbook, sheet_name, xlsx_exporter_data)
        workbook.close()


def main(
    settings: JobSettings,
    gql_version: int,
):
    logger.info("Program started")

    gql_client = get_mo_client(
        auth_server=settings.crontab_AUTH_SERVER,
        client_id=settings.client_id,
        client_secret=settings.crontab_CLIENT_SECRET,  # Careful - this is not a SecretStr
        mo_base_url=settings.mora_base,
        gql_version=gql_version,
    )

    # Report for employees and managers
    logger.info("Get employees from MO - this may take a while...")
    email_addr_type = get_email_addr_type(gql_client)
    employees = get_employees(gql_client, email_addr_type, 300)

    logger.info("Convert GraphQL employee data to the exporter format")
    employee_xlsx_rows = employees_to_xlsx_rows(employees)
    employee_xlsx_exporter_data = employee_to_xlsx_exporter_format(employee_xlsx_rows)

    logger.info("Upload employee data to MO")
    upload_report(
        settings,
        employee_xlsx_exporter_data,
        "Holstebro_medarbejdere_ledere.xlsx",
        "Ledere"
    )

    # Report for org units
    logger.info("Get org units from MO")
    org_units = get_org_units(gql_client)

    org_unit_xlsx_exporter_data = org_units_to_xlsx_exporter_format(org_units)

    logger.info("Upload org unit data to MO")
    upload_report(
        settings,
        org_unit_xlsx_exporter_data,
        "Holstebro_org_enheder.xlsx",
        "Enheder",
    )

    logger.info("Program finished")


if __name__ == "__main__":
    settings = JobSettings(log_level=LogLevel.INFO)
    settings.start_logging_based_on_settings()
    main(settings=settings, gql_version=22)
