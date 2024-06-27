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
            user_key
            given_name
            name
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


def employees_to_xlsx_rows(employees: list[dict[str, Any]]) -> list[XLSXRow]:
    def get_last_name(current: dict[str, Any]) -> str:
        return current["name"][len(current["given_name"]) + 1:]

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

    return [
        XLSXRow(
            employment_id=emp["current"]["user_key"],
            first_name=emp["current"]["given_name"],
            last_name=get_last_name(emp["current"]),
            email=first(emp["current"]["addresses"], dict()).get("name"),
            org_unit_user_key=get_org_unit_user_key(eng),
            is_manager=is_manager(emp["current"], eng),
        )
        for emp in employees
        for eng in emp["current"]["engagements"]
    ]


def to_xlsx_exporter_format(xlsx_rows: list[XLSXRow]) -> list[list[str]]:
    data = [
        [
            "Medarbejdernummer",
            "Fornavn",
            "Efternavn",
            "Mail",
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
                row.email if row.email is not None else "",
                row.org_unit_user_key,
                "Ja" if row.is_manager else "Nej",
            ]
        )
    return data


def upload_report(
    settings: JobSettings,
    xlsx_exporter_data: list[list[str]]
) -> None:
    # Hack - we need to convert the JobSettings
    settings_dict = {
        "crontab.CLIENT_ID": settings.client_id,
        "crontab.CLIENT_SECRET": settings.crontab_CLIENT_SECRET,
        "crontab.AUTH_SERVER": settings.crontab_AUTH_SERVER,
        "mora.base": settings.mora_base,
    }
    with file_uploader(settings_dict, "Medarbejdere.xlsx") as report_file:
        workbook = xlsxwriter.Workbook(report_file)
        excel = XLSXExporter(report_file)
        excel.add_sheet(workbook, "Medarbejdere", xlsx_exporter_data)
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

    logger.info("Get employees from MO - this may take a while...")
    email_addr_type = get_email_addr_type(gql_client)
    employees = get_employees(gql_client, email_addr_type, 300)

    logger.info("Convert GraphQL data to the exporter format")
    xlsx_rows = employees_to_xlsx_rows(employees)
    xlsx_exporter_data = to_xlsx_exporter_format(xlsx_rows)

    logger.info("Upload data to MO")
    upload_report(settings, xlsx_exporter_data)

    logger.info("Program finished")


if __name__ == "__main__":
    settings = JobSettings(log_level=LogLevel.INFO)
    settings.start_logging_based_on_settings()
    main(settings=settings, gql_version=22)
