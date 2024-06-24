from typing import Any
from uuid import UUID

from gql import gql
from more_itertools import one

from raclients.graph.client import GraphQLClient
from ra_utils.job_settings import JobSettings

from reports.holstebro.graphql import get_mo_client


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


def get_email_addr_type(gql_client: GraphQLClient) -> UUID:
    get_email_addr_type_query = gql(
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
    r = gql_client.execute(get_email_addr_type_query)
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
        if next_cursor is None:
            break

    return employees


def main(
    auth_server: str,
    client_id: str,
    client_secret: str,
    mo_base_url: str,
    gql_version: int,
):
    gql_client = get_mo_client(
        auth_server=auth_server,
        client_id=client_id,
        client_secret=client_secret,  # Careful - this is not a SecretStr
        mo_base_url=mo_base_url,
        gql_version=gql_version,
    )

    email_addr_type = get_email_addr_type(gql_client)
    employees = get_employees(gql_client, email_addr_type, 100)



if __name__ == "__main__":
    settings = JobSettings()
    main(
        auth_server=settings.auth_server,
        client_id=settings.client_id,
        client_secret=settings.client_secret,  # Careful - this is not a SecretStr
        mo_base_url=settings.mora_base,
        gql_version=22,
    )
