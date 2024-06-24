from typing import Any
from uuid import UUID

from gql import gql
from more_itertools import one

from raclients.graph.client import GraphQLClient
from ra_utils.job_settings import JobSettings

from reports.holstebro.graphql import get_mo_client


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

    addr_type_uuid = get_email_addr_type(gql_client)


if __name__ == "__main__":
    settings = JobSettings()
    main(
        auth_server=settings.auth_server,
        client_id=settings.client_id,
        client_secret=settings.client_secret,  # Careful - this is not a SecretStr
        mo_base_url=settings.mora_base,
        gql_version=22,
    )
