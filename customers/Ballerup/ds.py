import click
from gql import gql

from customers.Ballerup.graphql import get_mo_client

QUERY = gql("""
    query QueryOrgUnitsManagersEmployees($cursor: Cursor, $limit: int, $email_type: [UUID!]) {
      org_units(limit: $limit, cursor: $cursor) {
        objects {
          objects {
            uuid
            name
            parent {
              uuid
              name
            }
            managers(inherit: true) {
              employee {
                uuid
                givenname
                surname
                user_key
                addresses(address_types: $email_type) {
                  name
                }
              }
            }
            engagements {
              employee {
                uuid
                givenname
                surname
                user_key
                addresses(address_types: $email_type) {
                  name
                }
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
)


@click.command()
@click.option(
    "--auth-server",
    "auth_server",
    default="http://localhost:8090/auth",
    help="Keycloak auth server URL"
)
@click.option(
    "--client-id",
    "client_id",
    default="dipex",
    help="Keycloak client id"
)
@click.option(
    "--client-secret",
    "client_secret",
    required=True,
    help="Keycloak client secret for the DIPEX client"
)
@click.option(
    "--mo-base-url",
    "mo_base_url",
    default="http://localhost:5000",
    help="Base URL for calling MO"
)
@click.option(
    "--limit",
    "limit",
    type=click.INT,
    default=1,
    help="The GraphQL limit",
)
def main(
    auth_server: str,
    client_id: str,
    client_secret: str,
    mo_base_url: str,
    limit: int,
):
    gql_client = get_mo_client(
        auth_server=auth_server,
        client_id=client_id,
        client_secret=client_secret,
        mo_base_url=mo_base_url,
        gql_version=7,
    )

    next_cursor = None
    while True:
        r = gql_client.execute(
            QUERY,
            variable_values={
                "next_cursor": next_cursor,
                "limit": limit,
                "email_type": "26222138-7f01-4eef-acf1-397de3e5c117"
            },
        )
        print(r)
        break


if __name__ == "__main__":
    main()
