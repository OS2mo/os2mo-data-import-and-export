import json
from typing import Any
from uuid import UUID

import click
from gql import gql
from more_itertools import one, partition
from raclients.graph.client import GraphQLClient


def get_org_units(
    session, ou_uuid: UUID, org_units: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    query = gql(
        """
        query GetOrgUnit ($uuids: [UUID!]) {
          org_units(uuids: $uuids) {
            objects {
              name
              org_unit_level {
                uuid
                user_key
                name
              }
              unit_type {
                uuid
                user_key
                name
              }
              children {
                uuid
                name
                user_key
              }
              validity {
                from
                to
              }
              parent {
                name
                uuid
                user_key
              }
              uuid
              user_key
            }
          }
        }
        """
    )

    r = session.execute(query, variable_values={"uuids": str(ou_uuid)})

    ou = one(one(r["org_units"])["objects"])
    org_units.append(ou)
    children = ou["children"]
    if len(children) > 0:

        # Handle Silkeborg Kommune
        silkeborg_kommune_iter, others = partition(
            lambda ou: ou["name"] != "Silkeborg Kommune", children
        )
        silkeborg_kommune = list(silkeborg_kommune_iter)
        if len(silkeborg_kommune) > 0:
            unit = one(silkeborg_kommune)
            resp = session.execute(query, variable_values={"uuids": unit["uuid"]})
            sik_ou = one(one(resp["org_units"])["objects"])
            org_units.append(sik_ou)

        # Handle all others
        for child in others:
            get_org_units(session, child["uuid"], org_units)

    return org_units


def create_org_unit(session, org_unit: dict[str, Any]) -> None:
    mutation = gql(
        """
        mutation CreateOrgUnit ($input: OrganisationUnitCreateInput!) {
          org_unit_create(input: $input) {
            uuid
          }
        }
        """
    )

    create_org_unit_vars = {
        "input": {
            "uuid": org_unit["uuid"],
            "user_key": org_unit["user_key"],
            "name": org_unit["name"],
            "org_unit_level": org_unit["org_unit_level"]["uuid"],
            "org_unit_type": org_unit["unit_type"]["uuid"],
            "validity": {"from": "1930-01-01"},
        }
    }
    if org_unit["parent"] is not None:
        create_org_unit_vars["input"]["parent"] = org_unit["parent"]["uuid"]

    session.execute(mutation, variable_values=create_org_unit_vars)


@click.group()
@click.option("--base-url", type=click.STRING, default="http://mo-service:5000")
@click.option(
    "--auth-server", type=click.STRING, default="http://keycloak-service:8080/auth"
)
@click.option("--auth-realm", type=click.STRING, default="mo")
@click.option("--client-id", type=click.STRING, default="dipex")
@click.option("--client-secret", type=click.STRING, required=True)
@click.pass_context
def cli(
    ctx,
    base_url: str,
    auth_server: str,
    auth_realm: str,
    client_id: str,
    client_secret: str,
):
    ctx.ensure_object(dict)

    gql_client = GraphQLClient(
        sync=True,
        url=base_url + "/graphql/v3",
        client_id=client_id,
        client_secret=client_secret,
        auth_server=auth_server,
        auth_realm=auth_realm,
    )

    ctx.obj["gql_client"] = gql_client


@cli.command()
@click.option("--root-uuid", type=click.UUID, required=True, help="UUID of the root OU")
@click.pass_context
def get_ou_tree(ctx, root_uuid: UUID):
    gql_client = ctx.obj["gql_client"]

    with gql_client as session:
        org_units = get_org_units(session, root_uuid, [])

    with open("/tmp/org_units.json", "w") as fp:
        json.dump(org_units, fp, indent=2)


@cli.command()
@click.pass_context
def create_org_units(ctx):
    gql_client = ctx.obj["gql_client"]

    with open("/tmp/org_units.json", "r") as fp:
        org_units = json.load(fp)

    with gql_client as session:
        for ou in org_units:
            create_org_unit(session, ou)


if __name__ == "__main__":
    cli(obj=dict())
