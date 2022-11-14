from datetime import datetime
from uuid import UUID

import click
from gql import gql
import requests

from raclients.graph.client import GraphQLClient


def _move(
    uuid: UUID,
    parent_uuid: UUID,
    base_url: str,
    auth_server: str,
    auth_realm: str,
    client_id: str,
    client_secret: str
) -> None:
    assert uuid != parent_uuid

    # For the current use case it is inconvenient to use RAClients and
    # RAModels due to some mandatory model fields, so we will use plain
    # old requests instead

    token_url = auth_server + f"/realms/{auth_realm}/protocol/openid-connect/token"
    token_payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    r = requests.post(token_url, data=token_payload)
    token = r.json()["access_token"]
    headers = {"Authorization": f"bearer {token}"}

    # Move unit

    payload = {
        "type": "org_unit",
        "data": {
            "parent": {"uuid": str(parent_uuid)},
            "uuid": str(uuid),
            "clamp": True,
            "validity": {"from": datetime.now().strftime("%Y-%m-%d")}
        },
    }

    r = requests.post(
        f"{base_url}/service/details/edit",
        headers=headers,
        json=payload,
        params={"force": True}
    )
    click.echo(f"{r.status_code} {r.url}")


@click.group()
@click.option(
    "--parent-uuid",
    type=click.UUID,
    required=click.UUID
)
@click.option(
    "--base-url",
    type=click.STRING,
    default="http://mo-service:5000"
)
@click.option(
    "--auth-server",
    type=click.STRING,
    default="http://keycloak-service:8080/auth"
)
@click.option(
    "--auth-realm",
    type=click.STRING,
    default="mo"
)
@click.option(
    "--client-id",
    type=click.STRING,
    default="dipex"
)
@click.option(
    "--client-secret",
    type=click.STRING,
    required=True
)
@click.pass_context
def cli(
    ctx,
    parent_uuid: UUID,
    base_url: str,
    auth_server: str,
    auth_realm: str,
    client_id: str,
    client_secret: str,
):
    ctx.ensure_object(dict)
    ctx.obj["PARENT_UUID"] = parent_uuid
    ctx.obj["BASE_URL"] = base_url
    ctx.obj["AUTH_SERVER"] = auth_server
    ctx.obj["AUTH_REALM"] = auth_realm
    ctx.obj["CLIENT_ID"] = client_id
    ctx.obj["CLIENT_SECRET"] = client_secret


@cli.command()
@click.option(
    "--uuid",
    type=click.UUID,
    required=True
)
@click.pass_context
def move(
    ctx,
    uuid: UUID,
):
    """
    Move org unit to new parent
    """
    _move(
        uuid,
        ctx.obj["PARENT_UUID"],
        ctx.obj['BASE_URL'],
        ctx.obj["AUTH_SERVER"],
        ctx.obj['AUTH_REALM'],
        ctx.obj["CLIENT_ID"],
        ctx.obj["CLIENT_SECRET"],
    )


@cli.command()
@click.pass_context
def move_all_to_new_parent(ctx):
    """
    Move all "old" root org unit to "new" parent root org unit.
    The new parent root org unit must be created manually before
    running this script.
    """

    client = GraphQLClient(
        sync=True,
        url=ctx.obj["BASE_URL"] + "/graphql/v3",
        client_id=ctx.obj["CLIENT_ID"],
        client_secret=ctx.obj["CLIENT_SECRET"],
        auth_server=ctx.obj["AUTH_SERVER"],
        auth_realm=ctx.obj["AUTH_REALM"]
    )

    with client as session:
        query = gql(
            """
            query OrgUnitQuery {
                org_units(parents: null) {
                    uuid
                }
            }
            """
        )
        r = session.execute(query)

    org_units_uuids_including_new_parent = map(lambda ou: UUID(ou["uuid"]), r["org_units"])
    org_units_uuid = filter(
        lambda _uuid: _uuid != ctx.obj["PARENT_UUID"],
        org_units_uuids_including_new_parent
    )
    for _uuid in org_units_uuid:
        click.echo(f"Moving org unit: {str(_uuid)}")
        _move(
            _uuid,
            ctx.obj["PARENT_UUID"],
            ctx.obj["BASE_URL"],
            ctx.obj["AUTH_SERVER"],
            ctx.obj["AUTH_REALM"],
            ctx.obj["CLIENT_ID"],
            ctx.obj["CLIENT_SECRET"],
        )


if __name__ == "__main__":
    cli(obj=dict())
