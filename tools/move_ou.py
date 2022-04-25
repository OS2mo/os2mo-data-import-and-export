from datetime import datetime
from uuid import UUID

import click
import requests


@click.command()
@click.option(
    "--uuid",
    type=click.UUID,
    required=True
)
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
def move(
    uuid: UUID,
    parent_uuid: UUID,
    base_url: str,
    auth_server: str,
    auth_realm: str,
    client_id: str,
    client_secret: str
):
    """
    Move org unit to new parent
    """

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
    print(r.status_code, r.url)


if __name__ == "__main__":
    move()
