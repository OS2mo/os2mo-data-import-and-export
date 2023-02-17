from datetime import datetime
from functools import lru_cache
from uuid import UUID

import click
import httpx
from ra_utils.headers import TokenSettings


@lru_cache()
def today():
    return datetime.now().strftime("%Y-%m-%d")


def create_relation(
    client: httpx.Client, base_url: str, from_uuid: UUID, to_uuid: UUID
):
    """Create a relation between given org_units"""
    payload = {"destination": [str(to_uuid)], "validity": {"from": today()}}
    r = client.post(f"{base_url}/service/ou/{str(from_uuid)}/map", json=payload)
    return r.json()


@click.command()
@click.argument("input-file", type=click.File())
@click.option("--base_url", default="http://localhost:5000")
def cli(base_url, input_file):
    """Tool to import related organisations from a csv file.

    The input-file is assumed to be a space-seperated .csv file with two columns of uuids.

    Example contents of input file:

    7ddf4346-ce24-6ba5-7620-a1e7162fda68 96d2125e-7f5d-454a-a564-ce8ccb0b2d95
    f7bc29bf-0afb-f164-1649-80b002ecc047 a116fd1d-7d75-4855-9c9b-1c0707942622
    """
    client = httpx.Client()
    client.headers = TokenSettings().get_headers()
    while relation := input_file.readline():
        from_uuid, to_uuid = tuple(map(UUID, relation.split()))
        print(create_relation(client, base_url, from_uuid, to_uuid))


if __name__ == "__main__":
    cli()
