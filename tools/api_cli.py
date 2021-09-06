import json

import click
import requests
from ra_utils.headers import TokenSettings


@click.command()
@click.argument("url", nargs=1)
def cli(url):
    """CLI tool to call MOs api with keycloak token

    Takes an URL as argument, and prints the json returned by MO.
    Uses the envvar CLIENT_SECRET
    Example:
        metacli api_cli http://localhost:5000/service/o/
    """

    headers = TokenSettings().get_headers()

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    click.echo(json.dumps(r.json(), indent=2))


if __name__ == "__main__":
    cli()
