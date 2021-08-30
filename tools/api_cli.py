from pprint import pprint

import click
import requests
from ra_utils.headers import TokenSettings
from ra_utils.load_settings import load_settings


@click.command()
@click.argument("url", nargs=1)
@click.option(
    "--client-secret", envvar="CLIENT_SECRET", help="Keycloak secret for the dipex id"
)
def cli(url, client_secret):
    """CLI tool to call MOs api with keycloak token

    Takes an URL as argument, and prints the json returned by MO.
    Uses the envvar CLIENT_SECRET or fetches from the value
    crontab.client_secret in settings.json

    Example:
        metacli api_cli http://localhost:5000/service/o/
    """

    session = requests.session()

    client_secret = client_secret or load_settings().settings["crontab.client_secret"]
    session.headers = TokenSettings().get_headers()

    r = session.get(url)
    r.raise_for_status()
    pprint(r.json())


if __name__ == "__main__":
    cli()
