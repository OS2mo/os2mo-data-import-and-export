import click
import httpx
from more_itertools import one
from ra_utils.async_to_sync import async_to_sync
from ra_utils.headers import TokenSettings


class Dummy:
    """Dummy response for dry-runs"""

    status_code = 0

    def raise_for_status(self):
        pass


def dry_run_post(self, *args, **kwargs):
    """A dry run method to overload httpx.post"""
    click.echo(f"dry-run. Would post with {args=} and {kwargs=}")
    return Dummy()


@click.command()
@click.argument("givenname", required=True)
@click.argument("surname", required=True)
@click.option("--uuid", required=False, type=click.UUID)
@click.option("--cpr", required=False)
@click.option("--user_key", required=False)
@click.option("--mora-base", envvar="BASE_URL", default="http://localhost:5000")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option(
    "--client-secret",
    envvar="CLIENT_SECRET",
)
@click.option("--auth-server", envvar="AUTH_SERVER")
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option("--dry-run", is_flag=True)
@async_to_sync
async def cli(
    givenname,
    surname,
    uuid,
    cpr,
    user_key,
    mora_base,
    client_id,
    client_secret,
    auth_server,
    auth_realm,
    dry_run,
) -> None:
    """Script to create new user if a user with the given uuid doesn't exist"""
    # Override httpx.post if dry_run
    if dry_run:
        httpx.post = dry_run_post
    headers = TokenSettings().get_headers()

    # Get root organisation
    r = httpx.get(f"{mora_base}/service/o/", headers=headers)
    r.raise_for_status()
    root_uuid = one(r.json())["uuid"]

    # Create the payload for the new user
    payload = {
        "uuid": str(uuid),
        "givenname": givenname,
        "surname": surname,
        "cpr_no": cpr,
        "user_key": user_key,
        "org": {"uuid": root_uuid},
    }
    # If a uuid is given, check if a user exists
    user = (
        httpx.get(f"{mora_base}/service/e/{str(uuid)}/", headers=headers)
        if uuid
        else None
    )
    if user is None or user.status_code == 404:
        # Create the user
        r = httpx.post(f"{mora_base}/service/e/create", json=payload, headers=headers)

    if r.status_code == 201:
        click.echo(f"User created with uuid={r.json()}")
    elif r.status_code == 200:
        click.echo("User already exists")
    elif r.status_code == 0:
        # This is for dry-runs
        pass
    else:
        r.raise_for_status()


if __name__ == "__main__":
    cli()
