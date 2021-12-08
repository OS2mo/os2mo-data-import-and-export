import click
import httpx
from ra_utils.load_settings import load_setting


def delete_user_and_orgfuncs(user_uuid, mox_base):
    response = httpx.get(
        f"{mox_base}/organisation/organisationfunktion?vilkaarligrel={user_uuid}"
    )
    response.raise_for_status()
    org_funcs = response.json()["results"][0]
    for uuid in org_funcs:
        r = httpx.delete(f"{mox_base}/organisation/organisationfunktion/{uuid}")
        r.raise_for_status()

    r = httpx.delete(f"{mox_base}/organisation/bruger/{user_uuid}")
    r.raise_for_status()


@click.command()
@click.option("--user-uuid", type=click.UUID, required=True)
@click.option("--mox-base", default=load_setting("mox.base"))
def cli(user_uuid, mox_base):
    delete_user_and_orgfuncs(user_uuid, mox_base)


if __name__ == "__main__":
    cli()
