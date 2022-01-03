import click
import httpx
from ra_utils.load_settings import load_setting


def delete_object_and_orgfuncs(uuid, mox_base, object_type):
    assert object_type in ("bruger", "organisationenhed")
    rel = "tilknyttedebrugere" if object_type == "bruger" else "tilknyttedeenheder"
    response = httpx.get(f"{mox_base}/organisation/organisationfunktion?{rel}={uuid}")
    response.raise_for_status()
    org_funcs = response.json()["results"][0]
    for uuid in org_funcs:
        r = httpx.delete(f"{mox_base}/organisation/organisationfunktion/{uuid}")
        r.raise_for_status()

    r = httpx.delete(f"{mox_base}/organisation/{object_type}/{uuid}")
    r.raise_for_status()


@click.command()
@click.option("--uuid", type=click.UUID, required=True)
@click.option("--mox-base", default=load_setting("mox.base"))
@click.option(
    "--object-type",
    type=click.Choice(["user", "org_unit"], case_sensitive=False),
    required=True,
)
def cli(uuid, mox_base, object_type):
    """Deletes a user or org_unit from lora along with all associated org-funcs."""

    delete_object_and_orgfuncs(
        uuid=uuid,
        mox_base=mox_base,
        object_type="bruger" if object_type == "user" else "organisationsenhed",
    )


if __name__ == "__main__":
    cli()
