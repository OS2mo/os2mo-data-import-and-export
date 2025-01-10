import click
import httpx
from fastramqpi.ra_utils.load_settings import load_setting


def delete_object_and_orgfuncs(
    uuid: str, mox_base: str, object_type: str, dry_run: bool = False
):
    assert object_type in ("bruger", "organisationenhed")
    rel = "tilknyttedebrugere" if object_type == "bruger" else "tilknyttedeenheder"
    response = httpx.get(f"{mox_base}/organisation/organisationfunktion?{rel}={uuid}")
    response.raise_for_status()
    org_funcs = response.json()["results"][0]
    if dry_run:
        return org_funcs
    for org_func_uuid in org_funcs:
        r = httpx.delete(
            f"{mox_base}/organisation/organisationfunktion/{org_func_uuid}"
        )
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
@click.option("--dry-run", is_flag=True)
def cli(uuid, mox_base, object_type, dry_run):
    """Deletes a user or org_unit from lora along with all associated org-funcs."""

    org_funcs = delete_object_and_orgfuncs(
        uuid=uuid,
        mox_base=mox_base,
        object_type="bruger" if object_type == "user" else "organisationenhed",
        dry_run=dry_run,
    )
    if dry_run:
        click.echo(f"Dry-run - would delete {len(org_funcs)+1} objects in lora")


if __name__ == "__main__":
    cli()
