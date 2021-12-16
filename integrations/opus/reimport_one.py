import click
from ra_utils.load_settings import load_setting

from integrations.opus.clear_and_import_opus import import_opus
from tools.data_fixers.remove_user_from_lora import delete_object_and_orgfuncs


@click.command()
@click.option("--mox-base", default=load_setting("mox.base"))
@click.option("--delete-uuid", type=click.UUID)
@click.option("--opus-id", type=int, required=True)
@click.option(
    "--object-type",
    type=click.Choice(["user", "org_unit"], case_sensitive=False),
    required=True,
)
def cli(mox_base, delete_uuid, opus_id, object_type):
    """Reimport object from opus
    Reads all the dumps from opus and imports object with given ID to MO
    Optionally deletes the object with given uuid directly from Lora.
    """

    if delete_uuid:
        delete_object_and_orgfuncs(
            delete_uuid,
            mox_base,
            "bruger" if object_type == "user" else "organisationsenhed",
        )
    import_opus(ad_reader=None, import_all=True, opus_id=opus_id)


if __name__ == "__main__":
    cli()
