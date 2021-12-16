import click
from ra_utils.load_settings import load_setting

from integrations.opus.clear_and_import_opus import import_opus
from tools.data_fixers.remove_user_from_lora import delete_user_and_orgfuncs


@click.group()
def cli():
    pass


@cli.command()
def unit(uuid):
    click.echo("Not implemented yet")


@cli.command()
@click.option("--mox-base", default=load_setting("mox.base"))
@click.option("--delete-uuid", type=click.UUID)
@click.option("--opus-id", type=int, required=True)
def user(mox_base, delete_uuid, opus_id):
    """Reimport user from opus
    Reads all the dumps from opus and imports user with given ID to MO
    Optionally deletes the user with given uuid directly from Lora.
    """
    if delete_uuid:
        delete_user_and_orgfuncs(delete_uuid, mox_base)
    import_opus(ad_reader=None, import_all=True, opus_id=opus_id)


if __name__ == "__main__":
    cli()
