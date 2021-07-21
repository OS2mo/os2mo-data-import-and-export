import click
from integrations.os2sync.os2sync import delete_user
import config

settings = config.settings

BASE = settings["OS2MO_SERVICE_URL"]

@click.command()
@click.option('--uuid', type=click.UUID)
def delete_fk_org_user(uuid):
    delete_user(str(uuid))

if __name__ == '__main__':
    delete_fk_org_user()
