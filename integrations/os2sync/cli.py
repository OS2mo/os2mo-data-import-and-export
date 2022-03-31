import click
from ra_utils.load_settings import load_setting
from integrations.os2sync.os2mo import get_sts_orgunit, get_sts_user, os2mo_get
from integrations.os2sync.config import get_os2sync_settings
from requests import HTTPError
from operator import itemgetter
from integrations.os2sync import os2sync

@click.command()
@click.argument("uuid", type=click.UUID)
@click.option("--mora-base", default=load_setting("mora.base"))
@click.option("--dry-run", is_flag=True)
def cli(uuid, mora_base, dry_run):
    #lookup uuid, try person, then org_unit if person wasn't found
    settings = get_os2sync_settings()
    obj_type = None
    try:
        obj = os2mo_get(f"{{BASE}}/e/{str(uuid)}/").json()
        obj_type = "person"
        
    except HTTPError as e:
        assert e.response.status_code == 404
        obj = os2mo_get(f"{{BASE}}/ou/{str(uuid)}/").json()
        obj_type = "orgunit"
    

    if obj_type == "person":
        
        sts_user = get_sts_user(obj["uuid"],  settings=settings)
        
        if dry_run:
            click.echo(sts_user)
            return
        
        if sts_user["Positions"]:
            os2sync.upsert_user(sts_user)    
        else:
            os2sync.delete_user(uuid)
    elif obj_type == "orgunit":
        sts_org_unit = get_sts_orgunit(obj["uuid"], settings=settings)
    
        if dry_run:
            click.echo(sts_org_unit)
            return
        
    else:
        raise ValueError("WTF happend here!?")


if __name__ == "__main__":
    cli()