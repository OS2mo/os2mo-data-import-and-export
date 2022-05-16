# Script to import a mapping of MO-uuids to FK-org uuids as it-accounts on org_units in OS2MO
# metacli import_fkorg_orgunits integrations/fk-org\ it\ systems/test_uuids.json --dry-run
import json
from datetime import datetime
from typing import Optional
from typing import Tuple

import click
from more_itertools import one
from more_itertools import only
from ra_utils.async_to_sync import async_to_sync
from raclients.modelclient.mo import ModelClient
from ramodels.mo.details import ITUser

import constants


class DryRunClient(ModelClient):
    async def upload(self, *args, **kwargs):
        click.echo(f"dry-run. Would create {len(args[0])} new accounts")

    async def edit(self, *args, **kwargs):
        click.echo(f"dry-run. Would edit {len(args[0])} accounts")


async def check_it_system_value(
    moclient: ModelClient, mo_uuid: str, fk_org_uuid: str
) -> Tuple[Optional[str], bool]:
    """Checks the fk_org it accounts of an org_unit
    Returns the (optional) uuid of the current fk_org_uuid it system account
    and a boolean which tells whether or not the account should be changed or created.
    """
    current_it_accounts = await moclient.get(f"/service/ou/{mo_uuid}/details/it")
    fk_org_account = only(
        filter(
            lambda it: it["itsystem"]["name"] == constants.FK_org_uuid_it_system,
            current_it_accounts.json(),
        )
    )
    if fk_org_account is None:
        account_uuid, change = None, True
    else:
        account_uuid = fk_org_account["uuid"]
        change = True if fk_org_account["user_key"] != fk_org_uuid else False
    return account_uuid, change


@click.command()
@click.argument("input-file")
@click.option("--mora-base", envvar="BASE_URL")
@click.option("--client-id", envvar="CLIENT_ID")
@click.option(
    "--client-secret",
    envvar="CLIENT_SECRET",
)
@click.option("--auth-server", envvar="AUTH_SERVER")
@click.option("--dry-run", is_flag=True)
@async_to_sync
async def cli(
    input_file, mora_base, client_id, client_secret, auth_server, dry_run
) -> None:
    # Read the file
    with open(input_file) as f:
        import_rows = json.load(f)
    # Setup MO client
    MoModelClient = DryRunClient if dry_run else ModelClient

    mo_model_client = MoModelClient(
        base_url=mora_base,
        client_secret=client_secret,
        client_id=client_id,
        auth_server=auth_server,
        auth_realm="mo",
    )
    # Find uuid of the fk-org it-system
    root = await mo_model_client.get("/service/o/")
    root_uuid = one(root.json())["uuid"]
    it_systems = await mo_model_client.get(f"/service/o/{root_uuid}/it/")
    fk_org_it = one(
        filter(
            lambda it: it["name"] == constants.FK_org_uuid_it_system, it_systems.json()
        )
    )
    edits = []
    creates = []
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for row in import_rows:
        account_uuid, changed = await check_it_system_value(
            mo_model_client, row["mo_uuid"], row["fk_org_uuid"]
        )
        payload = {
            "user_key": row["fk_org_uuid"],
            "itsystem_uuid": fk_org_it["uuid"],
            "org_unit_uuid": row["mo_uuid"],
            "from_date": today,
        }
        if account_uuid is None:
            creates.append(ITUser.from_simplified_fields(**payload))
        elif changed:
            payload["uuid"] = account_uuid
            edits.append(ITUser.from_simplified_fields(**payload))

    await mo_model_client.edit(edits)
    await mo_model_client.upload(creates)


if __name__ == "__main__":
    cli()
