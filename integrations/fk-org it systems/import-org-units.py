# Script to import a mapping of MO-uuids to FK-org uuids as it-accounts on org_units in OS2MO
# eg: python3 integrations/fk-org\ it\ systems/import-org-units.py --dry-run customers/Silkeborg/Silkeborg_it.uuids
import asyncio
import csv
from datetime import datetime
from operator import itemgetter
from typing import Callable
from typing import List

import click
from more_itertools import flatten
from more_itertools import one
from more_itertools import only
from more_itertools import partition
from ra_utils.async_to_sync import async_to_sync
from ra_utils.load_settings import load_setting
from raclients.modelclient.mo import ModelClient as MoModelClient
from ramodels.mo.details import ITUser

import constants


@click.command()
@click.argument("input-file")
@click.option("--mora-base", default=load_setting("mora.base"), envvar="BASE_URL")
@click.option(
    "--client-id", default=load_setting("crontab.client_id"), envvar="CLIENT_ID"
)
@click.option(
    "--client-secret",
    default=load_setting("crontab.client_secret"),
    envvar="CLIENT_SECRET",
)
@click.option(
    "--auth-server", default=load_setting("crontab.auth_server"), envvar="AUTH_SERVER"
)
@click.option("--dry-run", is_flag=True)
@async_to_sync
async def cli(
    input_file, mora_base, client_id, client_secret, auth_server, dry_run
) -> None:
    # Read the file
    with open(input_file) as f:
        import_rows = dict(csv.reader(f))
    # Setup MO client
    mo_model_client = MoModelClient(
        base_url=mora_base,
        client_secret=client_secret,
        client_id=client_id,
        auth_server=auth_server,
        auth_realm="mo",
    )
    # Find uuid of the fk-org it-system
    root = await mo_model_client.get("service/o/")
    root_uuid = one(root.json())["uuid"]
    it_systems = await mo_model_client.get(f"service/o/{root_uuid}/it/")
    fk_org_it = one(
        filter(lambda it: it["name"] == constants.FK_org_uuid_it_system, it_systems.json())
    )

    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Read current it-accounts to check for duplicates
    async def get_it_systems(mo_uuid):
        t = await mo_model_client.get(f"/service/ou/{mo_uuid}/details/it")
        return t.json()

    current_it_accounts = await asyncio.gather(*map(get_it_systems, import_rows.keys()))

    fk_org_accounts = list(
        flatten(
            filter(
                lambda it: it["itsystem"]["name"] == constants.FK_org_uuid_it_system,
                accounts,
            )
            for accounts in current_it_accounts
        )
    )
    has_existing_it_account = set(
        map(lambda it: it["org_unit"]["uuid"], fk_org_accounts)
    )

    # Find changed values of fk-org uuid
    changed_accounts = filter(
        lambda it: it["user_key"] != import_rows[it["org_unit"]["uuid"]],
        fk_org_accounts,
    )

    # We only need to create the new
    new_rows = dict(
        filter(lambda it: it[0] not in has_existing_it_account, import_rows.items())
    )

    # Make payloads to create it-accounts
    def to_it_user(row):
        mo_uuid, fk_org_uuid = row
        return ITUser.from_simplified_fields(
            **{
                "user_key": fk_org_uuid,
                "itsystem_uuid": fk_org_it["uuid"],
                "org_unit_uuid": mo_uuid,
                "from_date": today,
            }
        )

    new_payloads = list(map(to_it_user, new_rows.items()))

    # make payloads to edit existing it-accounts
    def to_edit_payload(fk_org_account):
        return {
            "type": "it",
            "uuid": fk_org_account["uuid"],
            "data": {
                "user_key": import_rows[fk_org_account["org_unit"]["uuid"]],
                "validity": {"from": today, "to": None},
            },
        }

    edit_payloads = list(map(to_edit_payload, changed_accounts))

    if dry_run:
        click.echo(f"Creating {len(new_payloads)} new it-systems")
        click.echo(f"Editing values of {len(edit_payloads)} it-systems")
        return

    # Post edits
    await asyncio.gather(
        *[
            mo_model_client.post(f"/service/details/edit", json=payload)
            for payload in edit_payloads
        ]
    )
    # Upload new
    await mo_model_client.upload(new_payloads)


if __name__ == "__main__":
    cli()
