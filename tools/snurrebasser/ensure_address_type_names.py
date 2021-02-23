import asyncio

import click
from mox_helpers.mox_util import ensure_class_value_helper

import constants
from exporters.utils.async_to_sync import async_to_sync
from exporters.utils.load_settings import load_settings

settings = load_settings()
# Mapped of settings (which has an uuid associated to it in the settings file)
# to BVN of the address types as defined in the constants.py file
var_map = {
    "opus.addresses.employee.dar": constants.addresses_employee_dar,
    "opus.addresses.employee.phone": constants.addresses_employee_phone,
    "opus.addresses.employee.email": constants.addresses_employee_email,
    "opus.addresses.unit.se": constants.addresses_unit_se,
    "opus.addresses.unit.cvr": constants.addresses_unit_cvr,
    "opus.addresses.unit.ean": constants.addresses_unit_ean,
    "opus.addresses.unit.pnr": constants.addresses_unit_pnr,
    "opus.addresses.unit.phoneNumber": constants.addresses_unit_phoneNumber,
    "opus.addresses.unit.dar": constants.addresses_unit_dar,
}


@click.command()
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dry run and print the commands",
)
@async_to_sync
async def ensure_address_type_bvns(dry_run: bool):
    """Tool for ensuring classes for address types have correct BVN.

    It relies on uuids set in settings.json file and uses mox_util to ensure their BVN's are consistent.
    """
    for setting, bvn in var_map.items():
        uuid = settings[setting]
        try:
            await ensure_class_value_helper(
                mox_base=settings["mox.base"],
                uuid=uuid,
                variable="brugervendtnoegle",
                new_value=bvn,
                dry_run=dry_run,
            )
        except IndexError:
            print("Found no {} at {}".format(bvn, uuid))


if __name__ == "__main__":
    ensure_address_type_bvns()
