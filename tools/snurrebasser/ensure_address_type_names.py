import subprocess

import click
from exporters.utils.load_settings import load_settings

settings = load_settings()
# List of BVN's to be set based on values defined in opus_import.py
# Mapped to a uuid defined in the settings file
var_map = {
    "opus.addresses.employee.dar": "AdressePostEmployees",
    "opus.addresses.employee.phone": "PhoneEmployee",
    "opus.addresses.employee.email": "EmailEmployee",
    "opus.addresses.unit.se": "SE",
    "opus.addresses.unit.cvr": "CVR",
    "opus.addresses.unit.ean": "EAN",
    "opus.addresses.unit.pnr": "Pnummer",
    "opus.addresses.unit.phoneNumber": "PhoneUnit",
    "opus.addresses.unit.dar": "AddressPostUnit"}


basecommand = "venv/bin/python os2mo_data_import/mox_helpers/mox_util.py cli ensure-class-value --uuid {} --variable brugervendtnoegle --new_value {}"

@click.command()
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dry run and print the commands",
)
def ensure_address_type_names(dry_run: bool):
    """ Tool for ensuring classes for address types have correct names.
    It relies on uuids set in settings file and uses mox_util to ensure their BVN's are consistent

    """
    for setting, name in var_map.items():
        command = basecommand.format(settings[setting], name)
        if dry_run:
            click.echo(command)
        else:
            p = subprocess.run(command, shell=True)

if __name__ == '__main__':
    ensure_address_type_names()