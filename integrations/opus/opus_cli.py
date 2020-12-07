import json

import pathlib

import click

from integrations.opus.opus_exceptions import RunDBInitException
from integrations.opus.opus_helpers import (start_opus_import, update_employee,
                               start_opus_diff)
from integrations.ad_integration import ad_reader
from os2mo_data_import import ImportHelper


@click.group()
def cli():
    """Common entrypoint to OPUS programs."""
    pass


@click.command(help="Perform initial import")
def initial():
    ad = ad_reader.ADParameterReader()

    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    settings = json.loads(cfg_file.read_text())

    importer = ImportHelper(
        create_defaults=True,
        mox_base=settings['mox.base'],
        mora_base=settings['mora.base'],
        store_integration_data=False,
        seperate_names=True,
        demand_consistent_uuids=False,
    )

    try:
        start_opus_import(importer, ad_reader=ad, force=True)
    except RunDBInitException:
        print('RunDB not initialized')


@click.command(help="Perform diff import")
def update():
    ad = ad_reader.ADParameterReader()
    try:
        start_opus_diff(ad_reader=ad)
    except RunDBInitException:
        print('RunDB not initialized')


@click.command(help="Update a single employee")
@click.argument('employment_number')
@click.option('--days', default=1000, help='Number of days in the past to search')
def update_single_employee(employment_number, days):
    update_employee(employment_number, days)


cli.add_command(initial)
cli.add_command(update)
cli.add_command(update_single_employee)

if __name__ == "__main__":
    cli()
