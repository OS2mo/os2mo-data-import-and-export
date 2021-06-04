import json
import pathlib

import click
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup

from ra_utils.load_settings import load_settings
from integrations.ad_integration.ad_reader import ADParameterReader
from integrations.opus.opus_helpers import update_employee
from integrations.opus.opus_diff_import import start_opus_diff
from integrations.opus.opus_import import start_opus_import
from integrations.opus.opus_exceptions import RunDBInitException
from os2mo_data_import import ImportHelper


@click.command(help="Furesø import")
@optgroup.group("Action", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option("--import", is_flag=True, help='New import into empty MO')
@optgroup.option("--update", is_flag=True, help='Update with next xml file')
@optgroup.option(
    "--update-single-user",
    help='Update a single user, specify employee number',
)
@click.option(
    "--days",
    type=int,
    default=1000,
    help='Number of days in the past to sync single user (default=1000)',
)
def cli(**args):
    SETTINGS = load_settings()

    ad_reader = ADParameterReader()

    if args['update']:
        try:
            start_opus_diff(ad_reader=ad_reader)
        except RunDBInitException:
            print('RunDB not initialized')

    if args['import']:
        importer = ImportHelper(
            create_defaults=True,
            mox_base=SETTINGS['mox.base'],
            mora_base=SETTINGS['mora.base'],
            store_integration_data=False,
            seperate_names=True,
            demand_consistent_uuids=False
        )

        med_name = 'MED Organisation'
        importer.add_klasse(
            identifier=med_name,
            facet_type_ref='org_unit_type',
            user_key=med_name,
            scope='TEXT',
            title=med_name
        )

        importer.add_organisation_unit(
            identifier=med_name,
            name=med_name,
            user_key=med_name,
            type_ref=med_name,
            date_from='1930-01-01',
            date_to=None,
            parent_ref=None
        )

        try:
            start_opus_import(importer, ad_reader=ad_reader, force=True)
        except RunDBInitException:
            print('RunDB not initialized')

    if args['update_single_user']:
        employment_number = args['update_single_user']
        days = args['days']
        update_employee(employment_number, days)


if __name__ == "__main__":
    cli()