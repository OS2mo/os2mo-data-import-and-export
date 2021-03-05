import click
import time
from datetime import datetime
from operator import itemgetter
from pathlib import Path

import requests
from more_itertools import flatten, pairwise, prepend
from tqdm import tqdm
from functools import lru_cache
import constants
from exporters.utils.load_settings import load_settings
from integrations.ad_integration import ad_reader
from integrations.ad_integration.utils import apply
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import OpusDiffImport
from os2mo_data_import import ImportHelper


def perform_setup(
    settings=None, roles: set = None, engagement_types: set = None
) -> None:
    """Setup all necessary classes etc to perform opus-import.
    
    Takes a set of roles and engagement_types as input and creates classes for them.
    """
    settings = settings or load_settings()
    roles = roles or set()
    engagement_types = engagement_types or set()
    mox_base = settings.get("mox.base", "http://localhost:8080")
    mora_base = settings.get("mora.base", "http://localhost:5000")
    
    # Init
    os2mo = ImportHelper(
        create_defaults=True,
        store_integration_data=True,
        mox_base=mox_base,
        mora_base=mora_base,
    )

    # The Organisation class is the main entry point,
    # It exposes the related sub classes such as:
    # Facet, Klasse, Itsystem, OrganisationUnit, Employee
    main_name = settings.get('municipality.name', 'Magenta APS')
    main_uuid = opus_helpers.generate_uuid(main_name)
    os2mo.add_organisation(
        identifier=main_name,
        uuid=str(main_uuid),
        user_key=main_name,
        municipality_code=settings.get('municipality.code', 1234),
    )

    # Add klasse with reference to facet "org_unit_type"
    classes_to_create = [
        {"identifier": "primary", "facet_type_ref": "primary_type", "title": "primary"},
        {
            "identifier": "non-primary",
            "facet_type_ref": "primary_type",
            "title": "non-primary",
        },
        {
            "identifier": "explicitly-primary",
            "facet_type_ref": "primary_type",
            "title": "explicitly-primary",
        },
        {
            "identifier": "AddressPostUnit",
            "facet_type_ref": "org_unit_address_type",
            "title": "Adresse",
            "scope": "DAR",
        },
        {
            "identifier": "Organisation",
            "facet_type_ref": "org_unit_type",
            "title": "Organisation",
        },
        {
            "identifier": "Afdeling",
            "facet_type_ref": "org_unit_type",
            "title": "Afdeling",
        },
        {
            "identifier": "Niveau 1",
            "facet_type_ref": "org_unit_level",
            "user_key": "Niveau 1",
            "title": "Niveau 1",
        },
        {
            "identifier": "Niveau 2",
            "facet_type_ref": "org_unit_level",
            "user_key": "Niveau 2",
            "title": "Niveau 2",
        },
        {
            "identifier": "Niveau 3",
            "facet_type_ref": "org_unit_level",
            "user_key": "Niveau 3",
            "title": "Niveau 3",
        },
        {
            "identifier": "Arbejdstidsplaner",
            "facet_type_ref": "time_planning",
            "user_key": "Arbejdstidsplaner",
            "title": "Arbejdstidsplaner",
        },
        {
            "identifier": "Tjenestetid",
            "facet_type_ref": "time_planning",
            "user_key": "Tjenestetid",
            "title": "Tjenestetid",
        },
        {
            "identifier": "Lederansvar",
            "facet_type_ref": "responsibility",
            "title": "Lederansvar",
        },
        {
            "identifier": "EAN",
            "facet_type_ref": "org_unit_address_type",
            "title": "EAN-nr.",
            "scope": "EAN",
            "example": "1234567890123",
        },
        {
            "identifier": "PhoneUnit",
            "facet_type_ref": "org_unit_address_type",
            "title": "Tlf",
            "scope": "PHONE",
            "example": "20304060",
        },
        {
            "identifier": "SE",
            "facet_type_ref": "org_unit_address_type",
            "title": "SE",
            "scope": "TEXT",
        },
        {
            "identifier": "CVR",
            "facet_type_ref": "org_unit_address_type",
            "title": "CVR",
            "scope": "TEXT",
        },
        {
            "identifier": "Pnummer",
            "facet_type_ref": "org_unit_address_type",
            "title": "Pnummer",
            "scope": "TEXT",
        },
        {
            "identifier": "AdressePostEmployee",
            "facet_type_ref": "employee_address_type",
            "title": "Adresse",
            "scope": "DAR",
        },
        {
            "identifier": "PhoneEmployee",
            "facet_type_ref": "employee_address_type",
            "title": "Telefon",
            "scope": "PHONE",
        },
        {
            "identifier": "EmailEmployee",
            "facet_type_ref": "employee_address_type",
            "scope": "EMAIL",
        },
        {
            "identifier": "AD-AdressePostEmployee",
            "facet_type_ref": "employee_address_type",
            "title": "AD-Adresse",
            "scope": "DAR",
        },
        {
            "identifier": "AD-PhoneEmployee",
            "facet_type_ref": "employee_address_type",
            "title": "AD-Telefon",
            "scope": "PHONE",
        },
        {
            "identifier": "AD-EmailEmployee",
            "facet_type_ref": "employee_address_type",
            "scope": "AD-EMAIL",
        },
    ]
    role_classes = [
        {"identifier": role, "facet_type_ref": "role_type", "title": role}
        for role in roles
    ]
    engagement_type_classes = [
        {"identifier": eng, "facet_type_ref": "engagement_type", "title": eng}
        for eng in engagement_types
    ]
    classes_to_create += role_classes
    classes_to_create += engagement_type_classes

    for klasses in classes_to_create:
        os2mo.add_klasse(**klasses)

    os2mo.new_itsystem(
        identifier=constants.Opus_it_system, system_name=constants.Opus_it_system
    )
    os2mo.new_itsystem(
        identifier=constants.AD_it_system, system_name=constants.AD_it_system
    )

    # Perfom setup of defined classes etc.
    os2mo.import_all()


def truncate_db(MOX_BASE="http://localhost:8080"):
    r = requests.get(MOX_BASE + "/db/truncate")
    r.raise_for_status()


def to_job_type(e):
    return e.get("workContractText", "Ansat")


def to_roles(e):
    function = e.get("function")
    if function:
        roles = set()
        if type(function) == list:
            for f in e.get("function"):
                roles.add(f.get("artText"))
        else:
            roles.add(function.get("artText"))
        return roles
    return set()

def read_all_employees():
    """Make a list of all employee data from all files"""
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])

    dumps = opus_helpers.read_available_dumps()
    full_data = map(
        lambda d: opus_helpers.parser(d, filter_ids),
        tqdm(dumps.values(), desc="Reading opusfiles"),
    )
    all_employees = flatten(map(itemgetter(1), full_data))
    return all_employees


def read_all_files(filter_ids):
    """Create full list of data to write to MO.

    Searches files pairwise for changes and returns the date of change, units and employees with changes
    """
    dumps = opus_helpers.read_available_dumps()

    export_dates = prepend(None, sorted(dumps.keys()))

    @apply
    def lookup_units_and_employees(date1, date2):
        filename1 = dumps.get(date1)
        filename2 = dumps[date2]
        units, employees = opus_helpers.file_diff(
            filename1, filename2, filter_ids, disable_tqdm=True
        )
        print(f"{date2}: Found {len(units)} units and {len(employees)} employees")
        return date2, units, employees

    return map(lookup_units_and_employees, tqdm(pairwise(export_dates)))


def setup_new_mo(settings=load_settings()):
    """Create fresh MO setup with nessecary classes

    Clear MO database and read through all files to find roles and engagement_types
    """
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])

    truncate_db(settings.get('mox.base'))

    
    # Get all distinct roles and job types
    #TODO: This is quite bad as we have to read all files twice as reading all employees into a list is not an option with ~300 files. 
    #IF opus-import could create new roles and engagement types this entire step would be unnecessary.
    employees = read_all_employees()
    engagement_types = set(map(to_job_type, employees))
    
    employees = read_all_employees()
    roles = set.union(*map(to_roles, employees))
    
    # Setup classes and root organisation
    perform_setup(settings=settings, roles=roles, engagement_types=engagement_types)


def import_all(ad_reader=None):
    """Do a clean import from all opus files.
    Removes current OS2MO data and creates new installation with all the required classes etc.
    Then reads all opus files from the path defined in settings
    """
    settings = load_settings()
    filter_ids = settings.get("integrations.opus.units.filter_ids", [])

    setup_new_mo(settings=settings)
    employee_mapping = opus_helpers.read_cpr_mapping()
    date_units_and_employees = read_all_files(filter_ids)
    for date, units, employees in date_units_and_employees:
        diff = OpusDiffImport(
            date, ad_reader=ad_reader, employee_mapping=employee_mapping
        )
        diff.start_import(units, employees, include_terminations=True)
        #Write latest successful import to rundb so opus_diff_import can continue from where this ended
        opus_helpers.local_db_insert((date, 'Diff update ended: {}'))


@click.command()
@click.option('--import-amount',
              type=click.Choice(['none', 'all'], case_sensitive=False), required=True)
@click.option('--use-ad', is_flag=True, type=click.BOOL, default=False,
              help="Read from AD")
def clear_and_reload(import_amount, use_ad):
    if import_amount == 'all':
        AD = ad_reader.ADParameterReader() if use_ad else None
        import_all(ad_reader=AD)
    else:
        setup_new_mo()
    

if __name__ == "__main__":
    clear_and_reload()
