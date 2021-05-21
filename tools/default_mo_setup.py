import constants
from os2mo_data_import import ImportHelper
from mox_helpers import mox_util
from rautils.load_settings import load_settings
from integrations.opus import opus_helpers


def create_new_root_and_it(settings=None) -> None:
    """Setup all necessary classes etc to perform opus-import."""
    settings = settings or load_settings()
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
    main_name = settings.get("municipality.name", "Magenta ApS")
    main_uuid = opus_helpers.generate_uuid(main_name)
    os2mo.add_organisation(
        identifier=main_name,
        uuid=str(main_uuid),
        user_key=main_name,
        municipality_code=settings.get("municipality.code", 1234),
    )
    os2mo.new_itsystem(
        identifier=constants.Opus_it_system, system_name=constants.Opus_it_system
    )
    os2mo.new_itsystem(
        identifier=constants.AD_it_system, system_name=constants.AD_it_system
    )

    # Perfom setup of root unit and it systems.
    os2mo.import_all()

def ensure_default_classes():
    """Ensures the defined set of default classes exists in lora."""
    default_classes = [
        {
            "klasse": "primary",
            "facet": "primary_type",
            "title": "Ansat",
            "scope": "3000",
        },
        {
            "klasse": "non-primary",
            "facet": "primary_type",
            "title": "Ikke-primær ansættelse",
            "scope": "0",
        },
        {
            "klasse": "explicitly-primary",
            "facet": "primary_type",
            "title": "Manuelt primær ansættelse",
            "scope": "5000",
        },
        {
            "klasse": "Intern",
            "facet": "visibility",
            "title": "Må vises internt",
            "scope": "INTERNAL",
        },
        {
            "klasse": "Public",
            "facet": "visibility",
            "title": "Må vises eksternt",
            "scope": "PUBLIC",
        },
        {
            "klasse": "Secret",
            "facet": "visibility",
            "title": "Hemmelig",
            "scope": "SECRET",
        },
        {
            "klasse": "AD-Mobil",
            "facet": "employee_address_type",
            "title": "AD-Mobil",
            "scope": "PHONE",
        },
        {
            "klasse": "AD-PhoneEmployee",
            "facet": "employee_address_type",
            "title": "AD-Telefon",
            "scope": "PHONE",
        },
        {
            "klasse": "AD-EmailEmployee",
            "facet": "employee_address_type",
            "title": "AD-Email",
            "scope": "EMAIL",
        },
    ]
    for klasse in default_classes:
        mox_util.ensure_class_in_lora(**klasse)

if __name__ == "__main__":
    create_new_root_and_it()
    ensure_default_classes()