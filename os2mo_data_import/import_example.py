# -- coding: utf-8 --
from uuid import uuid4

from os2mo_data_import import ImportHelper

def example_import():
    """
    Run the example to import the fictional organisation Magenta.

    """

    # Init
    os2mo = ImportHelper(create_defaults=True, store_integration_data=True)


    # The Organisation class is the main entry point,
    # It exposes the related sub classes such as:
    # Facet, Klasse, Itsystem, OrganisationUnit, Employee

    os2mo.add_organisation(
        identifier="Magenta Aps",
        user_key="Magenta",
        municipality_code=101
    )

    # Add klasse with reference to facet "org_unit_type"
    os2mo.add_klasse(
        identifier="Hovedenhed",
        facet_type_ref="org_unit_type",
        user_key="D1ED90C5-643A-4C12-8889-6B4174EF4467",
        title="Hovedenhed"  # This is the displayed value
    )

    os2mo.add_klasse(
        identifier="Afdeling",
        facet_type_ref="org_unit_type",
        user_key="91154D1E-E7CA-439B-B910-D4622FD3FD21",
        title="Afdeling"
    )


    # Root unit: Magenta
    # Belongs to unit type: "Hovedenhed"
    os2mo.add_organisation_unit(
        identifier="Magenta",
        name="Magenta Aps",
        type_ref="Hovedenhed",  # Reference to the unit type
        date_from="1986-01-01"
    )

    # Use parent_ref to make it a sub group of "Magenta"
    os2mo.add_organisation_unit(
        identifier="Pilestræde",
        type_ref="Afdeling",  # This unit is of type: Afdeling
        parent_ref="Magenta",  # Sub unit of/Belongs to Magenta
        date_from="1986-01-01"
    )

    os2mo.add_organisation_unit(
        identifier="SJA2",
        type_ref="Afdeling",
        parent_ref="Magenta",  # Sub unit of/Belongs to Magenta
        date_from="1986-01-01",
    )



    # HOTFIX: nested units are failing
    # This will 'really' be fixed in the refactoring state

    # Adding sub units to the example
    os2mo.add_organisation_unit(
        identifier="Sysadmins",
        type_ref="Afdeling",
        parent_ref="SJA2",  # Sub unit of/Belongs to SJA2
        date_from="1986-01-01"
    )

    os2mo.add_organisation_unit(
        identifier="Dummy",
        type_ref="Afdeling",
        parent_ref="Sysadmins",  # Sub unit of/Belongs to SJA2
        date_from="1986-01-01"
    )

    # Address Types
    os2mo.add_klasse(
        identifier="AddressMailUnit",
        facet_type_ref="org_unit_address_type",
        title="Adresse",
        scope="DAR",
        example="<UUID>"
    )

    os2mo.add_address_type(
        organisation_unit="Magenta",
        value="0a3f50c4-379f-32b8-e044-0003ba298018",
        type_ref="AdressePost",
        date_from="1986-01-01"
    )

    os2mo.add_klasse(
        identifier="EAN",
        facet_type_ref="org_unit_address_type",
        user_key="C8EC85B4-A088-434A-B034-CA08A9FD655A",
        title="EAN-nr.",
        scope="EAN",
        example="00112233"
    )

    os2mo.add_address_type(
        organisation_unit="Magenta",
        value="00112233",
        type_ref="EAN",
        date_from="1986-01-01",
    )

    os2mo.add_klasse(
        identifier="PhoneUnit",
        facet_type_ref="org_unit_address_type",
        title="Tlf",
        scope="PHONE",
        example="20304060"
    )

    os2mo.add_address_type(
        organisation_unit="Magenta",
        value="11223344",
        type_ref="Telefon",
        date_from="1986-01-01",
    )

    # Create job functions and assign to employees

    # Add job functions
    os2mo.add_klasse(
        identifier="Direktør",
        facet_type_ref="engagement_type",
        user_key="Direktør",
        title="Direktør"
    )

    os2mo.add_klasse(
        identifier="Projektleder",
        facet_type_ref="engagement_type",
        user_key="Projektleder",
        title="Projektleder"
    )

    os2mo.add_klasse(
        identifier="Udvikler",
        facet_type_ref="engagement_type",
        user_key="Udvikler",
        title="Udvikler"
    )

    os2mo.add_klasse(
        identifier="Projektmedarbejder",
        facet_type_ref="engagement_type",
        user_key="Projektmedarbejder",
        title="Projektmedarbejder"
    )

    os2mo.add_employee(
        identifier="Susanne Chæf",
        cpr_no="0101862233"
    )

    os2mo.add_employee(
        identifier="Odin Perskov",
        cpr_no="0102862234"
    )

    os2mo.add_employee(
        identifier="Ronja Rwander",
        cpr_no="0103862234"
    )

    os2mo.add_employee(
        identifier="Jens Mortensen",
        cpr_no="0104862235"
    )

    os2mo.add_employee(
        identifier="Bolette Buhl",
        cpr_no="0105862235"
    )

    os2mo.add_employee(
        identifier="Carl Sand Holth",
        cpr_no="0106862235"
    )

    # Assign job functions
    os2mo.add_klasse(
        identifier="Ansat",
        facet_type_ref="engagement_type",
        user_key="CF297115-309B-4535-88C8-5BA41C90929B",
        title="Ansat"
    )

    os2mo.add_engagement(
        employee="Susanne Chæf",
        organisation_unit="Magenta",
        job_function_ref="Direktør",
        engagement_type_ref="Ansat",
        date_from="1986-01-01"
    )

    os2mo.add_engagement(
        employee="Odin Perskov",
        organisation_unit="Pilestræde",
        job_function_ref="Projektleder",
        engagement_type_ref="Ansat",
        date_from="1986-02-01"
    )

    os2mo.add_engagement(
        employee="Ronja Rwander",
        organisation_unit="SJA2",
        job_function_ref="Projektleder",
        engagement_type_ref="Ansat",
        date_from="1986-03-01"
    )

    os2mo.add_engagement(
        employee="Jens Mortensen",
        organisation_unit="Pilestræde",
        job_function_ref="Udvikler",
        engagement_type_ref="Ansat",
        date_from="1986-04-01"
    )

    os2mo.add_engagement(
        employee="Bolette Buhl",
        organisation_unit="SJA2",
        job_function_ref="Udvikler",
        engagement_type_ref="Ansat",
        date_from="1986-05-01"
    )

    os2mo.add_engagement(
        employee="Carl Sand Holth",
        organisation_unit="Pilestræde",
        job_function_ref="Projektmedarbejder",
        engagement_type_ref="Ansat",
        date_from="1986-06-01"
    )


    # Association
    os2mo.add_klasse(
        identifier="Ekstern Konsulent",
        facet_type_ref="association_type",
        user_key="F997F306-71DF-477C-AD42-E753F9C21B42",
        title="Ekstern Konsulent"
    )

    os2mo.add_association(
        employee="Carl Sand Holth",
        organisation_unit="Pilestræde",
        job_function_ref="Projektmedarbejder",
        association_type_ref="Ekstern Konsulent",
        address_uuid="0a3f50c4-379f-32b8-e044-0003ba298018",
        date_from="1986-10-01"
    )

    os2mo.add_klasse(
        identifier="AdressePostEmployee",
        facet_type_ref="employee_address_type",
        user_key="2F29C717-5D78-4AA9-BDAE-7CDB3A378018",
        title="Adresse",
        scope="DAR",
        example="<UUID>"
    )

    os2mo.add_address_type(
        employee="Susanne Chæf",
        value="0a3f50a0-ef5a-32b8-e044-0003ba298018",
        type_ref="AdressePostEmployee",
        date_from="1986-11-01",
    )

    os2mo.add_address_type(
        employee="Odin Perskov",
        value="0a3f50a0-ef5a-32b8-e044-0003ba298018",
        type_ref="AdressePostEmployee",
        date_from="1986-11-01",
    )


    os2mo.add_address_type(
        employee="Ronja Rwander",
        value="0a3f50a0-ef5a-32b8-e044-0003ba298018",
        type_ref="AdressePostEmployee",
        date_from="1986-11-01",
    )

    os2mo.add_address_type(
        employee="Jens Mortensen",
        value="0a3f50a0-ef5a-32b8-e044-0003ba298018",
        type_ref="AdressePostEmployee",
        date_from="1986-11-01",
    )

    os2mo.add_address_type(
        employee="Bolette Buhl",
        value="0a3f50a0-ef5a-32b8-e044-0003ba298018",
        type_ref="AdressePostEmployee",
        date_from="1986-11-01",
    )

    os2mo.add_address_type(
        employee="Carl Sand Holth",
        value="0a3f50a0-ef5a-32b8-e044-0003ba298018",
        type_ref="AdressePostEmployee",
        date_from="1986-11-01",
    )


    # Add roles and assign to employees
    os2mo.add_klasse(
        identifier="Medarbejder repræsentant",
        facet_type_ref="role_type",
        user_key="893A0670-BAFB-4DDF-8270-0EDACE6C520C",
        title="Medarbejder repræsentant"
    )

    os2mo.add_klasse(
        identifier="Nøgleansvarlig",
        facet_type_ref="role_type",
        user_key="0E078F23-A5B4-4FB4-909B-60E49295C5E9",
        title="Nøgleansvarlig"
    )


    os2mo.add_role(
        employee="Susanne Chæf",
        organisation_unit="Magenta",
        role_type_ref="Nøgleansvarlig",
        date_from="1986-12-01"
    )

    os2mo.add_role(
        employee="Bolette Buhl",
        organisation_unit="SJA2",
        role_type_ref="Medarbejder repræsentant",
        date_from="1986-12-01"
    )

    os2mo.add_role(
        employee="Jens Mortensen",
        organisation_unit="Pilestræde",
        role_type_ref="Medarbejder repræsentant",
        date_from="1986-12-01"
    )


    # Create manager type, level and responsibilites
    # and assign to employee

    # Manager type
    os2mo.add_klasse(
        identifier="Leder",
        facet_type_ref="manager_type",
        user_key="55BD7A09-86C3-4E15-AF5D-EAD20EB12F81",
        title="Virksomhedens direktør"
    )

    # Manager level
    os2mo.add_klasse(
        identifier="Højeste niveau",
        facet_type_ref="manager_level",
        user_key="6EAA7DA7-212D-4FD0-A068-BA3F932FDB10",
        title="Højeste niveau"
    )

    # Add responsabilities
    os2mo.add_klasse(
        identifier="Tage beslutninger",
        facet_type_ref="responsibility",
        user_key="A9ABDCCB-EC83-468F-AB7D-175B95E94956",
        title="Tage beslutninger"
    )

    os2mo.add_klasse(
        identifier="Motivere medarbejdere",
        facet_type_ref="responsibility",
        user_key="DC475AF8-21C9-4112-94AE-E9FB13FE8D14",
        title="Motivere medarbejdere"
    )

    os2mo.add_klasse(
        identifier="Betale løn",
        facet_type_ref="responsibility",
        user_key="0A929060-3392-4C07-8F4E-EF5F9B6AFDE2",
        title="Betale løn"
    )

    os2mo.add_manager(
        employee="Susanne Chæf",
        organisation_unit="Magenta",
        manager_type_ref="Leder",
        manager_level_ref="Højeste niveau",
        responsibility_list=["Tage beslutninger", "Motivere medarbejdere", "Betale løn"],
        date_from="1987-12-01",
    )

    # Leave of absence (Does not work after release 0.10)
    # Leave type requires an exisiting engagement type

    os2mo.add_klasse(
        identifier="Sygeorlov",
        facet_type_ref="leave_type",
        user_key="DB8E39C3-9160-47DB-A314-B0F8D1A2D536",
        title="Sygeorlov"
    )

    os2mo.add_leave(
        employee="Jens Mortensen",
        leave_type_ref="Sygeorlov",
        date_from="2018-01-22",
        date_to="2018-11-02"
    )

    os2mo.add_leave(
        employee="Bolette Buhl",
        leave_type_ref="Sygeorlov",
        date_from="2018-01-22",
        date_to="2018-11-02"
    )

    # Create IT system and assign to employee

    os2mo.new_itsystem(
        identifier="Servermiljø",
        system_name="Servermiljø"
    )

    os2mo.join_itsystem(
        employee="Jens Mortensen",
        user_key="jmort",
        itsystem_ref="Servermiljø",
        date_from="1987-10-01"
    )

    os2mo.join_itsystem(
        employee="Bolette Buhl",
        user_key="bolbu",
        itsystem_ref="Servermiljø",
        date_from="1987-10-01"
    )

    os2mo.import_all()


if __name__ == "__main__":
    example_import()
