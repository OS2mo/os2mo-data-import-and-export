# -- coding: utf-8 --

import os
from dawa import fuzzy_address
from logger import start_logging
from os2mo_data_import import ImportHelper



os2mo = ImportHelper(
    create_defaults=True, 
    store_integration_data=True
)


def create_default_klasse_types():

    os2mo.add_klasse(
        identifier="Hovedenhed",
        facet_type_ref="org_unit_type",
        user_key="Hovedenhed",
        title="Hovedenhed"
    )

    os2mo.add_klasse(
        identifier="Afdeling",
        facet_type_ref="org_unit_type",
        user_key="Afdeling",
        title="Afdeling"
    )

    os2mo.add_klasse(
        identifier="AddressMailUnit",
        facet_type_ref="org_unit_address_type",
        title="Adresse",
        scope="DAR",
        example="<UUID>"
    )

    os2mo.add_klasse(
        identifier="PhoneUnit",
        facet_type_ref="org_unit_address_type",
        title="Tlf",
        scope="PHONE",
        example="20304060"
    )

    os2mo.add_klasse(
        identifier="EAN",
        facet_type_ref="org_unit_address_type",
        user_key="EAN",
        title="EAN-nr.",
        scope="EAN",
        example="00112233"
    )


def import_org_unit(org, root_unit):

    org_type_txt = org.get("orgTypeTxt")

    if org_type_txt == "Organisation":
        return

    identifier = org.get("@id")
    org_name = org.get("longName")
    start_date = org.get("startDate")

    # Parent org
    parent_org = org.get("parentOrgUnit")
    if not parent_org:
        parent_org = root_unit

    identifier = item["@id"]
    uuid = item["_id"]
    name = item["longName"]
    date_from = item["startDate"]
    date_to = item["endDate"]

    phone = item["phoneNumber"]
    ean = item["eanNr"]

    address_string = item["street"]
    zip_code = item["zipCode"]
    city = item["city"]

    os2mo.add_organisation_unit(
        identifier=identifier,
        name=name,
        user_key=uuid
        parent_ref=parent_org,
        type_ref="Afdeling",
        date_from=date_from,
        date_to=date_to
    )

    os2mo.add_address_type(
        organisation_unit=identifier,
        value=phone,
        type_ref="PhoneUnit",
        date_from=date_from
    )

    address_uuid = fuzzy_address(
        address_string=address_string,
        zip_code=zip_code,
        city=city
    )

    if address_uuid:
        os2mo.add_address_type(
            organisation_unit=identifier,
            value=address_uuid,
            type_ref="AddressMailUnit",
            date_from=date_from
        )

def import_employee(data_as_dict, main_org):
    """

    :param data_as_list:
    :return:
    """

    if not isinstance(data_as_dict, dict):
        raise TypeError("Cannot import employee, type error")

    if data_as_dict.get("@action"):
        return

    if data_as_dict.get("function"):
        print("================ HAS FUNCTION ===============")
        function_output = json.dumps(data_as_dict, indent=2)

        print(function_output)


    cpr_data = data_as_dict.get("cpr")

    cpr_no = cpr_data.get("#text")

    name = "{first} {last}".format(
        first=data_as_dict.get("firstName"),
        last=data_as_dict.get("lastName")
    )

    date_from = data_as_dict["entryDate"]

    os2mo.add_employee(
        identifier=name,
        cpr_no=cpr_no
    )

    contract = data_as_dict["workContractText"]
    job = data_as_dict["position"]

    orgUnit = data_as_dict["orgUnit"]

    os2mo.add_engagement(
        employee=name,
        organisation_unit=orgUnit,
        job_function_ref=job,
        engagement_type_ref=contract,
        date_from=date_from
    )


    address_string = item["address"]
    zip_code = item["postalCode"]
    city = item["city"]

    address_uuid = fuzzy_address(
        address_string=address_string,
        zip_code=zip_code,
        city=city
    )

    if address_uuid:
        os2mo.add_address_type(
            employee=name,
            value=address_uuid,
            type_ref="AddressMailUnit",
            date_from=date_from
        )


    

def import_klasse_types(data_as_list):
    """

    :param org_data_as_list:
    :return:
    """

    if not isinstance(data_as_list, list):
        raise TypeError("Cannot create jobs, wrong type")

    contract_types = [
        item.get("workContractText")
        for item in data_as_list
        if item.get("workContractText")
    ]

    job_titles = [
        item.get("position")
        for item in data_as_list
        if item.get("position")
    ]

    roles = [
        item.get("function")
        for item in data_as_list
        if item.get("function")
    ]

    contract_set = set(contract_types)
    job_title_set = set(job_titles)

    # Create engagement types
    for contract in contract_set:
        os2mo.add_klasse(
            identifier=contract,
            facet_type_ref="engagement_type",
            user_key=contract,
            title=contract
        )

    # Create job function types
    for job_title in job_title_set:
        os2mo.add_klasse(
            identifier=job_title,
            facet_type_ref="engagement_job_function",
            user_key=job_title,
            title=job_title
        )
    
    # Create role types
    roles = data.get("function")

    if not roles:
        return
    
    if isinstance(roles, list):
        for role in roles:
                create_role(roles)
    else:
        create_role(role)


def create_role(data_as_ordered_dict):
     """
    {
        "@artId": "1001",
        "@startDate": "2015-01-19",
        "@endDate": "9999-12-31",
        "orgDaekning": null,
        "artText": "Arbejdsmilj\u00f8repr\u00e6sentant",
        "members": "0000",
        "roleId": "1002",
        "roleText": "Medlem"
    },
    :param data_as_dict:
    :return:
    """

    role = {
        key: val
        for key, val in data_as_ordered_dict.items()
    }

    role_name = role["artText"]

    os2mo.add_klasse(
        identifier=role_name,
        facet_type_ref="role_type",
        title=role_name
    )


def parser(target_file):
    """
    Parse XML data and covert into usable dictionaries

    :return:
    """

    discovered_organisations = []
    discovered_org_units = []

    with open(target_file) as xmldump:
        data = xmltodict.parse(xmldump.read())

        # Target KMD data
        kmd_data = data["kmd"]

        # Create list of organisation units
        # (Converted into dictionaries)


        org_data = [
            convert_tuple_to_dict(data_item)
            for data_item in kmd_data["orgUnit"]
        ]

        employee_data = [
            convert_tuple_to_dict(data_item)
            for data_item in kmd_data["employee"]
        ]

        return {
            "org": org_data,
            "employee": employee_data
        }


def run_import():

    # PASS ENV VARIABLE TO SPECIFY XML FILE
    xml_file = os.getenv("OPUS_XML_DUMP")

    if not xml_file:
        print("NO FILE TO PARSE")
        print("PLEASE SET ENV VAR")
        return

    parsed = parser(xml_file)

    organisation_data = parsed["org"]
    employee_data = parsed["employee"]

    os2mo.add_organisation(
        identifier="Næstved Kommune",
        user_key="Næstved",
        municipality_code=370
    )

    # Create defaults
    create_default_klasse_types()

    for org in organisation_data:
        if not org["parentOrgUnit"]:
            continue

        import_org_unit(org, "Næstved Kommune")

    # Create klasse types
    import_klasse_types(employee_data)

    for emp in employee_data:
        if isinstance(emp, OrderedDict):
            continue

        import_employees(emp, "Næstved Kommune")


if __name__ == "__main__":
    start_logging()
    run_import()