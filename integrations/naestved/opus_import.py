# -- coding: utf-8 --
import pickle
import requests
import xmltodict
# from dawa import fuzzy_address

# from logger import start_logging


def _parse_phone(phone_number):
    validated_phone = None
    if len(phone_number) == 8:
        validated_phone = phone_number
    elif len(phone_number) in (9, 11):
        validated_phone = phone_number.replace(' ', '')
    elif len(phone_number) in (4, 5):
        validated_phone = '0000' + phone_number.replace(' ', '')
    return validated_phone


# This code should be shared with apos and sd-import
def _dawa_request(street_name, postal_code, adgangsadresse=False,
                  skip_letters=False, add_letter=False):
    """ Perform a request to DAWA and return the json object
    :param address: An address object as returned by APOS
    :param adgangsadresse: If true, search for adgangsadresser
    :param skip_letters: If true, remove letters from the house number
    :return: The DAWA json object as a dictionary
    """
    if adgangsadresse:
        base = 'https://dawa.aws.dk/adgangsadresser?'
    else:
        base = 'https://dawa.aws.dk/adresser?strukur=mini'
    params = '&postnr={}&q={}'

    last_is_letter = (street_name[-1].isalpha() and
                      (not street_name[-2].isalpha()))
    if (skip_letters and last_is_letter):
        street_name = street_name[:-1]
    full_url = base + params.format(postal_code, street_name)
    path_url = full_url.replace('/', '_')

    try:
        with open(path_url + '.p', 'rb') as f:
            response = pickle.load(f)
    except FileNotFoundError:
        response = requests.get(full_url)
        with open(path_url + '.p', 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

    dar_data = response.json()
    return dar_data


class OpusImport(object):

    def __init__(self, importer, org_name, xml_data):
        self.importer = importer

        self.organisation_id = None
        self.units = None
        self.employees = None
        # Update values
        municipality_code = self.parser(xml_data)
        # We should also be able to take the name from here

        self.importer.add_organisation(
            identifier=org_name,
            user_key=org_name,
            municipality_code=municipality_code
        )

        self.employee_addresses = {}
        self._add_klasse('AddressPostUnit', 'Postadresse',
                         'org_unit_address_type', 'DAR')
        self._add_klasse('Pnummer', 'Pnummer',
                         'org_unit_address_type', 'PNUMBER')
        self._add_klasse('EAN', 'EAN', 'org_unit_address_type', 'EAN')
        self._add_klasse('PhoneUnit', 'Telefon', 'org_unit_address_type', 'PHONE')
        self._add_klasse('PhoneEmployee', 'Telefon', 'employee_address_type',
                         'PHONE')
        self._add_klasse('EmailEmployee', 'Email',
                         'employee_address_type', 'EMAIL')
        self._add_klasse('CVR', 'CVR', 'org_unit_address_type')
        self._add_klasse('SE', 'SE', 'org_unit_address_type')
        self._add_klasse('AdressePostEmployee', 'Postadresse',
                         'employee_address_type', 'DAR')
        self._add_klasse('Lederansvar', 'Lederansvar', 'responsibility')

    # This code should be shared with apos and sd-import
    def _dawa_lookup(self, street_name, postal_code):
        """ Lookup an APOS address object in DAWA and find a UUID
        for the address.
        :param address: APOS address object
        :return: DAWA UUID for the address, or None if it is not found
        """
        dar_uuid = None
        dar_data = _dawa_request(street_name, postal_code)
        print(len(dar_data))
        if len(dar_data) == 0:
            # Found no hits, first attempt is to remove the letter
            # from the address
            dar_data = _dawa_request(street_name, postal_code, skip_letters=True,
                                     adgangsadresse=True)
            if len(dar_data) == 1:
                dar_uuid = dar_data[0]['id']
        elif len(dar_data) == 1:
            dar_uuid = dar_data[0]['id']
        else:
            # Multiple results typically means we have found an
            # adgangsadresse
            dar_data = _dawa_request(street_name, postal_code, adgangsadresse=True)
            if len(dar_data) == 1:
                dar_uuid = dar_data[0]['id']
        return dar_uuid

    def insert_org_units(self):
        for unit in self.units:
            self._import_org_unit(unit)

    def insert_employees(self):
        for employee in self.employees:
            self._import_employee(employee)

    def _add_klasse(self, klasse_id, klasse, facet, scope='TEXT'):
        if not self.importer.check_if_exists('klasse', klasse_id):
            # print(klasse_id)
            self.importer.add_klasse(identifier=klasse_id,
                                     facet_type_ref=facet,
                                     user_key=klasse,
                                     scope=scope,
                                     title=klasse)
        return klasse_id

    def parser(self, target_file):
        """
        Parse XML data and covert into usable dictionaries

        :return:
        """

        with open(target_file) as xmldump:
            data = xmltodict.parse(xmldump.read())['kmd']

        self.organisation_id = data['orgUnit'][0]['@id']

        self.units = data['orgUnit'][1:]
        self.employees = data['employee']
        municipality_code = int(data['orgUnit'][0]['@client'])
        return municipality_code

    def _import_org_unit(self, unit):
        # UNUSED KEYS:
        # costCenter, zipCode, city, @lastChanged, street

        org_type = unit['orgType']
        self._add_klasse(org_type, unit['orgTypeTxt'], 'org_unit_type')

        identifier = unit['@id']
        user_key = unit['shortName']
        date_from = unit['startDate']
        if unit['endDate'] == '9999-12-31':
            date_to = None
        else:
            date_to = unit['endDate']
        name = unit['longName']

        parent_org = unit.get("parentOrgUnit")
        if parent_org == self.organisation_id:
            parent_org = None

        self.importer.add_organisation_unit(
            identifier=identifier,
            name=name,
            user_key=user_key,
            parent_ref=parent_org,
            type_ref=org_type,
            date_from=date_from,
            date_to=date_to
        )

        self.importer.add_address_type(
            organisation_unit=identifier,
            value=unit['seNr'],
            type_ref='SE',
            date_from=date_from,
            date_to=date_to
        )

        self.importer.add_address_type(
            organisation_unit=identifier,
            value=unit['cvrNr'],
            type_ref='CVR',
            date_from=date_from,
            date_to=date_to
        )

        if not unit['eanNr'] == '9999999999999':
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['eanNr'],
                type_ref='EAN',
                date_from=date_from,
                date_to=date_to
            )

        if not unit['pNr'] == '0000000000':
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['pNr'],
                type_ref='Pnummer',
                date_from=date_from,
                date_to=date_to
            )

        if unit['phoneNumber']:
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['phoneNumber'],
                type_ref='PhoneUnit',
                date_from=date_from,
                date_to=date_to
            )

        """
        address_string = item["street"]
        zip_code = item["zipCode"]
        city = item["city"]
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
        """

    def add_addresses_to_employees(self):
        for cpr, employee_addresses in self.employee_addresses.items():
            for facet, address in employee_addresses.items():
                self.importer.add_address_type(
                    employee=cpr,
                    value=address,
                    type_ref=facet,
                    date_from='1900-01-01',
                    date_to=None
                )

    def _import_employee(self, employee):
        # UNUSED KEYS:
        # 'city', 'subordinateLevel', 'postalCode', '@lastChanged',
        # 'userId', 'country', 'address'

        # if data_as_dict.get("@action"):
        #    return

        if 'userId' in employee:
            # What is this?
            # print(employee['userId'])
            pass

        if 'cpr' in employee:
            # Field also contains key @suppId - what is this?
            cpr = employee['cpr']['#text']
        else:
            # Most likely this employee has left the organisation
            # Chek if the action key exists in current users
            # print(employee['@action'])
            return

        name = "{first} {last}".format(
            first=employee['firstName'],
            last=employee['lastName']
        )

        date_from = employee['entryDate']
        date_to = employee['leaveDate']

        # Only add employee and address information once, this info is duplicated
        # if the employee has multiple engagements
        if not self.importer.check_if_exists('employee', cpr):
            self.employee_addresses[cpr] = {}
            self.importer.add_employee(
                identifier=cpr,
                name=name,
                cpr_no=cpr
            )

        if 'email' in employee:
            self.employee_addresses[cpr]['EmailEmployee'] = employee['email']
        if employee['workPhone'] is not None:
            phone = _parse_phone(employee['workPhone'])
            self.employee_addresses[cpr]['PhoneEmployee'] = phone

        job = employee["position"]
        contract = employee['workContract']
        self._add_klasse(job, job, 'engagement_job_function')
        self._add_klasse(contract, employee["workContractText"], 'engagement_type')

        org_unit = employee['orgUnit']
        job_id = employee['@id']  # To be used soon

        self.importer.add_engagement(
            employee=cpr,
            organisation_unit=org_unit,
            # user_key=job_id, # Will be added soon!!!
            job_function_ref=job,
            engagement_type_ref=contract,
            date_from=date_from,
            date_to=date_to
        )

        if employee['isManager'] == 'true':
            manager_type_ref = 'manager_type_' + job
            self._add_klasse(manager_type_ref, job, 'manager_type')

            self._add_klasse(employee['superiorLevel'],
                             employee['superiorLevel'],
                             'manager_level')

            self.importer.add_manager(
                employee=cpr,
                organisation_unit=org_unit,
                manager_level_ref=employee['superiorLevel'],
                manager_type_ref=manager_type_ref,
                responsibility_list=['Lederansvar'],
                date_from=date_from,
                date_to=date_to
            )

        if 'function' in employee:
            if not isinstance(employee['function'], list):
                roles = [employee['function']]
            else:
                roles = employee['function']

            for role in roles:
                # We have only a single class for roles, must combine the information
                if 'roleText' in role:
                    combined_role = '{} - {}'.format(role['artText'],
                                                     role['roleText'])
                else:
                    combined_role = role['artText']
                self._add_klasse(combined_role, combined_role, 'role_type')

                date_from = role['@startDate']
                if role['@endDate'] == '9999-12-31':
                    date_to = None
                else:
                    date_to = role['@endDate']

                self.importer.add_role(
                    employee=cpr,
                    organisation_unit=org_unit,
                    role_type_ref=combined_role,
                    date_from=date_from,
                    date_to=date_to
                )

        if 'postalCode' in employee:
            address_string = employee["address"]
            zip_code = employee["postalCode"]
            # city = employee["city"]

            print(self._dawa_lookup(address_string, zip_code))
            print()

        """
        if address_uuid:
            os2mo.add_address_type(
                employee=name,
                value=address_uuid,
                type_ref='AdressePostEmployee',
                date_from=date_from
            )
        """
