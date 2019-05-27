# -- coding: utf-8 --
import xmltodict
import dawa_helper


def _parse_phone(phone_number):
    validated_phone = None
    if len(phone_number) == 8:
        validated_phone = phone_number
    elif len(phone_number) in (9, 11):
        validated_phone = phone_number.replace(' ', '')
    elif len(phone_number) in (4, 5):
        validated_phone = '0000' + phone_number.replace(' ', '')
    return validated_phone


class OpusImport(object):

    def __init__(self, importer, org_name, xml_data):
        self.importer = importer

        self.organisation_id = None
        self.units = None
        self.employees = None
        # Update the above values
        municipality_code = self.parser(xml_data)

        self.importer.add_organisation(
            identifier=org_name,
            user_key=org_name,
            municipality_code=municipality_code
        )

        importer.new_itsystem(
            identifier='Opus',
            system_name='Opus'
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

    def insert_org_units(self):
        for unit in self.units:
            self._import_org_unit(unit)

    def insert_employees(self):
        for employee in self.employees:
            self._import_employee(employee)

    def _add_klasse(self, klasse_id, klasse, facet, scope='TEXT'):
        if not self.importer.check_if_exists('klasse', klasse_id):
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
        # costCenter, @lastChanged

        try:
            org_type = unit['orgType']
            self._add_klasse(org_type, unit['orgTypeTxt'], 'org_unit_type')
        except KeyError:
            org_type = 'Enhed'
            self._add_klasse(org_type, 'Enhed', 'org_unit_type')

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

        if 'seNr' in unit:
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['seNr'],
                type_ref='SE',
                date_from=date_from,
                date_to=date_to
            )

        if 'cvrNr' in unit:
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['cvrNr'],
                type_ref='CVR',
                date_from=date_from,
                date_to=date_to
            )

        if 'eanNr' in unit and (not unit['eanNr'] == '9999999999999'):
            self.importer.add_address_type(
                organisation_unit=identifier,
                value=unit['eanNr'],
                type_ref='EAN',
                date_from=date_from,
                date_to=date_to
            )

        if 'pNr' in unit and (not unit['pNr'] == '0000000000'):
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

        address_string = unit['street']
        zip_code = unit['zipCode']
        if address_string and zip_code:
            address_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
            if address_uuid:
                self.importer.add_address_type(
                    organisation_unit=identifier,
                    value=address_uuid,
                    type_ref='AddressPostUnit',
                    date_from=date_from,
                    date_to=date_to
                )

    def add_addresses_to_employees(self):
        for cpr, employee_addresses in self.employee_addresses.items():
            for facet, address in employee_addresses.items():
                print(address)
                if address:
                    self.importer.add_address_type(
                        employee=cpr,
                        value=address,
                        type_ref=facet,
                        date_from='1900-01-01',
                        date_to=None
                    )

    def _import_employee(self, employee):
        # UNUSED KEYS:
        # '@lastChanged'

        if 'cpr' in employee:
            cpr = employee['cpr']['#text']
        else:
            # Most likely this employee has left the organisation
            # Chek if the action key exists in current users
            # print(employee['@action'])
            return

        date_from = employee['entryDate']
        date_to = employee['leaveDate']

        # Only add employee and address information once, this info is duplicated
        # if the employee has multiple engagements
        if not self.importer.check_if_exists('employee', cpr):
            self.employee_addresses[cpr] = {}
            self.importer.add_employee(
                identifier=cpr,
                name=(employee['firstName'], employee['lastName']),
                cpr_no=cpr
            )

        if 'userId' in employee:
            self.importer.join_itsystem(
                employee=cpr,
                user_key=employee['userId'],
                itsystem_ref='Opus',
                date_from=date_from,
                date_to=date_to
            )

        if 'email' in employee:
            self.employee_addresses[cpr]['EmailEmployee'] = employee['email']
        if employee['workPhone'] is not None:
            phone = _parse_phone(employee['workPhone'])
            self.employee_addresses[cpr]['PhoneEmployee'] = phone

        if 'postalCode' in employee and employee['address']:
            if isinstance(employee['address'], dict):
                # TODO: This is a protected address
                # We currenly only support visibility for phones
                pass
            else:
                address_string = employee['address']
                zip_code = employee["postalCode"]
                addr_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
                if addr_uuid:
                    self.employee_addresses[cpr]['AdressePostEmployee'] = addr_uuid

        job = employee["position"]
        contract = employee['workContract']
        self._add_klasse(job, job, 'engagement_job_function')
        self._add_klasse(contract, employee["workContractText"], 'engagement_type')

        org_unit = employee['orgUnit']
        job_id = employee['@id']  # To be used soon

        self.importer.add_engagement(
            employee=cpr,
            organisation_unit=org_unit,
            user_key=job_id,
            job_function_ref=job,
            engagement_type_ref=contract,
            date_from=date_from,
            date_to=date_to
        )

        if employee['isManager'] == 'true':
            manager_type_ref = 'manager_type_' + job
            self._add_klasse(manager_type_ref, job, 'manager_type')

            # Opus has two levels of manager_level, since MO handles only one
            # they are concatenated into one.
            manager_level = '{}.{}'.format(employee['superiorLevel'],
                                           employee['subordinateLevel'])
            self._add_klasse(manager_level, manager_level, 'manager_level')

            self.importer.add_manager(
                employee=cpr,
                organisation_unit=org_unit,
                manager_level_ref=manager_level,
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
