#!/usr/bin/env python3
#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#   
import os
import pickle
import requests
import xmltodict
from anytree import Node
from os2mo_data_import import ImportHelper
# from os2mo_data_import import Organisation, ImportUtility


MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'SD-Løn Import')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_NAME', 0)
GLOBAL_DATE = os.environ.get('GLOBAL_DATE', '1977-01-01')
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')


def _dawa_request(address, adgangsadresse=False,
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

    street_name = address['StandardAddressIdentifier']
    last_is_letter = (street_name[-1].isalpha() and
                      (not street_name[-2].isalpha()))
    if (skip_letters and last_is_letter):
        street_name = address['StandardAddressIdentifier'][:-1]
    full_url = base + params.format(address['PostalCode'], street_name)

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


class SdImport(object):
    def __init__(self, importer, org_name, municipality_code):
        self.double_employment = []
        self.address_errors = {}

        self.importer = importer
        self.importer.add_organisation(
            identifier=org_name,
            user_key=org_name,
            municipality_code=municipality_code
        )

        self.nodes = {}  # Will be populated when org-tree is created
        self.add_people()
        self.info = self._read_department_info()
        for level in [(1040, 'Leder'), (1035, 'Chef'), (1030, 'Direktør')]:
            self._add_klasse(level[0], level[1], 'Lederniveau')
        for adresse_type in ['Email', 'Pnummer']:
            self._add_klasse(adresse_type, adresse_type, 'Adressetype')
        self._add_klasse('Lederansvar', 'Lederansvar', 'Lederansvar')
        self._add_klasse('non-primary',
                         'Ikke-primær ansættelse',
                         'Engagementstype')

    def _sd_lookup(self, filename):
        with open(filename, 'r') as f:
            data = f.read()
        xml_response = xmltodict.parse(data)
        outer_key = list(xml_response.keys())[0]
        return xml_response[outer_key]

    def _read_department_info(self):
        """ Load all deparment details and store for later user """
        department_info = {}

        departments = self._sd_lookup('GetDepartment20111201.xml')
        for department in departments['Department']:
            uuid = department['DepartmentUUIDIdentifier']
            department_info[uuid] = department
            unit_type = department['DepartmentLevelIdentifier']
            # if not self.org.Klasse.check_if_exists(unit_type):
            if True:
                self.importer.add_klasse(identifier=unit_type,
                                         facet_type_ref='Enhedstype',
                                         user_key=unit_type,
                                         title=unit_type)
        return department_info

    def _add_klasse(self, klasse_id, klasse, facet):
        #if not self.org.Klasse.check_if_exists(klasse_id):
        if True:
            self.importer.add_klasse(identifier=klasse_id,
                                     facet_type_ref=facet,
                                     user_key=klasse,
                                     scope='TEXT',
                                     title=klasse)

    def _dawa_lookup(self, address):
        """ Lookup an APOS address object in DAWA and find a UUID
        for the address.
        :param address: APOS address object
        :return: DAWA UUID for the address, or None if it is not found
        """
        dar_uuid = None
        dar_data = _dawa_request(address)
        if len(dar_data) == 0:
            # Found no hits, first attempt is to remove the letter
            # from the address
            dar_data = _dawa_request(address, skip_letters=True,
                                     adgangsadresse=True)
            if len(dar_data) == 1:
                dar_uuid = dar_data[0]['id']
        elif len(dar_data) == 1:
            dar_uuid = dar_data[0]['id']
        else:
            # Multiple results typically means we have found an
            # adgangsadresse
            dar_data = _dawa_request(address, adgangsadresse=True)
            if len(dar_data) == 1:
                dar_uuid = dar_data[0]['id']
        return dar_uuid

    def _add_sd_department(self, department, contains_subunits=False):
        """
        Add add a deparment to MO. If the unit has parents, these will
        also be added
        :param department: The SD-department, including parent units.
        :param contains_subunits: True if the unit has sub-units.
        """
        ou_level = department['DepartmentLevelIdentifier']
        unit_id = department['DepartmentUUIDIdentifier']
        user_key = department['DepartmentIdentifier']
        parent_uuid = None
        if 'DepartmentReference' in department:
            parent_uuid = (department['DepartmentReference']
                           ['DepartmentUUIDIdentifier'])

        info = self.info[unit_id]
        assert(info['DepartmentLevelIdentifier'] == ou_level)

        if not contains_subunits and parent_uuid is None:
            parent_uuid = 'OrphanUnits'

        # date_from = info['ActivationDate']
        # Activation dates are not consistent
        date_from = GLOBAL_DATE
        # No units have termination dates: date_to is None
        if not self.org.OrganisationUnit.check_if_exists(unit_id):
            self.org.OrganisationUnit.add(
                identifier=unit_id,
                name=info['DepartmentName'],
                user_key=user_key,
                org_unit_type_ref=ou_level,
                date_from=date_from,
                uuid=unit_id,
                date_to=None,
                parent_ref=parent_uuid)

        if 'ContactInformation' in info:
            emails = info['ContactInformation']['EmailAddressIdentifier']
            for email in emails:
                if email.find('Empty') == -1:
                    self.org.OrganisationUnit.add_type_address(
                        owner_ref=unit_id,
                        address_type_ref='Email',
                        value=email,
                        date_from=date_from
                    )
            if 'TelephoneNumberIdentifier' in info['ContactInformation']:
                # We only a sinlge phnone number, this is most likely
                # no a real number
                pass

        if 'ProductionUnitIdentifier' in info:
            self.org.OrganisationUnit.add_type_address(
                owner_ref=unit_id,
                address_type_ref='Pnummer',
                value=info['ProductionUnitIdentifier'],
                date_from=date_from
            )

        if 'PostalAddress' in info:
            needed = ['StandardAddressIdentifier', 'PostalCode']
            if all(element in info['PostalAddress'] for element in needed):
                dar_uuid = self._dawa_lookup(info['PostalAddress'])
                if dar_uuid is not None:
                    self.org.OrganisationUnit.add_type_address(
                        owner_ref=unit_id,
                        address_type_ref='AdressePost',
                        uuid=dar_uuid,
                        date_from=date_from
                    )
                else:
                    self.address_errors[unit_id] = info

        # Include higher level OUs, these do not have their own entry in SD
        if 'DepartmentReference' in department:
            self._add_sd_department(department['DepartmentReference'],
                                    contains_subunits=True)

    def _create_org_tree_structure(self):
        nodes = {}
        all_ous = self.org.OrganisationUnit.export()
        new_ous = []
        for ou in all_ous:
            parent = ou[1]['parent_ref']
            if parent is None:
                uuid = ou[0]
                for field in ou[1]['data']:
                    if field[0] == 'org_unit_type':
                        niveau = field[1]
                nodes[uuid] = Node(niveau, uuid=uuid)
            else:
                new_ous.append(ou)
        while len(new_ous) > 0:
            all_ous = new_ous
            new_ous = []
            for ou in all_ous:
                parent = ou[1]['parent_ref']
                if parent in nodes.keys():
                    uuid = ou[0]
                    for field in ou[1]['data']:
                        if field[0] == 'org_unit_type':
                            niveau = field[1]
                    nodes[uuid] = Node(niveau, parent=nodes[parent], uuid=uuid)
                else:
                    new_ous.append(ou)
        return nodes

    def add_people(self):
        """ Load all person details and store for later user """
        people = self._sd_lookup('GetPerson20111201.xml')
        for person in people['Person']:
            cpr = person['PersonCivilRegistrationIdentifier']
            name = (person['PersonGivenName'] + ' ' +
                    person['PersonSurnameName'])
            self.importer.add_employee(name=name,
                                       identifier=cpr,
                                       cpr_no=cpr,
                                       user_key=name)

    def create_ou_tree(self):
        """ Read all department levels from SD """
        self.org.OrganisationUnit.add(
                identifier='OrphanUnits',
                name='Forældreløse enheder',
                user_key='OrphanUnits',
                org_unit_type_ref='Enhed',
                date_from='1900-01-01',
                date_to=None,
                parent_ref=None)

        organisation = sd._sd_lookup('GetOrganization20111201.xml')
        departments = organisation['Organization']['DepartmentReference']
        for department in departments:
            self._add_sd_department(department)
        self.nodes = self._create_org_tree_structure()

    def create_employees(self):
        persons = sd._sd_lookup('GetEmployment20111201.xml')
        for person in persons['Person']:
            cpr = person['PersonCivilRegistrationIdentifier']
            employments = person['Employment']
            if not isinstance(employments, list):
                employments = [employments]

            max_rate = 0
            min_id = 999999
            for employment in employments:
                status = employment['EmploymentStatus']['EmploymentStatusCode']
                if (int(status) == 0):
                    continue
                employment_id = int(employment['EmploymentIdentifier'])
                occupation_rate = float(employment['WorkingTime']
                                        ['OccupationRate'])
                if occupation_rate == 0:
                    continue

                if occupation_rate == max_rate:
                    if employment_id < min_id:
                        min_id = employment_id
                if occupation_rate > max_rate:
                    max_rate = occupation_rate
                    min_id = employment_id

            exactly_one_primary = False
            for employment in employments:
                status = employment['EmploymentStatus']['EmploymentStatusCode']
                if int(status) == 0:
                    continue
                occupation_rate = float(employment['WorkingTime']
                                        ['OccupationRate'])
                employment_id = int(employment['EmploymentIdentifier'])

                if occupation_rate == max_rate and employment_id == min_id:
                    assert(exactly_one_primary is False)
                    engagement_type_ref = 'Ansat'
                    exactly_one_primary = True
                else:
                    engagement_type_ref = 'non-primary'

                job_id = int(employment['Profession']['JobPositionIdentifier'])
                job_func = employment['Profession']['EmploymentName']
                self._add_klasse(job_func, job_func, 'Stillingsbetegnelse')

                emp_dep = employment['EmploymentDepartment']
                unit = emp_dep['DepartmentUUIDIdentifier']

                date_from = emp_dep['ActivationDate']
                date_to = emp_dep['DeactivationDate']
                if date_to == '9999-12-31':
                    date_to = None

                # Employees are not allowed to be in these units (allthough
                # we do make an association). We must instead find the lowerst
                # higher level to put she or he.
                too_deep = ['Afdelings-niveau', 'NY1-niveau', 'NY2-niveau']
                original_unit = unit
                while self.nodes[unit].name in too_deep:
                    unit = self.nodes[unit].parent.uuid
                try:
                    self.org.Employee.add_type_engagement(
                        owner_ref=cpr,
                        org_unit_ref=unit,
                        job_function_ref=job_func,
                        engagement_type_ref=engagement_type_ref,
                        date_from=date_from,
                        date_to=date_to
                    )
                    # Remove this to remove any sign of the employee from the
                    # lowest levels of the org
                    self.org.Employee.add_type_association(
                        owner_ref=cpr,
                        org_unit_ref=original_unit,
                        job_function_ref=job_func,
                        association_type_ref=engagement_type_ref,
                        date_from=date_from
                    )

                except AssertionError:
                    self.double_employment.append(cpr)
                if job_id in [1040, 1035, 1030]:
                    manager_type_ref = 'manager_type_' + job_func
                    self._add_klasse(manager_type_ref,
                                     job_func,
                                     'Ledertyper')

                    self.org.Employee.add_type_manager(
                        owner_ref=cpr,
                        org_unit_ref=unit,
                        manager_level_ref=job_id,
                        address_uuid=None,  # TODO?
                        manager_type_ref=manager_type_ref,
                        responsibility_list=['Lederansvar'],
                        date_from=date_from,
                        date_to=date_to
                    )
            # This assertment really should hold...
            # assert(exactly_one_primary is True)
            if exactly_one_primary is not True:
                print()
                print(employments)


if __name__ == '__main__':

    importer = ImportHelper(create_defaults=True,
                            mox_base=MOX_BASE,
                            mora_base=MORA_BASE,
                            system_name='SD-Import',
                            end_marker='SDSTOP',
                            store_integration_data=True)
    
    sd = SdImport(importer, MUNICIPALTY_NAME, MUNICIPALTY_CODE)
    sd.create_ou_tree()
    sd.create_employees()

    importer.import_all()

    for info in sd.address_errors.values():
        print(info['DepartmentName'])
        print(info['DepartmentIdentifier'])
        print(info['PostalAddress']['StandardAddressIdentifier'])
        print(info['PostalAddress']['PostalCode'] + ' ' +
              info['PostalAddress']['DistrictName'])
        print()
        print()
    print(len(sd.address_errors))
