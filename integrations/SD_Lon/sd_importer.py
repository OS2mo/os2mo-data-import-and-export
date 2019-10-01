#!/usr/bin/env python3
#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import os
import sys
import uuid
import hashlib
import logging
import datetime
from anytree import Node

from sd_common import sd_lookup, calc_employment_id
sys.path.append('../ad_integration')
import ad_reader
sys.path.append('../')
import dawa_helper

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger('sdImport')

for name in logging.root.manager.loggerDict:
    if name in ('sdImport', 'sdCommon', 'AdReader', 'moImporterMoraTypes',
                'moImporterMoxTypes', 'moImporterUtilities', 'moImporterHelpers'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


class SdImport(object):
    def __init__(self, importer, org_name, municipality_code,
                 import_date_from, ad_info=None, org_only=False,
                 org_id_prefix=None, manager_rows=[], super_unit=None):
        self.base_url = 'https://service.sd.dk/sdws/'

        self.double_employment = []
        self.address_errors = {}
        self.manager_rows = manager_rows

        self.importer = importer
        self.org_name = org_name
        self.importer.add_organisation(
            identifier=org_name,
            user_key=org_name,
            municipality_code=municipality_code
        )

        self.org_id_prefix = org_id_prefix
        self.import_date = import_date_from.strftime('%d.%m.%Y')
        # self.import_date_from = import_date_from.strftime('%d.%m.%Y')

        # If a list of hard-coded uuids from AD is provided, use this. If
        # a true AD-reader is provided, save it so we can use it to
        # get all the info we need
        self.ad_people = {}
        self.ad_reader = None
        self.employee_forced_uuids = None
        if isinstance(ad_info, dict):
            self.employee_forced_uuids = ad_info
        if isinstance(ad_info, ad_reader.ADParameterReader):
            self.ad_reader = ad_info
            self.importer.new_itsystem(
                identifier='AD',
                system_name='Active Directory'
            )
            self.ad_reader.cache_all()

        self.nodes = {}  # Will be populated when org-tree is created

        self.org_only = org_only
        if not org_only:
            self.add_people()

        self.info = self._read_department_info()
        for level in [(1040, 'Leder'), (1035, 'Chef'), (1030, 'Direktør')]:
            self._add_klasse(level[0], level[1], 'manager_level')

        if self.manager_rows is None:
            self._add_klasse('Lederansvar', 'Lederansvar', 'responsibility')
        else:
            for row in self.manager_rows:
                resp = row.get('ansvar')
                self._add_klasse(resp, resp, 'responsibility')

        self._add_klasse('leder_type', 'Leder', 'manager_type')

        self._add_klasse('Pnummer', 'Pnummer',
                         'org_unit_address_type', 'PNUMBER')
        self._add_klasse('AddressMailUnit', 'Postdresse',
                         'org_unit_address_type', 'DAR')
        self._add_klasse('AdresseReturUnit', 'Returadresse',
                         'org_unit_address_type', 'DAR')
        self._add_klasse('AdresseHenvendelseUnit', 'Henvendelsessted',
                         'org_unit_address_type', 'DAR')
        self._add_klasse('PhoneUnit', 'Telefon',
                         'org_unit_address_type', 'PHONE')
        self._add_klasse('EmailUnit', 'Email',
                         'org_unit_address_type', 'EMAIL')

        self._add_klasse('AdressePostEmployee', 'Postadresse',
                         'employee_address_type', 'DAR')
        self._add_klasse('PhoneEmployee', 'Telefon',
                         'employee_address_type', 'PHONE')
        self._add_klasse('MobilePhoneEmployee', 'Mobiltelefon',
                         'employee_address_type', 'PHONE')
        self._add_klasse('LocationEmployee', 'Lokation',
                         'employee_address_type', 'TEXT')
        self._add_klasse('EmailEmployee', 'Email',
                         'employee_address_type', 'EMAIL')
        self._add_klasse('Orlov', 'Orlov', 'leave_type')

        self._add_klasse('Ansat', 'Ansat', 'engagement_type')
        self._add_klasse('status0', 'Ansat - Ikke i løn', 'engagement_type')
        self._add_klasse('non-primary', 'Ikke-primær ansættelse', 'engagement_type')
        self._add_klasse('explicitly-primary', 'Manuelt primær ansættelse', 'engagement_type')

        self._add_klasse('SD-medarbejder', 'SD-medarbejder', 'association_type')

        self._add_klasse('Ekstern', 'Må vises eksternt', 'visibility', 'PUBLIC')
        self._add_klasse('Intern', 'Må vises internt', 'visibility', 'INTERNAL')
        self._add_klasse('Hemmelig', 'Hemmelig', 'visibility', 'SECRET')

    def _generate_uuid(self, value):
        """
        Code almost identical to this also lives in the Opus importer.
        """
        if self.org_id_prefix:
            base_hash = hashlib.md5(self.org_id_prefix.encode())
        else:
            base_hash = hashlib.md5(self.org_name.encode())

        base_digest = base_hash.hexdigest()
        base_uuid = uuid.UUID(base_digest)

        combined_value = (str(base_uuid) + str(value)).encode()
        value_hash = hashlib.md5(combined_value)
        value_digest = value_hash.hexdigest()
        value_uuid = str(uuid.UUID(value_digest))
        return value_uuid

    def _update_ad_map(self, cpr):
        logger.debug('Update cpr{}'.format(cpr))
        self.ad_people[cpr] = {}
        if self.ad_reader:
            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            if response:
                self.ad_people[cpr] = response

    def _read_department_info(self):
        """ Load all deparment details and store for later user """
        department_info = {}

        params = {
            'ActivationDate': self.import_date,
            'DeactivationDate': self.import_date,
            'ContactInformationIndicator': 'true',
            'DepartmentNameIndicator': 'true',
            'PostalAddressIndicator': 'true',
            'ProductionUnitIndicator': 'true',
            'UUIDIndicator': 'true',
            'EmploymentDepartmentIndicator': 'false'
        }
        departments = sd_lookup('GetDepartment20111201', params)

        for department in departments['Department']:
            uuid = department['DepartmentUUIDIdentifier']
            if self.org_id_prefix:
                uuid = self._generate_uuid(uuid)

            department_info[uuid] = department
            unit_type = department['DepartmentLevelIdentifier']
            if not self.importer.check_if_exists('klasse', unit_type):
                self._add_klasse(unit_type, unit_type,
                                 'org_unit_type', scope='TEXT')
        return department_info

    def _add_klasse(self, klasse_id, klasse, facet, scope='TEXT'):
        if isinstance(klasse_id, str):
            klasse_id = klasse_id.replace('&', '_')
        if not self.importer.check_if_exists('klasse', klasse_id):
            klasse_uuid = self._generate_uuid(klasse_id)
            self.importer.add_klasse(identifier=klasse_id,
                                     uuid=klasse_uuid,
                                     facet_type_ref=facet,
                                     user_key=str(klasse_id),
                                     scope=scope,
                                     title=klasse)
        return klasse_id

    def _check_subtree(self, department, sub_tree):
        """ Check if a department is member of given sub-tree """
        in_sub_tree = False
        while 'DepartmentReference' in department:
            department = department['DepartmentReference']
            dep_uuid = department['DepartmentUUIDIdentifier']
            if self.org_id_prefix:
                dep_uuid = self._generate_uuid(dep_uuid)
            if dep_uuid == sub_tree:
                in_sub_tree = True
        return in_sub_tree

    def _add_sd_department(self, department, contains_subunits=False,
                           sub_tree=None, super_unit=None):
        """
        Add add a deparment to MO. If the unit has parents, these will
        also be added
        :param department: The SD-department, including parent units.
        :param contains_subunits: True if the unit has sub-units.
        """
        ou_level = department['DepartmentLevelIdentifier']
        if not self.org_id_prefix:
            unit_id = department['DepartmentUUIDIdentifier']
            user_key = department['DepartmentIdentifier']
        else:
            unit_id = self._generate_uuid(department['DepartmentUUIDIdentifier'])
            user_key = self.org_id_prefix + '_' + department['DepartmentIdentifier']

        # parent_uuid = None
        parent_uuid = super_unit

        # If contain_subunits is true, this sub tree is a valid member
        import_unit = contains_subunits
        if 'DepartmentReference' in department:
            if self._check_subtree(department, sub_tree):
                import_unit = True

            parent_uuid = department['DepartmentReference']['DepartmentUUIDIdentifier']
            if self.org_id_prefix:
                parent_uuid = self._generate_uuid(parent_uuid)
        else:
            import_unit = unit_id == sub_tree

        if not import_unit and sub_tree is not None:
            return

        info = self.info[unit_id]
        assert(info['DepartmentLevelIdentifier'] == ou_level)
        logger.debug('Add unit: {}'.format(unit_id))
        if (
                (not contains_subunits) and
                (parent_uuid is super_unit) and
                self.importer.check_if_exists('organisation_unit', 'OrphanUnits')
        ):
            parent_uuid = 'OrphanUnits'

        date_from = info['ActivationDate']
        # No units have termination dates: date_to is None
        if self.importer.check_if_exists('organisation_unit', unit_id):
            return
        else:
            self.importer.add_organisation_unit(
                identifier=unit_id,
                name=info['DepartmentName'],
                user_key=user_key,
                type_ref=ou_level,
                date_from=date_from,
                uuid=unit_id,
                date_to=None,
                parent_ref=parent_uuid)

            for row in self.manager_rows:
                if row['afdeling'].upper() == user_key.upper():
                    row['uuid'] = unit_id

        if 'ContactInformation' in info:
            emails = info['ContactInformation']['EmailAddressIdentifier']
            for email in emails:
                if email.find('Empty') == -1:
                    self.importer.add_address_type(
                        organisation_unit=unit_id,
                        type_ref='EmailUnit',
                        value=email,
                        date_from=date_from
                    )
            if 'TelephoneNumberIdentifier' in info['ContactInformation']:
                # We only a sinlge phnone number, this is most likely
                # no a real number
                pass

        if 'ProductionUnitIdentifier' in info:
            self.importer.add_address_type(
                organisation_unit=unit_id,
                type_ref='Pnummer',
                value=info['ProductionUnitIdentifier'],
                date_from=date_from
            )

        if 'PostalAddress' in info:
            needed = ['StandardAddressIdentifier', 'PostalCode']
            if all(element in info['PostalAddress'] for element in needed):
                address_string = info['PostalAddress']['StandardAddressIdentifier']
                zip_code = info['PostalAddress']['PostalCode']
                logger.debug('Look in Dawa: {}'.format(address_string))
                dar_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
                logger.debug('DAR: {}'.format(dar_uuid))

                if dar_uuid is not None:
                    self.importer.add_address_type(
                        organisation_unit=unit_id,
                        type_ref='AddressMailUnit',
                        value=dar_uuid,
                        date_from=date_from
                    )
                else:
                    self.address_errors[unit_id] = info

        # Include higher level OUs, these do not have their own entry in SD
        if 'DepartmentReference' in department:
            self._add_sd_department(department['DepartmentReference'],
                                    contains_subunits=True,
                                    sub_tree=sub_tree,
                                    super_unit=super_unit)

    def _create_org_tree_structure(self):
        nodes = {}
        all_ous = self.importer.export('organisation_unit')
        new_ous = []
        for key, ou in all_ous.items():
            parent = ou.parent_ref
            if parent is None:
                uuid = key
                niveau = ou.type_ref
                nodes[uuid] = Node(niveau, uuid=uuid)
            else:
                new_ous.append(ou)

        while len(new_ous) > 0:
            logger.info('Number of new ous: {}'.format(len(new_ous)))
            print('Number of new ous: {}'.format(len(new_ous)))
            all_ous = new_ous
            new_ous = []
            for ou in all_ous:
                parent = ou.parent_ref
                if parent in nodes.keys():
                    uuid = ou.uuid
                    niveau = ou.type_ref
                    nodes[uuid] = Node(niveau, parent=nodes[parent], uuid=uuid)
                else:
                    new_ous.append(ou)
        return nodes

    def add_people(self):
        """ Load all person details and store for later user """
        params = {
            'StatusActiveIndicator': 'true',
            'StatusPassiveIndicator': 'true',
            'ContactInformationIndicator': 'false',
            'PostalAddressIndicator': 'false'
        }
        params['EffectiveDate'] = self.import_date
        people = sd_lookup('GetPerson20111201', params)

        for person in people['Person']:
            cpr = person['PersonCivilRegistrationIdentifier']
            logger.info('Importing {}'.format(cpr))
            if cpr[-4:] == '0000':
                logger.warning('Skipping fictional user: {}'.format(cpr))
                continue

            self._update_ad_map(cpr)

            given_name = person.get('PersonGivenName', '')
            sur_name = person.get('PersonSurnameName', '')

            if 'ObjectGuid' in self.ad_people[cpr]:
                uuid = self.ad_people[cpr]['ObjectGuid']
            elif self.employee_forced_uuids:  # Should be wrapped in update_ad_map
                uuid = self.employee_forced_uuids.get(cpr, None)
            else:
                uuid = None

            # Name is placeholder for initals, do not know which field to extract
            if 'Name' in self.ad_people[cpr]:
                user_key = self.ad_people[cpr]['Name']
            else:
                user_key = '{} {}'.format(given_name, sur_name)

            self.importer.add_employee(
                name=(given_name, sur_name),
                identifier=cpr,
                cpr_no=cpr,
                user_key=user_key,
                uuid=uuid
            )

            if 'SamAccountName' in self.ad_people[cpr]:
                self.importer.join_itsystem(
                    employee=cpr,
                    user_key=self.ad_people[cpr]['SamAccountName'],
                    itsystem_ref='AD',
                    date_from=None
                )

            # NOTICE: This will soon be removed and replaced with the
            # AD-sync functionality which will sync all and just a few
            # magic fields.
            phone = self.ad_people[cpr].get('MobilePhone')
            if phone:
                self.importer.add_address_type(
                    employee=cpr,
                    value=phone,
                    type_ref='PhoneEmployee',
                    date_from=None
                )

            email = self.ad_people[cpr].get('EmailAddress')
            if email:
                self.importer.add_address_type(
                    employee=cpr,
                    value=email,
                    type_ref='EmailEmployee',
                    date_from=None
                )

    def create_ou_tree(self, create_orphan_container, sub_tree=None,
                       super_unit=None):
        """ Read all department levels from SD """
        # TODO: Currently we can only read a top sub-tree
        if create_orphan_container:
            self._add_klasse('Orphan', 'Virtuel Enhed', 'org_unit_type')
            self.importer.add_organisation_unit(
                identifier='OrphanUnits',
                uuid='11111111-0000-0000-0000-111111111111',
                name='Forældreløse enheder',
                user_key='OrphanUnits',
                type_ref='Orphan',
                date_from='1900-01-01',
                date_to=None,
                parent_ref=None
            )
        params = {
            'ActivationDate': self.import_date,
            'DeactivationDate': self.import_date,
            'UUIDIndicator': 'true'
        }
        organisation = sd_lookup('GetOrganization20111201', params)
        departments = organisation['Organization']['DepartmentReference']

        for department in departments:
            self._add_sd_department(department, sub_tree=sub_tree,
                                    super_unit=super_unit)
        self.nodes = self._create_org_tree_structure()

    def create_employees(self):
        params = {
            'StatusActiveIndicator': 'true',
            'DepartmentIndicator': 'true',
            'EmploymentStatusIndicator': 'true',
            'ProfessionIndicator': 'true',
            'WorkingTimeIndicator': 'true',
            'UUIDIndicator': 'true',
            'StatusPassiveIndicator': 'true',
            'SalaryAgreementIndicator': 'false',
            'SalaryCodeGroupIndicator': 'false',
            'EffectiveDate': self.import_date
        }
        logger.info('Create emplyoees')
        persons = sd_lookup('GetEmployment20111201', params)
        self._create_employees(persons)

    def _create_employees(self, persons):
        for person in persons['Person']:
            cpr = person['PersonCivilRegistrationIdentifier']
            if cpr[-4:] == '0000':
                logger.warning('Skipping fictional user: {}'.format(cpr))
                continue

            employments = person['Employment']
            if not isinstance(employments, list):
                employments = [employments]

            max_rate = -1
            min_id = 999999
            for employment in employments:
                status = employment['EmploymentStatus']['EmploymentStatusCode']
                if status == '3':
                    # Orlov
                    pass
                if status in ('7', '8', '9'):
                    # Fratrådt eller pensioneret.
                    continue

                employment_id = calc_employment_id(employment)
                occupation_rate = float(employment['WorkingTime']
                                        ['OccupationRate'])

                # if occupation_rate == 0:
                # continue

                if occupation_rate == max_rate:
                    if employment_id['value'] < min_id:
                        min_id = employment_id['value']
                if occupation_rate > max_rate:
                    max_rate = occupation_rate
                    min_id = employment_id['value']

            exactly_one_primary = False
            for employment in employments:
                status = employment['EmploymentStatus']['EmploymentStatusCode']
                """
                if int(status) in (8, 9):
                    # Fratrådt
                    continue
                """

                # Find a valid job_function name, this might be overwritten from
                # AD, if an AD value is available for this employment
                job_id = int(employment['Profession']['JobPositionIdentifier'])
                try:
                    job_func = employment['Profession']['EmploymentName']
                except KeyError:
                    job_func = employment['Profession']['JobPositionIdentifier']

                occupation_rate = float(employment['WorkingTime']
                                        ['OccupationRate'])

                employment_id = calc_employment_id(employment)

                if occupation_rate == max_rate and employment_id['value'] == min_id:
                    assert(exactly_one_primary is False)
                    engagement_type_ref = 'Ansat'
                    exactly_one_primary = True

                    # TODO: This part of the AD integration most likely needs to go, it
                    # does not obey temporality.

                    # ad_titel = self.ad_people[cpr].get('Title', None)
                    # if ad_titel:  # Title exists in AD, this is primary engagement
                    #     job_func = ad_titel
                else:
                    engagement_type_ref = 'non-primary'

                if status == '0':  # If status 0, uncondtionally override
                    engagement_type_ref = 'status0'

                job_func_ref = self._add_klasse(job_func,
                                                job_func,
                                                'engagement_job_function')
                emp_dep = employment['EmploymentDepartment']
                unit = emp_dep['DepartmentUUIDIdentifier']

                # This can removed once we fully trust the new date logic
                # date_from = datetime.datetime.strptime(
                #     employment['EmploymentDate'],
                #     '%Y-%m-%d'
                # )

                if status in ('7', '8', '9'):
                    date_from = datetime.datetime.strptime(
                        employment['EmploymentDate'],
                        '%Y-%m-%d'
                    )
                    date_to = datetime.datetime.strptime(
                        employment['EmploymentStatus']['ActivationDate'],
                        '%Y-%m-%d'
                    )
                    date_to = date_to - datetime.timedelta(days=1)
                else:
                    date_from = datetime.datetime.strptime(
                        employment['EmploymentStatus']['ActivationDate'],
                        '%Y-%m-%d'
                    )
                    date_to = datetime.datetime.strptime(
                        employment['EmploymentStatus']['DeactivationDate'],
                        '%Y-%m-%d'
                    )

                if date_from > date_to:
                    date_from = date_to

                date_from = datetime.datetime.strftime(date_from, "%Y-%m-%d")
                if date_to == datetime.datetime(9999, 12, 31, 0, 0):
                    date_to = None
                else:
                    date_to = datetime.datetime.strftime(date_to, "%Y-%m-%d")

                # Employees are not allowed to be in these units (allthough
                # we do make an association). We must instead find the lowest
                # higher level to put she or he.
                too_deep = ['Afdelings-niveau', 'NY1-niveau']
                original_unit = unit
                while self.nodes[unit].name in too_deep:
                    unit = self.nodes[unit].parent.uuid
                try:
                    self.importer.add_engagement(
                        employee=cpr,
                        user_key=employment_id['id'],
                        organisation_unit=unit,
                        job_function_ref=job_func_ref,
                        fraction=int(occupation_rate * 1000000),
                        engagement_type_ref=engagement_type_ref,
                        date_from=date_from,
                        date_to=date_to
                    )
                    # Remove this to remove any sign of the employee from the
                    # lowest levels of the org
                    self.importer.add_association(
                        employee=cpr,
                        user_key=employment_id['id'],
                        organisation_unit=original_unit,
                        association_type_ref='SD-medarbejder',
                        date_from=date_from,
                        date_to=date_to
                    )
                    if status == '3':
                        self.importer.add_leave(
                            employee=cpr,
                            leave_type_ref='Orlov',
                            date_from=date_from,
                            date_to=date_to
                        )
                except AssertionError:
                    self.double_employment.append(cpr)

                # If we do not have a list of managers, we take the manager,
                # information fro the job_function_code.
                if not self.manager_rows:
                    # These job functions will normally (but necessarily)
                    #  correlate to a manager position
                    if job_id in [1040, 1035, 1030]:
                        self.importer.add_manager(
                            employee=cpr,
                            organisation_unit=unit,
                            manager_level_ref=job_id,
                            address_uuid=None,  # Manager address is not used
                            manager_type_ref='leder_type',
                            responsibility_list=['Lederansvar'],
                            date_from=date_from,
                            date_to=date_to
                        )
            if self.manager_rows:
                for row in self.manager_rows:
                    if row['cpr'] == cpr:
                        if 'uuid' not in row:
                            logger.warning('NO UNIT: {}'.format(row['afdeling']))
                            continue
                        if job_id in [1040, 1035, 1030]:
                            manager_level = job_id
                        else:
                            manager_level = 1040

                        logger.info(
                            'Manager {} to {}'.format(cpr, row['afdeling'])
                        )
                        self.importer.add_manager(
                            employee=cpr,
                            organisation_unit=row['uuid'],
                            manager_level_ref=manager_level,
                            manager_type_ref='leder_type',
                            responsibility_list=[row['ansvar']],
                            date_from='1900-01-01',
                            date_to=None
                        )

            # This assertment really should hold...
            # assert(exactly_one_primary is True)
            if exactly_one_primary is not True:
                pass
                # print()
                # print('More than one primary: {}'.format(employments))
