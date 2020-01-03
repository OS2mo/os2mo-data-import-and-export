import json
import pathlib
import logging
import requests
import datetime
import argparse
import sd_payloads

from os2mo_helpers.mora_helpers import MoraHelper
from integrations.SD_Lon.sd_common import sd_lookup
from integrations.SD_Lon.sd_common import mora_assert
from integrations.SD_Lon.exceptions import NoCurrentValdityException

# LOG_LEVEL = logging.DEBUG
# LOG_FILE = 'fix_sd_departments.log'

logger = logging.getLogger('fixDepartments')

# TODO!
# detail_logging = ('sdCommon', 'fixDepartments')
# for name in logging.root.manager.loggerDict:
#     if name in detail_logging:
#         logging.getLogger(name).setLevel(LOG_LEVEL)
#     else:
#         logging.getLogger(name).setLevel(logging.ERROR)

# logging.basicConfig(
#     format='%(levelname)s %(asctime)s %(name)s %(message)s',
#     level=LOG_LEVEL,
#     filename=LOG_FILE
# )


class FixDepartments(object):
    def __init__(self):
        logger.info('Start program')
        # TODO: Soon we have done this 4 times. Should we make a small settings
        # importer, that will also handle datatype for specicic keys?
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.institution_uuid = self.get_institution()
        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)

        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        logger.info('Read org_unit types')
        self.level_types = self.helper.read_classes_in_facet('org_unit_level')[0]
        unit_types = self.helper.read_classes_in_facet('org_unit_type')[0]

        # Currently only a single unit type exists, we will not do anything fancy
        # until it has been decided what the source for types should be.
        self.unit_type = None
        for unit in unit_types:
            if unit['user_key'] == 'Enhed':
                self.unit_type = unit

        if self.unit_type is None:
            raise Exception('Unit types not correctly configured')

    def get_institution(self):
        """
        Get the institution uuid of the current organisation. It is uniquely
        determined from the InstitutionIdentifier. The identifier is read
        from settings.json. The value is rarely used, but is needed to dertermine
        if a unit is a root unit.
        :return: The SD institution uuid for the organisation.
        """
        inst_id = self.settings['integrations.SD_Lon.institution_identifier']
        params = {
            'UUIDIndicator': 'true',
            'InstitutionIdentifier': inst_id
        }
        institution_info = sd_lookup('GetInstitution20111201', params)
        # print(institution_info.keys())
        institution = institution_info['Region']['Institution']
        institution_uuid = institution['InstitutionUUIDIdentifier']
        return institution_uuid

    def create_single_department(self, unit_uuid, validity_date):
        """
        Create a single department by reading the state of the department from SD.
        The unit will be created with validity from the creation date returned by SD
        to infinity. Notice that this validity is not necessarily correct and a
        call to fix_department_at_single_date might be needed to ensure internal
        consistency of the organisation.
        :param unit_uuid: The uuid of the unit to be created, this uuid is also used
        for the new unit in MO.
        :param validity_date: The validity_date to use when reading the properties of
        the unit from SD.
        """
        logger.info('Create department: {}, at {}'.format(unit_uuid, validity_date))
        validity = {
            'from_date': validity_date.strftime('%d.%m.%Y'),
            'to_date': validity_date.strftime('%d.%m.%Y')
        }
        # We ask for a single date, and will always get a single element.
        department = self.get_department(validity, uuid=unit_uuid)[0]
        logger.debug('Department info to create from: {}'.format(department))
        print('Department info to create from: {}'.format(department))
        parent = self.get_parent(department['DepartmentUUIDIdentifier'],
                                 validity_date)
        if parent is None:  # This is a root unit.
            parent = self.org_uuid

        for unit_level in self.level_types:
            if unit_level['user_key'] == department['DepartmentLevelIdentifier']:
                unit_level_uuid = unit_level['uuid']

        payload = sd_payloads.create_single_org_unit(
            department=department,
            unit_type=self.unit_type['uuid'],
            unit_level=unit_level_uuid,
            parent=parent
        )
        logger.debug('Create department payload: {}'.format(payload))
        response = self.helper._mo_post('ou/create', payload)
        response.raise_for_status()
        logger.info('Created unit {}'.format(
            department['DepartmentIdentifier'])
        )
        logger.debug('Response: {}'.format(response.text))

    def fix_department_at_single_date(self, unit_uuid, validity_date):
        logger.info('Set department {} to state as of today'.format(unit_uuid))
        validity = {
            'from_date': validity_date.strftime('%d.%m.%Y'),
            'to_date': validity_date.strftime('%d.%m.%Y')
        }
        department = self.get_department(validity, uuid=unit_uuid)[0]

        for unit_level in self.level_types:
            if unit_level['user_key'] == department['DepartmentLevelIdentifier']:
                unit_level_uuid = unit_level['uuid']

        try:
            parent = self.get_parent(unit_uuid, validity_date)
            department = self.get_department(validity, uuid=unit_uuid)[0]
            name = department['DepartmentName']
            shortname = department['DepartmentIdentifier']
        except NoCurrentValdityException:
            msg = 'Attempting to fix unit with no parent at {}!'
            logger.error(msg.format(validity_date))
            raise Exception(msg.format(validity_date))

        # SD has a challenge with the internal validity-consistency, extend first
        # validity indefinitely
        from_date = '1900-01-01'
        if parent is None:
            parent = self.org_uuid
        print('Unit parent at {} is {}'.format(from_date, parent))

        payload = sd_payloads.edit_org_unit(
            user_key=shortname,
            name=name,
            unit_uuid=unit_uuid,
            parent=parent,
            ou_level=unit_level_uuid,
            ou_type=self.unit_type['uuid'],
            from_date=from_date
        )
        logger.debug('Edit payload to fix unit: {}'.format(payload))
        response = self.helper._mo_post('details/edit', payload)
        if response.status_code == 400:
            assert(response.text.find('raise to a new registration') > 0)
        else:
            response.raise_for_status()
        logger.debug('Response: {}'.format(response.text))

    def get_department(self, validity, shortname=None, uuid=None):
        """
        Read department information from SD.
        NOTICE: Shortname is not universally unitque in SD, and even a request
        spanning a single date might return more than one row if searched by
        shortname.
        :param validity: Validity dictionaty containing two datetime objects
        with keys from_date and to_date.
        :param shortname: Shortname for the unit(s).
        :param uuid: uuid for the unit.
        :return: A list of information about the unit(s).
        """
        params = {
            'ActivationDate': validity['from_date'],
            'DeactivationDate': validity['to_date'],
            'ContactInformationIndicator': 'true',
            'DepartmentNameIndicator': 'true',
            'PostalAddressIndicator': 'false',
            'ProductionUnitIndicator': 'false',
            'UUIDIndicator': 'true',
            'EmploymentDepartmentIndicator': 'false'
        }
        if uuid is not None:
            params['DepartmentUUIDIdentifier'] = uuid
        if shortname is not None:
            params['DepartmentIdentifier'] = shortname

        if uuid is None and shortname is None:
            raise Exception('Provide either uuid or shortname')

        department_info = sd_lookup('GetDepartment20111201', params)
        department = department_info.get('Department')
        if department is None:
            raise NoCurrentValdityException()
        if isinstance(department, dict):
            department = [department]
        return department

    # Notice! This code also exists in sd_changed_at!
    def _find_engagement(self, mo_engagements, job_id):
        """
        Given a list of engagements for a person, find the one with a specific
        job_id. If severel elements covering the same engagement is in the list
        an unspecified element will be returned.
        :param mo_engaements: A list of engagements as returned by MO.
        :param job_id: The SD JobIdentifier to find.
        :return: Some element in the list that has the correct job_id. If no
        engagement is found, None is returned.
        """
        relevant_engagement = None
        try:
            user_key = str(int(job_id)).zfill(5)
        except ValueError:  # We will end here, if int(job_id) fails
            user_key = job_id

        for mo_eng in mo_engagements:
            if mo_eng['user_key'] == user_key:
                relevant_engagement = mo_eng

        if relevant_engagement is None:
            msg = 'Fruitlessly searched for {} in {}'.format(job_id,
                                                             mo_engagements)
            logger.info(msg)
        return relevant_engagement

    def _fix_NY_logic(self, unit_uuid, validity_date):
        # This should be called AFTER the recursive fix of the department_tree
        sd_validity = {
            'from_date': validity_date.strftime('%d.%m.%Y'),
            'to_date': validity_date.strftime('%d.%m.%Y')
        }
        too_deep = self.settings['integrations.SD_Lon.import.too_deep']
        department = self.get_department(sd_validity, uuid=unit_uuid)[0]
        if not department['DepartmentLevelIdentifier'] in too_deep:
            print('{} regnes ikke som et SD afdelingsniveau'.format(unit_uuid))
            return

        mo_unit = self.helper.read_ou(unit_uuid, use_cache=False)
        while mo_unit['org_unit_level']['user_key'] in too_deep:
            mo_unit = mo_unit['parent']
            logger.debug('Parent unit: {}'.format(mo_unit))
        destination_unit = mo_unit['uuid']
        logger.debug('Destination found: {}'.format(destination_unit))

        params = {
            'DepartmentIdentifier': department['DepartmentIdentifier'],
            'DepartmentLevelIdentifier': department['DepartmentLevelIdentifier'],
            'StatusActiveIndicator': True,
            'StatusPassiveIndicator': False,
            'DepartmentIndicator': True,
            'UUIDIndicator': True
        }

        # We need to catch all current and future engagements, this is an attempt to
        # do so, without making too many calls to the api.
        time_deltas = [0, 90, 365]

        all_people = {}
        for time_delta in time_deltas:
            effective_date = validity_date + datetime.timedelta(days=time_delta)
            params['EffectiveDate'] = effective_date.strftime('%d.%m.%Y'),

            employments = sd_lookup('GetEmployment20111201', params, use_cache=True)
            people = employments['Person']
            if not isinstance(people, list):
                people = [people]

            for person in people:
                cpr = person['PersonCivilRegistrationIdentifier']
                if cpr not in all_people:
                    all_people[cpr] = person

        # We now have a list of all current and future people in the unit,
        # they should all be unconditionally moved if they are not already
        # in destination_unit.
        for person in all_people.values():
            cpr = person['PersonCivilRegistrationIdentifier']
            job_id = person['Employment']['EmploymentIdentifier']
            print('Chekking job-id: {}'.format(job_id))
            sd_uuid = (person['Employment']['EmploymentDepartment']
                       ['DepartmentUUIDIdentifier'])
            if not sd_uuid == unit_uuid:
                # This employment is not from the current departpartment,
                # but is inherited from a lower level. Can happen if this
                # tool is initiated on a level higher than Afdelings-niveau.
                continue

            mo_person = self.helper.read_user(user_cpr=cpr,
                                              org_uuid=self.org_uuid)

            mo_engagements = self.helper.read_user_engagement(
                    mo_person['uuid'],
                    read_all=True,
                    only_primary=True,
                    skip_past=True,
                    use_cache=False
            )

            # Find the uuid of the relevant engagement and update all current and
            # future rows.
            mo_engagement = self._find_engagement(mo_engagements, job_id)
            for eng in mo_engagements:
                if eng['uuid'] == mo_engagement['uuid']:
                    if eng['org_unit']['uuid'] == destination_unit:
                        continue

                    from_date = datetime.datetime.strptime(
                        eng['validity']['from'], '%Y-%m-%d')
                    if from_date < validity_date:
                        eng['validity']['from'] = validity_date.strftime('%Y-%m-%d')

                    data = {'org_unit': {'uuid': destination_unit},
                            'validity': eng['validity']}
                    payload = sd_payloads.engagement(data, mo_engagement)
                    print(payload)
                    response = self.helper._mo_post('details/edit', payload)
                    mora_assert(response)

    def get_parent(self, unit_uuid, validity_date):
        """
        Return the parent of a given department at at given point in time.
        Notice that the query is perfomed against SD, not against MO.
        It is generally not possible to predict whether this call will succeed, since
        this depends on the internal start-date at SD, which cannot be read from the
        API; the user of this function should be prepared to handle
        NoCurrentValdityException, unless the validity of the unit is known from
        other sources. In general queries to the future and near past should always
        be safe if the unit exists at the point in time.
        :param unit_uuid: uuid of the unit to be queried.
        :param validity_date: python datetie object with the date to query.
        :return: uuid of the parent department, None if the department is a root.
        """
        params = {
            'EffectiveDate': validity_date.strftime('%d.%m.%Y'),
            'DepartmentUUIDIdentifier': unit_uuid
        }
        parent_response = sd_lookup('GetDepartmentParent20190701', params)
        if 'DepartmentParent' not in parent_response:
            msg = 'No parent for {} found at validity: {}'
            logger.error(msg.format(unit_uuid, validity_date))
            raise NoCurrentValdityException()
        parent = parent_response['DepartmentParent']['DepartmentUUIDIdentifier']
        if parent == self.institution_uuid:
            parent = None
        return parent

    def get_all_parents(self, leaf_uuid, validity_date):
        """
        Find all parents from leaf unit up to the root of the tree.
        Notice, this is a query to SD, not to MO.
        :param leaf_uuid: The starting point of the chain, this does not stictly need
        to be a leaf node.
        :validity_date: The validity date of the fix.
        :return: A list of unit uuids sorted from leaf to root.
        """
        validity = {
            'from_date': validity_date.strftime('%d.%m.%Y'),
            'to_date': validity_date.strftime('%d.%m.%Y')
        }
        department_branch = []
        department = self.get_department(validity=validity, uuid=leaf_uuid)[0]
        department_branch.append((department['DepartmentIdentifier'], leaf_uuid))

        current_uuid = self.get_parent(department['DepartmentUUIDIdentifier'],
                                       validity_date=validity_date)

        while current_uuid is not None:
            current_uuid = self.get_parent(department['DepartmentUUIDIdentifier'],
                                           validity_date=validity_date)
            department = self.get_department(validity=validity, uuid=current_uuid)[0]
            shortname = department['DepartmentIdentifier']
            level = department['DepartmentLevelIdentifier']
            uuid = department['DepartmentUUIDIdentifier']
            department_branch.append((shortname, uuid))
            current_uuid = self.get_parent(current_uuid, validity_date=validity_date)
            msg = 'Department: {}, uuid: {}, level: {}'
            logger.debug(msg.format(shortname, uuid, level))
        return department_branch

    def fix_or_create_branch(self, leaf_uuid, date):
        """
        Run through all units up to the top of the tree and synchroize the state of
        MO to the state of SD. This includes reanming of MO units, moving MO units
        and creating units that currently does not exist in MO. The updated validity
        of the MO units will extend from 1900-01-01 to infinity and any existing
        validities will be overwritten.
        :param leaf_uuid: The starting point of the fix, this does not stictly need
        to be a leaf node.
        :date: The validity date of the fix.
        """
        # This is a question to SD, units will not need to exist in MO
        branch = self.get_all_parents(leaf_uuid, date)

        for unit in branch:
            mo_unit = self.helper.read_ou(unit[1])
            if 'status' in mo_unit:  # Unit does not exist in MO
                logger.warning('Unknown unit {}, will create'.format(unit))
                self.create_single_department(unit[1], date)
        for unit in reversed(branch):
            self.fix_department_at_single_date(unit[1], date)

    def _cli(self):
        """
        Command line interface to sync SD departent information to MO.
        """
        parser = argparse.ArgumentParser(description='Department updater')
        parser.add_argument('--department-uuid', nargs=1, required=True,
                            metavar='UUID of the department to update')
        args = vars(parser.parse_args())

        today = datetime.datetime.today()
        department_uuid = args.get('department_uuid')[0]

        # Use a future date to be sure that the unit exists in SD.
        fix_date = today + datetime.timedelta(weeks=80)
        self.fix_or_create_branch(department_uuid, fix_date)

        self._fix_NY_logic(department_uuid, today)


if __name__ == '__main__':
    unit_fixer = FixDepartments()
    # uruk = 'cf9864bf-1ed8-4800-9600-000001290002'
    # today = datetime.datetime.today()
    # print(unit_fixer.get_all_parents(uruk, from_date))
    # print(unit_fixer.get_all_parents(uruk, today))
    # unit_fixer.fix_or_create_branch(uruk, today)
    unit_fixer._cli()
