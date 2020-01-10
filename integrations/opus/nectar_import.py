import logging
import requests

from integrations import dawa_helper
from integrations.opus import opus_helpers
from integrations.opus.opus_base import OpusBase

from integrations.opus.opus_exceptions import UnknownOpusAction

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger('OpusNectarImport')

for name in logging.root.manager.loggerDict:
    if name in ('moImporterMoraTypes', 'moImporterMoxTypes', 'moImporterUtilities',
                'moImporterHelpers'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


class NectarImport(OpusBase):
    def __init__(self, importer):
        super().__init__(importer)

    def read_from_nectar(self, url):
        import pickle
        pickle_key = url.replace('/', '_') + '.p'
        with open(pickle_key, 'rb') as f:
            nectar_objects = pickle.load(f)
        return nectar_objects

        max_index = 9999999  # Will be updated after the first request
        next_position = 0
        nectar_objects = []
        while len(nectar_objects) < max_index:
            print(url.format(next_position))
            params = {
                'formatType': 'Native',
                'page': next_position
            }
            response = requests.get(
                url.format(next_position),
                verify=False,
                params=params,
                auth=(self.settings['integrations.nectar.user'],
                      self.settings['integrations.nectar.password'])
            )
            response.raise_for_status()

            data = response.json()
            if not data.get('Successful', False):
                # If we ever get a non-successful reply, these keys might be useful
                # data['ResultType'], data['Exception'] data['Message']
                msg = 'Error in Nectar response'
                logger.error(msg)
                raise Exception(msg)

            # nectar_objects = nectar_objects + data['Objects'] #  Type simple
            nectar_objects = nectar_objects + data['ResultRecords']

            next_position = data['NextPagePosition']
            max_index = data['MaxIndex']
            print('Next positio: {}'.format(next_position))
            print('Max index: {}'.format(max_index))
            print('Collected objects: {}'.format(len(nectar_objects)))

        with open(pickle_key, 'wb') as f:
            pickle.dump(nectar_objects, f, pickle.HIGHEST_PROTOCOL)

        return nectar_objects

    def _import_org_unit(self, nectar_unit):
        # Are there no org_unit_types in Nectar?
        org_type = 'Enhed'
        self._add_klasse(org_type, 'Enhed', 'org_unit_type')

        unit = {}
        address_string = None
        zip_code = None
        for element in nectar_unit['Properties']:
            if element['Name'] == 'OrganizationalUnitID':
                identifier = element['Value']
            elif element['Name'] == 'ParentOrganizationalUnitID':
                parent_org = element['Value']
            elif element['Name'] == 'OrganizationName':
                name = element['Value']
            elif element['Name'] == 'OrganizationAddress':
                address_string = element['Value']
            elif element['Name'] == 'OrganizationCity':
                pass
            elif element['Name'] == 'OrganizationPostCode':
                zip_code = element['Value']
            elif element['Name'] == 'OrganizationPhone':
                unit['phoneNumber'] = element['Value']
            else:
                raise Exception('Found new key')

        print('{}, id: {}, parent: {}'.format(name, identifier, parent_org))

        uuid = opus_helpers.generate_uuid(identifier)
        logger.debug('Generated uuid for {}: {}'.format(identifier, uuid))

        # Where has all this gone? Is it not part of Nectar import
        # user_key = unit['shortName']
        # date_from = unit['startDate']
        # if unit['endDate'] == '9999-12-31':
        #     date_to = None
        # else:
        #     date_to = unit['endDate']

        # if parent_org == self.organisation_id and not self.import_first:
        #     parent_org = None

        # Default in the lack of actual values:
        date_from = '1900-01-01'
        date_to = None
        user_key = name
        # From here should go to OpusBase
        # self._unit_import(identifier, name, uuid, user_key,
        # org_type, date_from, date_to, unit)
        self.importer.add_organisation_unit(
            identifier=identifier,
            name=name,
            uuid=str(uuid),
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

    # TODO: Check carefully for overlap with opus-import-classic
    def _import_employee(self, employee):
        logger.debug('Employee object: {}'.format(employee))
        if 'cpr' in employee:
            cpr = employee['cpr']['#text']
            if employee['firstName'] is None and employee['lastName'] is None:
                # Service user, skip
                logger.info('Skipped {}, we think it is a serviceuser'.format(cpr))
                return

        else:  # This employee has left the organisation
            if not employee['@action'] == 'leave':
                msg = 'Unknown action: {}'.format(employee['@action'])
                logger.error(msg)
                raise UnknownOpusAction(msg)
            return

        self._update_ad_map(cpr)

        uuid = self.employee_forced_uuids.get(cpr)
        logger.info('Employee in force list: {} {}'.format(cpr, uuid))
        if uuid is None and 'ObjectGuid' in self.ad_people[cpr]:
            uuid = self.ad_people[cpr]['ObjectGuid']

        date_from = employee['entryDate']
        date_to = employee['leaveDate']

        # Only add employee and address information once, this info is duplicated
        # if the employee has multiple engagements
        if not self.importer.check_if_exists('employee', cpr):
            self.employee_addresses[cpr] = {}
            self.importer.add_employee(
                identifier=cpr,
                name=(employee['firstName'], employee['lastName']),
                cpr_no=cpr,
                uuid=uuid
            )

        if 'SamAccountName' in self.ad_people[cpr]:
            self.importer.join_itsystem(
                employee=cpr,
                user_key=self.ad_people[cpr]['SamAccountName'],
                itsystem_ref='AD',
                date_from=None
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
            phone = opus_helpers.parse_phone(employee['workPhone'])
            self.employee_addresses[cpr]['PhoneEmployee'] = phone

        if 'postalCode' in employee and employee['address']:
            if isinstance(employee['address'], dict):
                # This is a protected address, cannot import
                pass
            else:
                address_string = employee['address']
                zip_code = employee["postalCode"]
                addr_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
                if addr_uuid:
                    self.employee_addresses[cpr]['AdressePostEmployee'] = addr_uuid

        job = employee['position']
        self._add_klasse(job, job, 'engagement_job_function')

        if 'workContractText' in employee:
            contract = employee['workContract']
            self._add_klasse(contract, employee['workContractText'],
                             'engagement_type')
        else:
            contract = '1'
            self._add_klasse(contract, 'Ansat', 'engagement_type')

        org_unit = employee['orgUnit']
        job_id = employee['@id']
        engagement_uuid = opus_helpers.generate_uuid(job_id)

        logger.info('Add engagement: {} to {}'.format(job_id, cpr))
        self.importer.add_engagement(
            employee=cpr,
            uuid=str(engagement_uuid),
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
            logger.info('{} is manager {}'.format(cpr, manager_level))
            self.importer.add_manager(
                employee=cpr,
                user_key=job_id,
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
                logger.debug('{} has role {}'.format(cpr, role))
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

    def insert_org_units(self):
        url = self.settings['integrations.nectar.url'] + 'Organization/OPUSLoen/All'
        units = self.read_from_nectar(url=url)
        for unit in units:
            self._import_org_unit(unit)

    def insert_users(self):
        url = self.settings['integrations.nectar.url'] + 'person/OPUSLoen/All'
        opus_employee = {}
        employees = self.read_from_nectar(url=url)

        # This will go to the for-loop, for now one example will do
        nectar_employee = employees[117]['Properties']

        skip_fields = ['OrganizationCity', 'ParentOrganizationalUnitID',
                       'OrganizationAddress', 'OrganizationPostCode',
                       'OrganizationPhone']

        for employee in employees:
            debug_employee = False

            nectar_employee = employee['Properties']
            for element in nectar_employee:
                if element['Name'] == 'CPR':
                    opus_employee['cpr'] = element['Value']
                elif element['Name'] == 'MedarbejderId':
                    opus_employee['userId'] = element['Value']
                elif element['Name'] == 'FirstName':
                    opus_employee['firstName'] = element['Value']
                elif element['Name'] == 'LastName':
                    opus_employee['lastName'] = element['Value']
                elif element['Name'] == 'StartDate':
                    opus_employee['entryDate'] = element['Value']
                elif element['Name'] == 'EndDate':
                    opus_employee['leaveDate'] = element['Value']
                elif element['Name'] == 'OrganizationalUnitID':
                    opus_employee['orgUnit'] = element['Value']
                elif element['Name'] == 'PositionName':
                    opus_employee['position'] = element['Value']
                elif element['Name'] == 'PositionId':
                    # This is assumed to be the opus userId
                    opus_employee['@id'] = element['Value']
                elif element['Name'] == 'EmploymentType':
                    # This is assumed somehow be the same as workContract?
                    opus_employee['workContract'] = element['Value']
                elif element['Name'] == 'IsManager':
                    if element['Value'][0] == '0':
                        opus_employee['isManager'] = 'false'
                    else:
                        # TODO: We do not know the meaing of the value
                        # print('IsManager: "{}"'.format(element['Value']))
                        opus_employee['isManager'] = 'true'
                elif element['Name'] == 'EmployeeGroup':
                    # Unknwon field
                    # print('EmployeeGroup: "{}"'.format(element['Value']))
                    pass
                elif element['Name'] == 'ManagerId':
                    # Buest current guess, this is the name of the person's manager
                    # We do not need this for information.
                    pass

                # These two emelements both indicate whether an engagment is
                # active or terminated:
                elif element['Name'] == 'Status':
                    if not element['Value'] == 'Active':
                        # print('Status: "{}"'.format(element['Value']))
                        pass
                elif element['Name'] == 'StatusId':
                    if not element['Value'] == '3':
                        # debug_employee = True
                        # print('StatusId: "{}"'.format(element['Value']))
                        pass
                elif element['Name'] in skip_fields:
                    pass
                else:
                    raise Exception('Found new key: {}'.format(element))
            if debug_employee:
                print(employee)
            # self._import_employee(opus_employee)


if __name__ == '__main__':
    nectar = NectarImport()
