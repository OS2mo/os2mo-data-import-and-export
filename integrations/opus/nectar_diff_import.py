import logging
import requests

from datetime import datetime

from integrations.opus import payloads
# from integrations.opus import opus_helpers
from integrations.opus.opus_diff_common import OpusDiffCommon

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'nectar.log'

logger = logging.getLogger('OpusNectarImport')

for name in logging.root.manager.loggerDict:
    if name in ('moImporterMoraTypes', 'moImporterMoxTypes', 'moImporterUtilities',
                'moImporterHelpers', 'OpusNectarImport'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


class NectarImport(OpusDiffCommon):
    def __init__(self):
        logger.info('Nectar Opus Diff Import Running')
        latest_date = '1900-01-01'
        ad_reader = None
        super().__init__(latest_date, ad_reader)

        self.latest_date = datetime.now()  # NOTICE!!!!!!!!!!!

    def read_from_nectar(self, url):
        import pickle
        pickle_key = url.replace('/', '_') + '.p'
        # with open(pickle_key, 'rb') as f:
        #     nectar_objects = pickle.load(f)
        # return nectar_objects

        max_index = 9999999  # Will be updated after the first request
        next_position = 0
        nectar_objects = []
        while len(nectar_objects) < max_index:
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

    def update_employee(self, employee):
        cpr = employee['cpr']['#text']
        logger.info('----')
        logger.info('Now updating {}'.format(cpr))
        logger.debug('Available info: {}'.format(employee))
        mo_user = self.helper.read_user(user_cpr=cpr)
        if mo_user is None:
            employee_mo_uuid = self.create_user(employee)
        else:
            employee_mo_uuid = mo_user['uuid']
            if not ((employee['firstName'] == mo_user['givenname']) and
                    (employee['lastName'] == mo_user['surname'])):
                payload = payloads.create_user(employee, self.org_uuid,
                                               employee_mo_uuid)
                return_uuid = self.helper._mo_post('e/create', payload).json()
                msg = 'Updated name of employee {} with uuid {}'
                logger.info(msg.format(cpr, return_uuid))

        self._update_employee_address(employee_mo_uuid, employee)

        # Now we have a MO uuid, update engagement:
        mo_engagements = self.helper.read_user_engagement(employee_mo_uuid,
                                                          read_all=True)
        current_mo_eng = None
        for eng in mo_engagements:
            if eng['user_key'] == employee['@id']:
                current_mo_eng = eng['uuid']
                val_from = datetime.strptime(eng['validity']['from'], '%Y-%m-%d')
                if eng['validity']['to'] is None:
                    val_to = datetime.strptime('9999-12-31', '%Y-%m-%d')
                else:
                    val_to = datetime.strptime(eng['validity']['to'], '%Y-%m-%d')
                if val_from < self.latest_date < val_to:
                    logger.info('Found current validty {}'.format(eng['validity']))
                    break

        if current_mo_eng is None:
            self.create_engagement(employee_mo_uuid, employee)
        else:
            logger.info('Validity for {}: {}'.format(employee['@id'],
                                                     eng['validity']))
            self.update_engagement(eng, employee)

        self.update_manager_status(employee_mo_uuid, employee)
        self.updater.set_current_person(cpr=cpr)
        self.updater.recalculate_primary()

    def insert_users(self):
        url = self.settings['integrations.nectar.url'] + 'person/OPUSLoen/All'
        opus_employee = {}
        employees = self.read_from_nectar(url=url)

        skip_fields = ['OrganizationCity', 'ParentOrganizationalUnitID',
                       'OrganizationAddress', 'OrganizationPostCode',
                       'OrganizationPhone']

        for employee in employees:
            nectar_employee = employee['Properties']
            for element in nectar_employee:
                if element['Name'] == 'CPR':
                    opus_employee['cpr'] = {'#text': element['Value']}
                elif element['Name'] == 'MedarbejderId':
                    opus_employee['userId'] = element['Value']
                elif element['Name'] == 'FirstName':
                    opus_employee['firstName'] = element['Value']
                elif element['Name'] == 'LastName':
                    opus_employee['lastName'] = element['Value']
                elif element['Name'] == 'StartDate':
                    opus_employee['entryDate'] = element['Value']
                elif element['Name'] == 'EndDate':
                    if element['Value'] == '9999-12-31':
                        opus_employee['leaveDate'] = None
                    else:
                        opus_employee['leaveDate'] = element['Value']
                elif element['Name'] == 'OrganizationalUnitID':
                    opus_employee['orgUnit'] = str(int(element['Value']))
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
                        # Where do we get these?
                        opus_employee['superiorLevel'] = '0'
                        opus_employee['subordinateLevel'] = '0'
                elif element['Name'] == 'EmployeeGroup':
                    # Unknown field
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

            self.update_employee(opus_employee)


if __name__ == '__main__':
    nectar = NectarImport()

    nectar.insert_users()
