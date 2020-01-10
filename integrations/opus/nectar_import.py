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
            if debug_employee:
                print(employee)
            self._import_employee(opus_employee)


if __name__ == '__main__':
    nectar = NectarImport()
