import os
import uuid
import pika
import logging
import datetime
import xmltodict
import sd_mox_payloads as smp

from collections import OrderedDict
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper

logger = logging.getLogger('sdMox')


INSTITUTION_IDENTIFIER = os.environ.get('INSTITUTION_IDENTIFIER')
SD_USER = os.environ.get('SD_USER', None)
SD_PASSWORD = os.environ.get('SD_PASSWORD', None)
if not (INSTITUTION_IDENTIFIER and SD_USER and SD_PASSWORD):
    raise Exception('SD Webservice credentials missing')

AMQP_USER = os.environ.get('AMQP_USER')
AMQP_PASSWORD = os.environ.get('AMQP_PASSWORD', None)
VIRTUAL_HOST = os.environ.get('VIRTUAL_HOST', None)
if not (AMQP_USER and AMQP_PASSWORD and VIRTUAL_HOST):
    raise Exception('SD AMQP credentials missing')

class sdMoxException(Exception):
    pass

class sdMox(object):
    def __init__(self, from_date=None, to_date=None):
        self._init_amqp_comm()
        if from_date:
            self._update_virkning(from_date)

        # TODO: This url is hard-codet
        self.mh = MoraHelper(hostname='http://localhost:5000')

        sd_levels = [
            ('Top', '324b8c95-5ff9-439b-a49c-1a6a6bba4651'),
            ('NY7-niveau', '42b9042f-5f20-4998-b0e5-c4deb6c5f42e'),
            ('NY6-niveau', '414a035e-9c22-42eb-b035-daa7d7f2ade8'),
            ('NY5-niveau', '819ae28e-04e0-4030-880e-7b699faeaff9'),
            ('NY4-niveau', 'ff8c3f53-85ec-44d7-a9d6-07c619ac50df'),
            ('NY4-niveau', '70c69826-4ba1-4e1e-82f0-4c47c89a7ecc'),
            ('NY2-niveau', 'ec882c49-3cc2-4bc9-994f-a6f29136401b'),
            ('NY1-niveau', 'd9bd186b-3c11-4dbf-92d1-4e3b61140302'),
            ('Afdelings-niveau', '345a8893-eb1f-4e20-b76d-63b95b5809f6')
        ]
        self.sd_levels = OrderedDict(sd_levels)

    def _init_amqp_comm(self):
        self.exchange_name = 'org-struktur-changes-topic'
        credentials = pika.PlainCredentials(AMQP_USER, AMQP_PASSWORD)
        parameters = pika.ConnectionParameters(host='msg-amqp.silkeborgdata.dk',
                                               port=5672,
                                               virtual_host=VIRTUAL_HOST,
                                               credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def on_response(self, ch, method, props, body):
        logger.error('Unexpected response!')
        logger.error(body)
        raise sdMoxException('Unexpected amqp response')

    def call(self, xml):
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key='#',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue
            ),
            body=xml
        )
        # Todo: We should to a lookup at verify actual unit
        # matches the expected result
        return True

    def _update_virkning(self, from_date, to_date=None):
        self.virkning = smp.sd_virkning(from_date, to_date)
        if not from_date.day == 1:
            raise sdMoxException('Day of month must be 1')

    def read_department(self, unit_code=None, unit_uuid=None):
        from_date = self.virkning['sd:FraTidspunkt']['sd:TidsstempelDatoTid'][0:10]
        params = {
            'ActivationDate': from_date,
            'DeactivationDate': from_date,
            'ContactInformationIndicator': 'true',
            'DepartmentNameIndicator': 'true',
            'PostalAddressIndicator': 'true',
            'ProductionUnitIndicator': 'true',
            'UUIDIndicator': 'true',
            'EmploymentDepartmentIndicator': 'false'
        }
        if unit_code:
            params['DepartmentIdentifier'] = unit_code,
        if unit_uuid:
            params['DepartmentUUIDIdentifier'] = unit_uuid

        try:
            department = sd_lookup('GetDepartment20111201', params)
            department_info = department.get('Department', None)
        except KeyError:
            # Bug in SD soap-interface, most likely these units actually do exist
            # department_info = 'Unknown department'
            department_info = None

        if isinstance(department_info, list):
            msg = 'Unit not unique. Code {}, uuid {}'.format(unit_code, unit_uuid)
            logger.error(msg)
            logger.error('Number units: {}'.format(len(department_info)))
            raise sdMoxException(msg)
        return department_info

    def _check_department(self, department, name, unit_code, unit_uuid):
        """
        Verify that an SD department contain what think it should contain.
        Besides the supplied parameters, the activation date is also checked
        agains the global from_date.
        :param department: An SD department as returned by read_department().
        :param name: Expected name.
        :param unit_code: Expexted unit code.
        :param unit_uuid: Exptected uunit.
        :return: Returns list errors, empty list if no errors.
        """
        from_date = self.virkning['sd:FraTidspunkt']['sd:TidsstempelDatoTid'][0:10]
        errors = []
        if not department['DepartmentName'] == name:
            errors.append('Name')
        if not department['DepartmentIdentifier'] == unit_code:
            errors.append('Unit code')
        if not department['ActivationDate'] == from_date:
            errors.append('Activation Date')
        if not department['DepartmentUUIDIdentifier'] == unit_uuid:
            errors.append('UUID')
        # print(department['PostalAddress'])
        return errors

    def _create_xml_import(self, name, unit_code, parent, unit_level, unit_uuid):
        """
        Create suitable xml-payload to create a unit. This is a helper function, it
        is expected that the values are allredy validated to be legal and consistent.
        :param name: Name of the new unit.
        :param unit_code: Short unique code (enhedskode) for the unit.
        :param parent: Parent unit for the unit.
        :param uuid: uuid for the unit.
        """
        value_dict = {
            'RelationListe': smp.relations_import(self.virkning, parent),
            'AttributListe': smp.attributes_import(
                self.virkning,
                unit_code=unit_code,
                unit_name=name,
                unit_type=unit_level
            ),
            'Registrering': smp.create_registrering(self.virkning,
                                                    registry_type='Opstaaet'),
            'ObjektID': smp.create_objekt_id(unit_uuid)
        }
        edit_dict = {'RegistreringBesked': value_dict}
        edit_dict['RegistreringBesked'].update(smp.boilerplate)
        xml = xmltodict.unparse(edit_dict)
        return xml

    def _create_xml_ret(self, unit_uuid, name):
        value_dict = {
            'RelationListe': smp.relations_ret(
                self.virkning,
                pnummer='1003407739',
                phone='995666655',
                adresse={
                    'silkdata:AdresseNavn': 'Arnegaard 799',
                    'silkdata:PostKodeIdentifikator': '2200',
                    'silkdata:ByNavn': 'Fd'
                }
            ),
            'AttributListe': smp.attributes_ret(self.virkning,
                                                unit_name=name),
            'Registrering': smp.create_registrering(self.virkning,
                                                    registry_type='Rettet'),
            'ObjektID': smp.create_objekt_id(unit_uuid)
        }
        edit_dict = {'RegistreringBesked': value_dict}
        edit_dict['RegistreringBesked'].update(smp.boilerplate)
        xml = xmltodict.unparse(edit_dict)
        return xml

    def _validate_unit_code(self, unit_code):
        logger.info('Validating unit code {}'.format(unit_code))
        code_errors = []
        if len(unit_code) < 2:
            code_errors.append('Enhedskode for kort')
        if len(unit_code) > 4:
            code_errors.append('Enhedskode for lang')
        if not unit_code.isalnum():
            code_errors.append('Ugyldigt tegn i enhedskode')

        if not code_errors:
            department = self.read_department(unit_code=unit_code)
            if department is not None:
                code_errors.append('Enhedskode er i brug')
        return code_errors

    def _validate_unit_uuid(self, unit_uuid):
        uuid_errors = []
        try:
            uuid.UUID(unit_uuid, version=4)
        except ValueError:
            uuid_errors.append('Ugyldig uuid')

        if not uuid_errors:
            department = self.read_department(unit_uuid=unit_uuid)
            if department is not None:
                uuid_errors.append('uuid er i brug')
        return uuid_errors

    def _mo_to_sd_address(self, unit_uuid):
        mo_unit_address = self.mh.read_ou_address(unit_uuid)
        address = mo_unit_address['Adresse']
        split_address = address.rsplit(' ', maxsplit=1)
        city = split_address[1].strip()
        street = split_address[0].split(',')[0].strip()
        zip_code = split_address[0].split(',')[1].strip()
        sd_address = {
            'silkdata:AdresseNavn': street,
            'silkdata:PostKodeIdentifikator': zip_code,
            'silkdata:ByNavn': city
        }
        return sd_address

    def create_unit(self, name, unit_code, parent, unit_level,
                    unit_uuid=None, test_run=True):
        """
        Create a new unit in SD.
        :param name: Unit name.
        :param unit_code: Short (3-4 chars) unique name (enhedskode).
        :param parent: Unit code of parent unit.
        :param unit_level: In SD the unit_type is tied to its level.
        :param uuid: uuid for unit, a random uuid will be generated if not provided.
        :param test_run: If true, all validations will be performed, but the
        amqp-call will not be executed, this allows for a pre-check that will
        confirm that the call will most likely succeed.
        :return: The uuid for the new unit. For test-runs with no provided uuid, this
        will not be the same random uuid as for the actual run, unless the returned
        uuid is stored and given as parameter for the actual run.
        """
        # Verify that uuid and unit_code is valid and unused.
        if not unit_uuid:
            unit_uuid = str(uuid.uuid4())
            uuid_errors = []
        else:
            uuid_errors = self._validate_unit_uuid(unit_uuid)
        code_errors = self._validate_unit_code(unit_code)

        if code_errors or uuid_errors:
            raise sdMoxException(str(code_errors + uuid_errors))

        # Verify the parent department actually exist
        print('Reading parent department')
        parent_department = self.read_department(unit_code=parent)
        print(parent)
        print(type(parent_department))
        print(len(parent_department))
        
        if not parent_department:
            raise sdMoxException('Forældrenheden finds ikke')

        unit_index = list(self.sd_levels.keys()).index(unit_level)
        print(parent_department)
        parent_index = list(self.sd_levels.keys()).index(
            parent_department['DepartmentLevelIdentifier']
        )
        print('Stopping')
        1/0
        if not unit_index > parent_index:
            raise sdMoxException('Enhedstypen passer ikke til forældreenheden')

        xml = self._create_xml_import(
            name=name,
            unit_uuid=unit_uuid,
            unit_code=unit_code,
            unit_level=unit_level,
            parent=parent
        )
        print(xml)
        if not test_run:
            self.call(xml)
        return unit_uuid

    def edit_unit(self, unit_uuid, phone=None, pnummer=None, address=None,
                  integration_values=None):
        pass

    def create_unit_from_mo(self, unit_uuid, test_run=True):
        logger.info('Create {} from MO, test run: {}'.format(unit_uuid, test_run))
        unit_info = mox.mh.read_ou(unit_uuid)
        logger.debug('Unit info: {}'.format(unit_info))

        from_date = datetime.datetime.strptime(
            unit_info['validity']['from'], '%Y-%m-%d'
        )

        self._update_virkning(from_date)

        parent_unit_code = unit_info['parent']['user_key']
        # Temperary fix for frontend being unable to set user_key
        unit_code = unit_info['user_key'][0:4]

        try:
            uuid = self.create_unit(
                name=unit_info['name'],
                parent=parent_unit_code,
                unit_code=unit_code,
                unit_level=unit_info['org_unit_type']['user_key'],
                unit_uuid=unit_uuid,
                test_run=test_run
            )
            print(uuid)
        except sdMoxException as e:
            print('Error: {}'.format(e))
            msg = 'Test for unit {} failed: {}'.format(unit_uuid, e)
            if test_run:
                logger.info(msg)
            else:
                logger.error(msg)
            return False

        integration_values = {
            'time_planning': unit_info.get('time_planning', None),
            'formaalskode': None,
            'skolekode': None
        }
        integration_addresses = self.mh._mo_lookup(unit_uuid,
                                                   'ou/{}/details/address')
        for integration_address in integration_addresses:
            if integration_address['address_type']['user_key'] == 'Formaalskode':
                integration_values['formaalskode'] = integration_address['value']
            if integration_address['address_type']['user_key'] == 'Skolekode':
                integration_values['skolekode'] = integration_address['value']

        pnummer = self.mh.read_ou_address(unit_uuid, scope='PNUMBER').get('value')
        phone = self.mh.read_ou_address(unit_uuid, scope='PHONE').get('value')

        address = mox._mo_to_sd_address(unit_uuid)

        if not test_run:
            # Running test-run for this edit is a bit more tricky
            self.edit_unit(
                phone=phone,
                pnummer=pnummer,
                address=address,
                integration_value=integration_values
            )


if __name__ == '__main__':
    # from_date = datetime.datetime(2020, 5, 1, 0, 0)
    # to_date = datetime.datetime(2020, 6, 1, 0, 0)

    unit_uuid = '9d6c8041-61c0-4900-a600-000001550002'

    mox = sdMox()

    mox.create_unit_from_mo(unit_uuid, test_run=True)

    # if False:
    #     unit_code = 'TST0'
    #     unit_uuid = mox.create_unit(
    #         unit_uuid=unit_uuid,
    #         name='Dav',
    #         unit_code=unit_code,
    #         unit_level='Afdelings-niveau',
    #         parent=parent_uuid
    #     )
    #     1/0
    #     department = mox.read_department(unit_code)
    #     # errors = mox._check_department(department, name, unit_code, unit_uuid)
    #     # print(errors)

    # if False:
    #     xml = mox.edit_unit(
    #         uuid=uuid,
    #         name='Test 2'
    #     )


# TODO: Soon we are ready to write small tests to verify expected output
# from xml-producing functions.
