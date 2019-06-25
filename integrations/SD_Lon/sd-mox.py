import os
import uuid
import pika
import datetime
import xmltodict
import sd_mox_payloads as smp

from collections import OrderedDict
from sd_common import sd_lookup
from os2mo_helpers.mora_helpers import MoraHelper

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


class sdMox(object):
    def __init__(self, from_date, to_date=None):

        self.exchange_name = 'org-struktur-changes-topic'
        credentials = pika.PlainCredentials(AMQP_USER, AMQP_PASSWORD)
        parameters = pika.ConnectionParameters(host='msg-amqp.silkeborgdata.dk',
                                               port=5672,
                                               virtual_host=VIRTUAL_HOST,
                                               credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.mh = MoraHelper(hostname='localhost:5000')

        sd_levels = [
            ('NY7-niveau', None),
            ('NY6-niveau', None),
            ('NY5-niveau', None),
            ('NY4-niveau', None),
            ('NY2-niveau', None),
            ('NY2-niveau', None),
            ('NY1-niveau', None),
            ('Afdelings-niveau', None)
        ]
        sd_levels = collections.OrderedDict(sd_levels)
        
        ut = self.mh.read_classes_in_facet('org_unit_type')
        for unit_type in ut[0]:
            if unit_type['user_key']
        1/0
        if not from_date.day == 1:
            raise Exception('Day of month must be 1')

        self.virkning = smp.sd_virkning(from_date, to_date)

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

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

        print(params)
            
        try:
            department = sd_lookup('GetDepartment20111201', params)
            department_info = department.get('Department', None)
        except KeyError:  # Bug in SD soap-interface
            department_info = None
        return department_info

    def _check_department(self, department, name, unit_code, unit_uuid):
        from_date = self.virkning['sd:FraTidspunkt']['sd:TidsstempelDatoTid'][0:10]
        print(department)
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

    def create_xml_import(self, name, unit_code, parent, unit_uuid):
        value_dict = {
            'RelationListe': smp.relations_import(self.virkning, parent),
            'AttributListe': smp.attributes_import(
                self.virkning,
                unit_code=unit_code,
                unit_name=name,
                niveau='TODO'
            ),
            'Registrering': smp.create_registrering(self.virkning,
                                                    registry_type='Opstaaet'),
            'ObjektID': smp.create_objekt_id(unit_uuid)
        }
        edit_dict = {'RegistreringBesked': value_dict}
        edit_dict['RegistreringBesked'].update(smp.boilerplate)
        xml = xmltodict.unparse(edit_dict)
        return xml

    def create_xml_ret(self, unit_uuid, name):
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
                uuid_errors.append('Enhedskode er i brug')
        return uuid_errors

    def _mo_to_sd_address(self, unit_uuid):
        mo_unit_address = mox.mh.read_ou_address('5aafa2f4-9a8b-46dd-82fa-56138e495a2e')
        address = mo_unit_address['Adresse']
        print(address)
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
        
    def create_unit(self, name, unit_code, parent, unit_uuid=None):
        if not unit_uuid:
            unit_uuid = str(uuid.uuid4())
            uuid_errors = []
        else:
            uuid_errors = self._validate_unit_uuid(unit_uuid)
        code_errors = self._validate_unit_code(unit_code)
        
        if code_errors or uuid_errors:
            raise Exception(str(code_errors + uuid_errors))

        parent_department = self.read_department(unit_uuid=parent)
        if not parent_department:
            raise Exception('Forældrenheden finds ikke')

        xml = self.create_xml_import(
            name=name,
            unit_uuid=unit_uuid,
            unit_code=unit_code,
            parent=parent
        )
        # self.call(xml)

        # run an edit to include all information
        return unit_uuid

    def on_response(self, ch, method, props, body):
        print('Response?!??!?!')
        print(body)

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

if __name__ == '__main__':
    from_date = datetime.datetime(2020, 5, 1, 0, 0)
    # to_date = datetime.datetime(2020, 6, 1, 0, 0)

    parent_uuid = 'fd47d033-61c0-4900-b000-000001520002'
    unit_uuid = '5aafa2f4-9a8b-46dd-82fa-56138e495a2e'
  
    mox = sdMox(from_date)
    mox._mo_to_sd_address(unit_uuid)
 
    unit_info = mox.mh.read_ou(unit_uuid)['org_unit_type']['name']
    print(unit_info)
    name = unit_info['name']
    unit_code = unit_info['user_key'][0:4]
    1/0
   
    if True:
        unit_uuid = mox.create_unit(
            unit_uuid=unit_uuid,
            name=name,
            unit_code=unit_code,
            parent=parent_uuid
        )
        1/0
        department = mox.read_department(unit_code)
        errors = mox._check_department(department, name, unit_code, unit_uuid)
        print(errors)
        
    if True:
        xml = mox.edit_unit(
            uuid=uuid,
            name='Test 2'
        )


# TODO: Soon we are ready to write small tests to verify expected output
# from xml-producing functions.
