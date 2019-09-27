import os
import uuid
import pika
import time
import logging
import datetime
import xmltodict
from integrations.SD_Lon.sd_logging import start_logging
from integrations.SD_Lon import sd_mox_payloads as smp
from integrations.SD_Lon.sd_common import SD


from collections import OrderedDict

logger = logging.getLogger('sdMox')

CFG_PREFIX = "integrations.SD_Lon.sd_mox."


class sdMoxException(Exception):
    pass


class sdMox(object):
    def __init__(self, from_date=None, to_date=None, **kwargs):
        cfg = self.config = kwargs

        sd_cfg = cfg["sd_common"]
        self.sd = SD(**sd_cfg)

        try:
            self.amqp_user = cfg["AMQP_USER"]
            self.amqp_password = cfg["AMQP_PASSWORD"]
            self.virtual_host = cfg["VIRTUAL_HOST"]
            self.amqp_host = cfg["AMQP_HOST"]
            self.amqp_port = cfg["AMQP_PORT"]
            self._init_amqp_comm()
        except Exception as e:
            logger.exception("SD AMQP credentials missing")
            raise

        try:
            sd_levels = [
                ('NY6-niveau', cfg["NY6_NIVEAU"]),
                ('NY5-niveau', cfg["NY5_NIVEAU"]),
                ('NY4-niveau', cfg["NY4_NIVEAU"]),
                ('NY3-niveau', cfg["NY3_NIVEAU"]),
                ('NY2-niveau', cfg["NY2_NIVEAU"]),
                ('NY1-niveau', cfg["NY1_NIVEAU"]),
                ('Afdelings-niveau', cfg["AFDELINGS_NIVEAU"])
            ]
            self.sd_levels = OrderedDict(sd_levels)
            self.level_by_uuid = {v: k for k, v in self.sd_levels.items()}

            sd_arbtid = [
                ('Dannes ikke', cfg["TR_DANNES_IKKE"]),
                ('Arbejdstidsplaner', cfg["TR_ARBEJDSTIDSPLANER"]),
                ('Tjenesetid', cfg["TR_TJENESTETID"]),
            ]
            self.sd_arbtid = OrderedDict(sd_arbtid)
            self.arbtid_by_uuid = {v: k for k, v in self.sd_arbtid.items()}

        except Exception as e:
            logger.exception("SD Levels are missing")
            raise

        if from_date:
            self._update_virkning(from_date)



    def _init_amqp_comm(self):
        self.exchange_name = 'org-struktur-changes-topic'
        credentials = pika.PlainCredentials(self.amqp_user, self.amqp_password)
        # parameters = pika.ConnectionParameters(host='msg-amqp.silkeborgdata.dk',
        #                                       port=5672,
        parameters = pika.ConnectionParameters(host=self.amqp_host,
                                               port=self.amqp_port,
                                               virtual_host=self.virtual_host,
                                               credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            consumer_callback=self.on_response,
        )
            #auto_ack=True

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

    def read_department(self, unit_code=None, unit_uuid=None, unit_level=None):
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
            params['DepartmentIdentifier'] = unit_code
        if unit_uuid:
            params['DepartmentUUIDIdentifier'] = unit_uuid
        if unit_level:
            params['DepartmentLevelIdentifier'] = unit_level
        logger.debug('Read department, params: {}'.format(params))
        department = self.sd.lookup('GetDepartment20111201', params)
        department_info = department.get('Department', None)
        # try:
        #     department = sd_lookup('GetDepartment20111201', params)
        #     department_info = department.get('Department', None)
        # except KeyError:
        #     # Bug in SD soap-interface, most likely these units actually do exist
        #     # department_info = 'Unknown department'
        #     department_info = None

        if isinstance(department_info, list):
            msg = 'Unit not unique. Code {}, uuid {}, level {}'.format(
                unit_code, unit_uuid, unit_level
            )
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
        :param parent: uuid for parent unit.
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

    def _validate_unit_code(self, unit_code, unit_level=None):
        logger.info('Validating unit code {}'.format(unit_code))
        code_errors = []
        if len(unit_code) < 2:
            code_errors.append('Enhedskode for kort')
        if len(unit_code) > 4:
            code_errors.append('Enhedskode for lang')
        if not unit_code.isalnum():
            code_errors.append('Ugyldigt tegn i enhedskode')

        if not code_errors:
            # customers expect unique unit_codes globally
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

    def _mo_to_sd_address(self, address):
        if address == None:
            return None
        street, zip_code, city = address.rsplit(' ', maxsplit=2)
        if street.endswith(","):
            street = street[:-1]
        sd_address = {
            'silkdata:AdresseNavn': street.strip(),
            'silkdata:PostKodeIdentifikator': zip_code.strip(),
            'silkdata:ByNavn': city.strip()
        }
        return sd_address

    # TODO: Consider to also fold unit into a dict.
    def create_unit(self, name, unit_code, parent, unit_level, unit_uuid=None,
                    test_run=True):
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
        parent_department = self.read_department(unit_code=parent['unit_code'],
                                                 unit_level=parent['level'])
        if not parent_department:
            raise sdMoxException('Forældrenheden finds ikke')

        unit_index = list(self.sd_levels.keys()).index(unit_level)
        parent_index = list(self.sd_levels.keys()).index(
            parent_department['DepartmentLevelIdentifier']
        )

        if not unit_index > parent_index:
            raise sdMoxException('Enhedstypen passer ikke til forældreenheden')

        xml = self._create_xml_import(
            name=name,
            unit_uuid=unit_uuid,
            unit_code=unit_code,
            unit_level=unit_level,
            parent=parent['uuid']
        )
        logger.debug('Create unit xml: {}'.format(xml))
        if not test_run:
            print('Calling amqp')
            logger.info('Create unit {}, {}, {}'.format(name, unit_code, unit_uuid))
            self.call(xml)
        return unit_uuid

    def edit_unit(self, unit_uuid, phone=None, pnummer=None, address=None,
                  integration_values=None):
        print('I want to edit:')
        print('Phone: {}'.format(phone))
        print('pnummer: {}'.format(pnummer))
        print()
        print('address: {}'.format(address))
        print()
        print('Integration values: {}'.format(integration_values))

    def payload_create(self, unit_uuid, unit, parent):
        unit_level = self.level_by_uuid.get(unit["org_unit_type"]["uuid"])
        if not unit_level:
            raise KeyError("Enhedstype er ikke et kendt NY-niveau")

        parent_level = self.level_by_uuid.get(parent["org_unit_type"]["uuid"])
        if not parent_level:
            raise KeyError("Enhedstype er ikke et kendt NY-niveau")

        return {
            "name": unit["name"],
            "parent":{
                "unit_code": parent['user_key'],
                "uuid": parent['uuid'],
                "level": parent_level
            },
            "unit_code": unit['user_key'],
            "unit_level": unit_level,
            "unit_uuid": unit_uuid,
        }

    def grouped_addresses(self, details):
        keyed, scoped = {}, {}
        for d in details:
            scope, key = d["address_type"]["scope"], d["address_type"]["user_key"]
            if scope == "DAR":
                scoped.setdefault(scope,[]).append(d["name"])
            else:
                scoped.setdefault(scope,[]).append(d["value"])
            keyed.setdefault(key,[]).append(d["value"])
        return scoped, keyed


    def payload_edit(self, unit_uuid, unit, addresses):

        scoped, keyed = self.grouped_addresses(addresses)
        return {
            "unit_uuid": unit_uuid,
            "phone" : scoped.get("PHONE", [None])[0],
            "pnummer": scoped.get("PNUMBER", [None])[0],
            "address": self._mo_to_sd_address(scoped.get("DAR", [None])[0]),
            "integration_values": {
                'time_planning': unit.get('time_planning', None),
                'formaalskode': keyed.get("Formålskode", [None])[0],
                'skolekode': keyed.get("Skolekode", [None])[0],
            }
        }

    def create_unit_from_mo(self, unit_uuid, test_run=True):

        # TODO: This url is hard-codet
        from os2mo_data_import.os2mo_helpers.mora_helpers import MoraHelper
        self.mh = MoraHelper(hostname='http://localhost:5000')

        logger.info('Create {} from MO, test run: {}'.format(unit_uuid, test_run))
        unit_info = mox.mh.read_ou(unit_uuid)
        logger.debug('Unit info: {}'.format(unit_info))

        from_date = datetime.datetime.strptime(
            unit_info['validity']['from'], '%Y-%m-%d'
        )
        self._update_virkning(from_date)

        unit_create_payload = self.payload_create(
            unit_uuid,
            unit_info,
            unit_info["parent"]
        )

        try:
            uuid = self.create_unit(test_run=test_run, **unit_create_payload)
            if test_run:
                logger.info('dry-run succeeded: {}'.format(uuid))
            else:
                logger.info('amqp-call succeeded: {}'.format(uuid))
        except sdMoxException as e:
            print('Error: {}'.format(e))
            msg = 'Test for unit {} failed: {}'.format(unit_uuid, e)
            if test_run:
                logger.info(msg)
            else:
                logger.error(msg)
            return False

        integration_addresses = self.mh._mo_lookup( unit_uuid, 'ou/{}/details/address')
        unit_edit_payload = self.payload_edit(
            unit_uuid,
            unit_info,
            integration_addresses
        )

        if not test_run:
            # Running test-run for this edit is a bit more tricky
            self.edit_unit(**unit_edit_payload)
        return True


if __name__ == '__main__':
    from_date = datetime.datetime(2019, 7, 1, 0, 0)
    # to_date = datetime.datetime(2020, 6, 1, 0, 0)

    mox = sdMox(from_date)

    # unit_uuid = '32d9b4ed-eff2-4fa9-a372-c697eed2a597'
    # print(mox.create_unit_from_mo(unit_uuid, test_run=False))

    unit_code = '06GÅ'
    unit_level = 'Afdelings-niveau'
    parent = {
        'unit_code': '32D9',
        'uuid': '32d9b4ed-eff2-4fa9-a372-c697eed2a597',
        'level': 'NY2-niveau'
    }

    # unit_uuid = mox.create_unit(
    #     # unit_uuid=unit_uuid,
    #     name='Daw dav',
    #     unit_code=unit_code,
    #     unit_level=unit_level,
    #     parent=parent,
    #     test_run=False
    # )
    # print(unit_uuid)

    # time.sleep(2)

    # Der er noget galt, vi finder ikke enheder som helt sikkert findes.
    department = mox.read_department(unit_code=unit_code, unit_level=unit_level)

    unit_uuid = '31b43f5d-d8e8-4bd2-8420-a41148ca229f'
    unit_name = 'Daw dav'
    if department:
        errors = mox._check_department(department, unit_name, unit_code, unit_uuid)
        print(errors)
    else:
        print('Department does not exist')

    # if False:
    #     xml = mox.edit_unit(
    #         uuid=uuid,
    #         name='Test 2'
    #     )


# TODO: Soon we are ready to write small tests to verify expected output
# from xml-producing functions.
