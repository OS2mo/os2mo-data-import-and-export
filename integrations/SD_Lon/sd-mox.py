import os
import pika
import datetime
import xmltodict
import sd_mox_payloads as smp

from os2mo_helpers.mora_helpers import MoraHelper

AMQP_USER = os.environ.get('AMQP_USER')
AMQP_PASSWORD = os.environ.get('AMQP_PASSWORD', None)
VIRTUAL_HOST = os.environ.get('VIRTUAL_HOST', None)
if not (AMQP_USER and AMQP_PASSWORD and VIRTUAL_HOST):
    raise Exception('Credentials missing')


class sdMox(object):
    def __init__(self):

        self.exchange_name = 'org-struktur-changes-topic'
        credentials = pika.PlainCredentials(AMQP_USER, AMQP_PASSWORD)
        parameters = pika.ConnectionParameters(host='msg-amqp.silkeborgdata.dk',
                                               port=5672,
                                               virtual_host=VIRTUAL_HOST,
                                               credentials=credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.mh = MoraHelper(hostname='localhost:5000')

        self.from_date = datetime.datetime(2020, 5, 1, 0, 0)
        to_date = datetime.datetime(2020, 6, 1, 0, 0)
        self.virkning = smp.sd_virkning(self.from_date, to_date)
        self.xml = None

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def create_registrering(self, registry_type):
        assert registry_type in ('Rettet', 'Opstaaet')
        now = datetime.datetime.strftime(
            datetime.datetime.now(),
            '%Y-%m-%dT%H:%M:%S.00'
        )
        registrering = {
            "sd:FraTidspunkt": {
                "sd:TidsstempelDatoTid": now
            },
            'sd:LivscyklusKode': registry_type,
            'TilstandListe': {
                "orgfaelles:Gyldighed": {
                    "sd:Virkning": self.virkning,
                    "orgfaelles:GyldighedStatusKode": "Aktiv"
                },
                "sd:LokalUdvidelse": None
            },
        }
        return registrering

    def create_xml_import(self, uuid, unit_code, parent):
        value_dict = {
            'RelationListe': smp.create_relations_import(self.virkning, parent),
            'AttributListe': smp.create_attribut_liste_import(
                self.virkning,
                unit_code=unit_code,
                unit_name='Klaf',
                niveau='TODO'
            ),
            'Registrering': self.create_registrering(registry_type='Opstaaet'),
            'ObjektID': smp.create_objekt_id(uuid)
        }
        edit_dict = {'RegistreringBesked': value_dict}
        edit_dict['RegistreringBesked'].update(smp.boilerplate)
        self.xml = xmltodict.unparse(edit_dict)

    def create_xml_ret(self, uuid, name):
        value_dict = {
            'RelationListe': smp.create_relations_ret(
                self.virkning,
                pnummer='1003407739',
                phone='995666655',
                adresse={
                    'silkdata:AdresseNavn': 'Arnegaard 799',
                    'silkdata:PostKodeIdentifikator': '2200',
                    'silkdata:ByNavn': 'Fd'
                }
            ),
            'AttributListe': smp.create_attribut_liste_ret(self.virkning,
                                                           unit_name=name),
            'Registrering': self.create_registrering(registry_type='Rettet'),
            'ObjektID': smp.create_objekt_id(uuid)
        }
        edit_dict = {'RegistreringBesked': value_dict}
        edit_dict['RegistreringBesked'].update(smp.boilerplate)
        self.xml = xmltodict.unparse(edit_dict)

    def on_response(self, ch, method, props, body):
        print('Response?!??!?!')
        print(body)

    def call(self):
        print(self.xml)

        self.response = None
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key='#',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue
            ),
            body=self.xml
        )

        # Todo: We should to a lookup at verify actual unit
        # matches the expected result
        return True


mox = sdMox()
print('Send request')

uuid = '07783071-0000-0007-9200-000001550002',

if True:
    mox.create_xml_import(
        uuid=uuid,
        unit_code='LLBI',
        parent='fd47d033-61c0-4900-b000-000001520002'
    )

if False:
    mox.create_xml_ret(
        uuid=uuid,
        name='Test 2'
    )

response = mox.call()
print('-----------------------')
print(response)


# TODO: Soon we are ready to write small tests to verify expected output
# from xml-producing functions.
