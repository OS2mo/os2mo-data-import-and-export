import os
import time
import pika
import uuid
import datetime
import xmltodict
import sd_mox_payloads

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

        self.from_date = datetime.datetime(2010, 5, 1, 0, 0)
        to_date = datetime.datetime(2040, 6, 1, 0, 0)
        self.virkning = sd_mox_payloads.sd_virkning(self.from_date, to_date)
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

    def create_objekt_id(self, unit):
        objekt_id = {
            'sd:UUIDIdentifikator': unit,
            'sd:IdentifikatorType': 'OrganisationEnhed'
        }
        return objekt_id

    def create_attribut_liste(self, unit, unit_name=None):
        # print(self.mh.read_ou(unit)) #This will not work for test-data
        if not unit_name:
            pass
        attribut_liste = {
            "Egenskab": {
                "sd:EnhedNavn": 'UUUUUUUUUUUUU',
                "sd:Virkning": self.virkning
            },
            "sd:LokalUdvidelse": {
                "silkdata:Integration": [
                    {
                        "sd:Virkning": self.virkning,
                        "silkdata:AttributNavn": 'EnhedKode',
                        "silkdata:AttributVaerdi": "KLAF"
                    },
                    {
                        "sd:Virkning": self.virkning,
                        "silkdata:AttributNavn": 'Niveau',
                        "silkdata:AttributVaerdi": "Afdelings-niveau"
                    }#,
                    #{
                    #    "sd:Virkning": self.virkning,
                    #    "silkdata:AttributNavn": "FunktionKode",
                    #    "silkdata:AttributVaerdi": "32201"
                    #},
                    #{
                    #    "sd:Virkning": self.virkning,
                    #    "silkdata:AttributNavn": "SkoleKode",
                    #    "silkdata:AttributVaerdi": "12346"
                    #},
                    #{
                    #    "sd:Virkning": self.virkning,
                    #    "silkdata:AttributNavn": "Tidsregistrering",
                    #    "silkdata:AttributVaerdi": "Arbejdstidsplaner"
                    #}
                ]
            }
        }
        return attribut_liste

    def create_relations_liste(self, pnummer=None, tlf=None, parent=None):
        """
        relations_liste = {
            'sd:LokalUdvidelse': {
                'silkdata:Lokation': {
                    "silkdata:DanskAdresse": {
                        "sd:Virkning": self.virkning,
                        "silkdata:AdresseNavn": "Arnegaardsvej 5",
                        "silkdata:PostKodeIdentifikator": "8600",
                        "silkdata:ByNavn": "Silkeborg"
                    }
                }
            }
        }
        """
        relations_liste = {
            'sd:Overordnet': {
                'sd:Virkning': self.virkning,
                'sd:ReferenceID': {
                    'sd:UUIDIdentifikator': parent
                }
            },
            'sd:ReferenceID': {
                'sd:UUIDIdentifikator': '2235de61-1e61-4c23-847d-b9ed71829ec9'
            },
            'sd:LokalUdvidelse': {}
        }

        if pnummer is not None:
            relations_liste['sd:LokalUdvidelse']['silkdata:Lokation'] = {
                'silkdata:ProduktionEnhed': {
                    'sd:Virkning': self.virkning,
                    'silkdata:ProduktionEnhedIdentifikator': pnummer
                }
            }
        if tlf is not None:
            relations_liste['sd:LokalUdvidelse']['silkdata:Lokation'] = {
                'silkdata:Kontakt': {
                    'sd:Virkning': self.virkning,
                    'silkdata:LokalTelefonnummerIdentifikator': tlf
                }
            }
        return relations_liste

    def create_xml(self, unit, create=False, parent=None):
        edit_dict = {'RegistreringBesked': {}}
        edit_dict['RegistreringBesked'].update(sd_mox_payloads.boilerplate)
        edit_dict['RegistreringBesked']['RelationListe'] = self.create_relations_liste(parent=parent)
        edit_dict['RegistreringBesked']['AttributListe'] = self.create_attribut_liste(unit)
        if create:
            edit_dict['RegistreringBesked']['Registrering'] = self.create_registrering(registry_type='Opstaaet')
        else:
            edit_dict['RegistreringBesked']['Registrering'] = self.create_registrering(registry_type='Rettet')
        edit_dict['RegistreringBesked']['ObjektID'] = self.create_objekt_id(unit)
        self.xml = xmltodict.unparse(edit_dict)

    def on_response(self, ch, method, props, body):
        print('Response:')
        print('Body: {}'.format(body))
        print('Corr id: {}'.format(self.corr_id))
        print('props: {}'.format(props))
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key='#',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=self.xml
        )

        while self.response is None:
            print(self.response)
            self.connection.process_data_events()
            time.sleep(0.1)

            return self.response # NOTICE. Return is here to temporary prevent endless loop

        return self.response


mox = sdMox()
print('Send request')

mox.create_xml(
    # unit='89ad7a4c-61c0-4900-9200-000001550002'
    unit='00000000-00c0-4900-9200-000001550002',
    create=True,
    parent='fd47d033-61c0-4900-b000-000001520002'
)

response = mox.call()
print('-----------------------')
print(response)
