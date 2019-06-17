import os
import pika
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

        self.from_date = datetime.datetime(2020, 4, 1, 0, 0)
        to_date = datetime.datetime(2020, 5, 1, 0, 0)
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

    def create_attribut_liste_ret(self, unit_uuid=None, unit_name='Klaf',
                                  unit_code=None):
        # print(self.mh.read_ou(unit_uuid)) #This will not work for test-data
        attribut_liste = {
            "sd:LokalUdvidelse": {
                "silkdata:Integration": [
                    {
                        "sd:Virkning": self.virkning,
                        "silkdata:AttributNavn": 'EnhedKode',
                        "silkdata:AttributVaerdi": unit_code
                    },
                    {
                        "sd:Virkning": self.virkning,
                        "silkdata:AttributNavn": 'Niveau',
                        "silkdata:AttributVaerdi": "Afdelings-niveau"
                    },
                    {
                        "sd:Virkning": self.virkning,
                        "silkdata:AttributNavn": "FunktionKode",
                        "silkdata:AttributVaerdi": "32201"
                    },
                    {
                        "sd:Virkning": self.virkning,
                        "silkdata:AttributNavn": "SkoleKode",
                        "silkdata:AttributVaerdi": "12346"
                    },
                    {
                        "sd:Virkning": self.virkning,
                        "silkdata:AttributNavn": "Tidsregistrering",
                        "silkdata:AttributVaerdi": "Arbejdstidsplaner"
                    }
                ]
            }
        }
        if unit_name:
            attribut_liste['Egenskab'] = {
                "sd:EnhedNavn": unit_name,
                "sd:Virkning": self.virkning
            }
        return attribut_liste

    def _create_attribut_items(self, attributes):
        attribute_items = []
        for key, value in attributes.items():
            attribute_items.append(
                {
                    'sd:Virkning': self.virkning,
                    'silkdata:AttributNavn': key,
                    'silkdata:AttributVaerdi': value
                }
            )
        return attribute_items

    def create_attribut_liste_import(self, unit_name, unit_code, niveau):
        attributes = {'EnhedKode': unit_code, 'Niveau': 'Afdelings-niveau'}
        integration_items = self._create_attribut_items(attributes)
        attribut_liste = {
            "sd:LokalUdvidelse": {
                "silkdata:Integration": integration_items
            },
            'Egenskab': {
                "sd:EnhedNavn": unit_name,
                "sd:Virkning": self.virkning
            }
        }
        return attribut_liste

    def create_relations_liste_import(self, parent=None):
        relations_liste = {
            'sd:Overordnet': {
                'sd:Virkning': self.virkning,
                'sd:ReferenceID': {
                    'sd:UUIDIdentifikator': parent
                }
            },
            'sd:LokalUdvidelse': {}
        }
        return relations_liste

    def create_relations_liste_ret(self, pnummer=None, phone=None, adresse=None):
        adresse = {
            'sd:Virkning': self.virkning,
            'silkdata:AdresseNavn': 'Arnegaard 99',
            'silkdata:PostKodeIdentifikator': '2000',
            'silkdata:ByNavn': 'Fr'
        }
        
        relations_liste = {
            'sd:LokalUdvidelse': {
                'silkdata:Lokation': {
                        "silkdata:DanskAdresse": adresse
                }
            }
        }
        
        if pnummer is not None:
            relations_liste['sd:LokalUdvidelse']['silkdata:Lokation']['silkdata:ProduktionEnhed'] = {
                    'sd:Virkning': self.virkning,
                    'silkdata:ProduktionEnhedIdentifikator': pnummer
                }
        if phone is not None:
            relations_liste['sd:LokalUdvidelse']['silkdata:Lokation']['silkdata:Kontakt'] = {
                    'sd:Virkning': self.virkning,
                    'silkdata:LokalTelefonnummerIdentifikator': phone
                }

        import pprint
        pp = pprint.PrettyPrinter(indent=1)
        pp.pprint(relations_liste)
        # 1/0
        return relations_liste

    def create_xml_import(self, uuid, unit_code, parent):
        value_dict = {
            'RelationListe': self.create_relations_liste_import(parent),
            'AttributListe': self.create_attribut_liste_import(
                unit_code=unit_code,
                unit_name='Klaf',
                niveau='TODO'
            ),
            'Registrering': self.create_registrering(registry_type='Opstaaet'),
            'ObjektID': sd_mox_payloads.create_objekt_id(uuid)
        }
        edit_dict = {'RegistreringBesked': value_dict}
        edit_dict['RegistreringBesked'].update(sd_mox_payloads.boilerplate)
        self.xml = xmltodict.unparse(edit_dict)

    def create_xml_ret(self, uuid, name):
        value_dict = {
            'RelationListe': self.create_relations_liste_ret(
                pnummer='1003407739',
                phone='55666655'
            ),
            'AttributListe': self.create_attribut_liste_ret(unit_name=name),
            'Registrering': self.create_registrering(registry_type='Rettet'),
            'ObjektID': sd_mox_payloads.create_objekt_id(uuid)
        }
        edit_dict = {'RegistreringBesked': value_dict}
        edit_dict['RegistreringBesked'].update(sd_mox_payloads.boilerplate)
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

uuid='07783071-0000-4900-9200-000001550002',

if False:
    mox.create_xml_import(
        uuid=uuid,
        unit_code='LLBD',
        parent='fd47d033-61c0-4900-b000-000001520002'
    )

if True:
    mox.create_xml_ret(
        uuid=uuid,
        name='Test 2'
    )

response = mox.call()
print('-----------------------')
print(response)
