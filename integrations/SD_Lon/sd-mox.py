import os
import time
import datetime
import xmltodict
import sd_mox_payloads

AMQP_USER = os.environ.get('AMQP_USER')
AMQP_PASSWORD = os.environ.get('AMQP_PASSWORD', None)
VIRTUAL_HOST = os.environ.get('VIRTUAL_HOST', None)
if not (AMQP_USER and AMQP_PASSWORD and VIRTUAL_HOST):
    raise Exception('Credentials missing')

from_date = datetime.datetime(2020, 2, 1, 0, 0)
to_date = datetime.datetime(2020, 3, 1, 0, 0)
now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%S.00')
virkning = sd_mox_payloads.sd_virkning(from_date, to_date)

print(virkning)

edit_dict = {
    "RegistreringBesked": {
    }
}

objekt_id = {
    'sd:UUIDIdentifikator': '89ad7a4c-61c0-4900-9200-000001550002',
    'sd:IdentifikatorType': 'OrganisationEnhed'
}

relations_liste = {
    "sd:LokalUdvidelse": {
        "silkdata:Lokation": {
            "silkdata:ProduktionEnhed": {
                "sd:Virkning": virkning,
                "silkdata:ProduktionEnhedIdentifikator": "1011600936"
            },
            "silkdata:Kontakt": {
                "sd:Virkning": virkning,
                "silkdata:LokalTelefonnummerIdentifikator": "89892540"
            },
            "silkdata:DanskAdresse": {
                "sd:Virkning": virkning,
                "silkdata:AdresseNavn": "Arnegaardsvej 5",
                "silkdata:PostKodeIdentifikator": "8600",
                "silkdata:ByNavn": "Silkeborg"
            }
        }
    }
}

registrering = {
    "sd:FraTidspunkt": {
        "sd:TidsstempelDatoTid": now
    },
    "sd:LivscyklusKode": "Rettet",
    "sd:BrugerRef": {
        "sd:UUIDIdentifikator": "3bb66b0d-132d-4b98-a903-ea29f6552d53",
        "sd:IdentifikatorType": "AD"
    },
    "TilstandListe": {
        "orgfaelles:Gyldighed": {
            "sd:Virkning": virkning,
            "orgfaelles:GyldighedStatusKode": "Aktiv"
        },
        "sd:LokalUdvidelse": None
    },
}

attribut_liste = {
    "Egenskab": {
        "sd:EnhedNavn": 'PPPPP',
        "sd:Virkning": virkning
    },
    "sd:LokalUdvidelse": {
        "silkdata:Integration": [
            {
                "sd:Virkning": virkning,
                "silkdata:AttributNavn": "FunktionKode",
                "silkdata:AttributVaerdi": "32201"
            },
            {
                "sd:Virkning": virkning,
                "silkdata:AttributNavn": "SkoleKode",
                "silkdata:AttributVaerdi": "12346"
            },
            {
                "sd:Virkning": virkning,
                "silkdata:AttributNavn": "Tidsregistrering",
                "silkdata:AttributVaerdi": "Arbejdstidsplaner"
            }
        ]
    }
}


edit_dict['RegistreringBesked'].update(sd_mox_payloads.boilerplate)
edit_dict['RegistreringBesked']['RelationListe'] = relations_liste
edit_dict['RegistreringBesked']['AttributListe'] = attribut_liste
edit_dict['RegistreringBesked']['Registrering'] = registrering
edit_dict['RegistreringBesked']['ObjektID'] = objekt_id

XML = xmltodict.unparse(edit_dict)

import pika
import uuid

class sdMox(object):
    def __init__(self):
        
        self.exchange_name = 'org-struktur-changes-topic'
        credentials =  pika.PlainCredentials(AMQP_USER, AMQP_PASSWORD)
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
            body=XML
        )

        while self.response is None:
            print(self.response)
            self.connection.process_data_events()
            time.sleep(0.1)

            return self.response # NOTICE. Return is here to temporary prevent endless loop

        return self.response


mox = sdMox()
print('Send request')
response = mox.call()
print('-----------------------')
print(response)
