# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import datetime
import unittest
import freezegun
import xmltodict
import pprint
from integrations.SD_Lon import sd_mox
from triggers import sd_mox_trigger

xml_create = """<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:silkdata="urn:oio:silkdata:1.0.0" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:oio="urn:oio:definitions:1.0.0" xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd" xmlns:sd="urn:oio:sagdok:3.0.0"><RelationListe><sd:Overordnet><sd:ReferenceID><sd:UUIDIdentifikator>12345-11-11-11-12345</sd:UUIDIdentifikator></sd:ReferenceID><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></sd:Overordnet></RelationListe><ObjektID><sd:UUIDIdentifikator>12345-22-22-22-12345</sd:UUIDIdentifikator><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType></ObjektID><AttributListe><sd:LokalUdvidelse><silkdata:Integration><silkdata:AttributNavn>EnhedKode</silkdata:AttributNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>user-key-22222</silkdata:AttributVaerdi></silkdata:Integration><silkdata:Integration><silkdata:AttributNavn>Niveau</silkdata:AttributNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>Afdelings-niveau</silkdata:AttributVaerdi></silkdata:Integration></sd:LokalUdvidelse><Egenskab><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><sd:EnhedNavn>A-sdm2</sd:EnhedNavn></Egenskab></AttributListe><Registrering><TilstandListe><orgfaelles:Gyldighed><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></orgfaelles:Gyldighed></TilstandListe><sd:FraTidspunkt><sd:TidsstempelDatoTid>2020-01-01T12:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:LivscyklusKode>Opstaaet</sd:LivscyklusKode></Registrering></RegistreringBesked>"""  # noqa

xml_edit_simple="""<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:sd="urn:oio:sagdok:3.0.0" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:oio="urn:oio:definitions:1.0.0" xmlns:silkdata="urn:oio:silkdata:1.0.0" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd"><Registrering><sd:LivscyklusKode>Rettet</sd:LivscyklusKode><TilstandListe><orgfaelles:Gyldighed><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode></orgfaelles:Gyldighed></TilstandListe><sd:FraTidspunkt><sd:TidsstempelDatoTid>2020-01-01T12:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></Registrering><AttributListe><Egenskab><sd:EnhedNavn>A-sdm2</sd:EnhedNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></Egenskab><sd:LokalUdvidelse><silkdata:Integration><silkdata:AttributVaerdi>32201</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributNavn>FunktionKode</silkdata:AttributNavn></silkdata:Integration><silkdata:Integration><silkdata:AttributVaerdi>12347</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributNavn>SkoleKode</silkdata:AttributNavn></silkdata:Integration><silkdata:Integration><silkdata:AttributVaerdi>Arbejdstidsplaner</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributNavn>Tidsregistrering</silkdata:AttributNavn></silkdata:Integration></sd:LokalUdvidelse></AttributListe><ObjektID><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType><sd:UUIDIdentifikator>12345-22-22-22-12345</sd:UUIDIdentifikator></ObjektID><RelationListe><sd:LokalUdvidelse><silkdata:Lokation></silkdata:Lokation></sd:LokalUdvidelse></RelationListe></RegistreringBesked>""" # noqa

xml_edit_address="""<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:silkdata="urn:oio:silkdata:1.0.0" xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xmlns:oio="urn:oio:definitions:1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:sd="urn:oio:sagdok:3.0.0" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0"><ObjektID><sd:UUIDIdentifikator>12345-22-22-22-12345</sd:UUIDIdentifikator><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType></ObjektID><RelationListe><sd:LokalUdvidelse><silkdata:Lokation><silkdata:ProduktionEnhed><silkdata:ProduktionEnhedIdentifikator>0123456789</silkdata:ProduktionEnhedIdentifikator><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></silkdata:ProduktionEnhed><silkdata:DanskAdresse><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AdresseNavn>Toftevej 2</silkdata:AdresseNavn><silkdata:ByNavn>Vold</silkdata:ByNavn><silkdata:PostKodeIdentifikator>10000</silkdata:PostKodeIdentifikator></silkdata:DanskAdresse><silkdata:Kontakt><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:LokalTelefonnummerIdentifikator>12345678</silkdata:LokalTelefonnummerIdentifikator></silkdata:Kontakt></silkdata:Lokation></sd:LokalUdvidelse></RelationListe><Registrering><sd:LivscyklusKode>Rettet</sd:LivscyklusKode><sd:FraTidspunkt><sd:TidsstempelDatoTid>2020-01-01T12:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><TilstandListe><orgfaelles:Gyldighed><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></orgfaelles:Gyldighed></TilstandListe></Registrering><AttributListe><Egenskab><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><sd:EnhedNavn>A-sdm2</sd:EnhedNavn></Egenskab><sd:LokalUdvidelse><silkdata:Integration><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>32201</silkdata:AttributVaerdi><silkdata:AttributNavn>FunktionKode</silkdata:AttributNavn></silkdata:Integration><silkdata:Integration><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>12347</silkdata:AttributVaerdi><silkdata:AttributNavn>SkoleKode</silkdata:AttributNavn></silkdata:Integration><silkdata:Integration><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>Arbejdstidsplaner</silkdata:AttributVaerdi><silkdata:AttributNavn>Tidsregistrering</silkdata:AttributNavn></silkdata:Integration></sd:LokalUdvidelse></AttributListe></RegistreringBesked>""" # noqa

xml_edit_integration_values="""<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:sd="urn:oio:sagdok:3.0.0" xmlns:oio="urn:oio:definitions:1.0.0" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns:silkdata="urn:oio:silkdata:1.0.0" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/"><Registrering><sd:LivscyklusKode>Rettet</sd:LivscyklusKode><sd:FraTidspunkt><sd:TidsstempelDatoTid>2020-01-01T12:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><TilstandListe><orgfaelles:Gyldighed><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></orgfaelles:Gyldighed></TilstandListe></Registrering><ObjektID><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType><sd:UUIDIdentifikator>12345-33-33-33-12345</sd:UUIDIdentifikator></ObjektID><RelationListe><sd:LokalUdvidelse><silkdata:Lokation></silkdata:Lokation></sd:LokalUdvidelse></RelationListe><AttributListe><Egenskab><sd:EnhedNavn>A-sdm3</sd:EnhedNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></Egenskab><sd:LokalUdvidelse><silkdata:Integration><silkdata:AttributVaerdi>32201</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributNavn>FunktionKode</silkdata:AttributNavn></silkdata:Integration><silkdata:Integration><silkdata:AttributVaerdi>12347</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributNavn>SkoleKode</silkdata:AttributNavn></silkdata:Integration><silkdata:Integration><silkdata:AttributVaerdi>Arbejdstidsplaner</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributNavn>Tidsregistrering</silkdata:AttributNavn></silkdata:Integration></sd:LokalUdvidelse></AttributListe></RegistreringBesked>""" # noqa

unit_parent = {
    "name": "A-sdm1",
    "uuid": "12345-11-11-11-12345",
    "org_unit_type": {"uuid": "uuid-a"},
    "user_key": "user-key-11111",
}


mox_cfg = {
    "TR_DANNES_IKKE": "uuid-tr-dannes-ikke",
    "TR_ARBEJDSTIDSPLANER": "uuid-tr-arbejdstidsplaner",
    "TR_TJENESTETID": "uuid-tr-tjenestetid",
    "AMQP_USER": "example",
    "AMQP_HOST": "example.com",
    "AMQP_PORT": 2222,
    "VIRTUAL_HOST": "example.com",
    "AMQP_PASSWORD": "",
    "NY6_NIVEAU": "6",
    "NY5_NIVEAU": "5",
    "NY4_NIVEAU": "4",
    "NY3_NIVEAU": "3",
    "NY2_NIVEAU": "2",
    "NY1_NIVEAU": "1",
    "AFDELINGS_NIVEAU": "uuid-a",
    "sd_common": {
        "SD_USER": "",
        "SD_PASSWORD": "",
        "BASE_URL": "",
        "INSTITUTION_IDENTIFIER":"",
    }
}


class Tests(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        from_date = datetime.datetime(2019, 7, 1, 0, 0)
        self.mox = sd_mox.sdMox(from_date, **mox_cfg)

    def test_payload_create(self):
        pc = self.mox.payload_create(
            unit_uuid="12345-22-22-22-12345",
            unit={
                "name": "A-sdm2",
                #"uuid": "12345-22-22-22-12345",
                "org_unit_type": {"uuid": "uuid-a"},
                "user_key": "user-key-22222",
            },
            parent=unit_parent
        )

        self.assertEqual({
            'name': 'A-sdm2',
            'parent': {'level': 'Afdelings-niveau',
                       'unit_code': 'user-key-11111',
                       'uuid': '12345-11-11-11-12345'},
            'unit_code': 'user-key-22222',
            'unit_level': 'Afdelings-niveau',
            'unit_uuid': '12345-22-22-22-12345'
        },  pc)

        expected = xmltodict.parse(xml_create, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_import(
                name=pc["name"],
                unit_uuid=pc["unit_uuid"],
                unit_code=pc["unit_code"],
                unit_level=pc["unit_level"],
                parent=pc["parent"]['uuid']
            )
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))


    def test_payload_edit_simple(self):
        pe = self.mox.payload_edit(
            unit_uuid="12345-22-22-22-12345",
            unit={
                "name": "A-sdm2",
                "org_unit_type": {"uuid": "uuid-a"},
                "user_key": "user-key-22222",
            },
            addresses=[]
        )

        self.assertEqual({
            'name': 'A-sdm2',
            'unit_code': 'user-key-22222',
            'phone': None,
            'adresse': None,
            'pnummer': None,
            'integration_values': {'formaalskode': None,
                                   'skolekode': None,
                                   'time_planning': None},
            'unit_uuid': '12345-22-22-22-12345'
        },  pe)


        expected = xmltodict.parse(xml_edit_simple, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_ret(**pe)
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))


    def test_payload_edit_address(self):
        pe = self.mox.payload_edit(
            unit_uuid="12345-22-22-22-12345",
            unit={
                "name": "A-sdm2",
                "org_unit_type": {"uuid": "uuid-a"},
                "user_key": "user-key-22222",
            },
            addresses=[
                {"address_type": {"scope": "DAR", "user_key":"dar-userkey-not-used"},
                                  "name":"Toftevej 2, 10000 Vold", "value":"dar-value-not-used"},
                {"address_type": {"scope": "PHONE", "user_key":"phone-user-key-not-used" },
                                  "value":"12345678"},
                {"address_type": {"scope": "PNUMBER", "user_key":"pnummer-user-key-not-used" },
                                  "value":"0123456789"},
            ]
        )

        self.assertEqual({
            'name': 'A-sdm2',
            'unit_code': 'user-key-22222',
            'phone': "12345678",
            'adresse': {'silkdata:AdresseNavn': 'Toftevej 2',
                        'silkdata:ByNavn': 'Vold',
                        'silkdata:PostKodeIdentifikator': '10000'},
            'pnummer': "0123456789",
            'integration_values': {'formaalskode': None,
                                   'skolekode': None,
                                   'time_planning': None},
            'unit_uuid': '12345-22-22-22-12345'
        },  pe)

        expected = xmltodict.parse(xml_edit_address, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_ret(**pe)
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))




    def test_payload_edit_integration_values(self):
        pe = self.mox.payload_edit(
            unit_uuid="12345-33-33-33-12345",
            unit={
                "name": "A-sdm3",
                "org_unit_type": {"uuid": "uuid-a"},
                "user_key": "user-key-33333",
                "time_planning": "uuid-tr-arbejdstidsplaner",
            },
            addresses=[
                {"address_type": {"scope": "TEXT", "user_key":"Formålskode"},
                                  "name":"fkode-name-not-used", "value":"Formål1"},
                {"address_type": {"scope": "TEXT", "user_key":"Skolekode" },
                                  "value":"Skole1"},
            ]
        )

        self.assertEqual({
            'name': 'A-sdm3',
            'unit_code': 'user-key-33333',
            'phone': None,
            'adresse': None,
            'pnummer': None,
            'integration_values': {'formaalskode': "Formål1",
                                   'skolekode': "Skole1",
                                   'time_planning': "Arbejdstidsplaner"},
            'unit_uuid': '12345-33-33-33-12345'
        },  pe)

        expected = xmltodict.parse(xml_edit_integration_values, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_ret(**pe)
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))


