# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import datetime
import unittest

import freezegun
import xmltodict
from integrations.SD_Lon import sd_mox

xml_create = """<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:sd="urn:oio:sagdok:3.0.0" xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xmlns:silkdata="urn:oio:silkdata:1.0.0" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:oio="urn:oio:definitions:1.0.0"><ObjektID><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType><sd:UUIDIdentifikator>12345-22-22-22-12345</sd:UUIDIdentifikator></ObjektID><Registrering><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:LivscyklusKode>Opstaaet</sd:LivscyklusKode><sd:BrugerRef><sd:IdentifikatorType>ILTEST</sd:IdentifikatorType><sd:UUIDIdentifikator>3bb66b0d-132d-4b98-a903-ea29f6552mmm</sd:UUIDIdentifikator></sd:BrugerRef><AttributListe><Egenskab><sd:EnhedNavn>A-sdm2</sd:EnhedNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:TilTidspunkt><sd:TidsstempelDatoTid>9999-12-31T00:00:00.00</sd:TidsstempelDatoTid></sd:TilTidspunkt></sd:Virkning></Egenskab><sd:LokalUdvidelse><silkdata:Integration><silkdata:AttributVaerdi>user-key-22222</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:TilTidspunkt><sd:TidsstempelDatoTid>9999-12-31T00:00:00.00</sd:TidsstempelDatoTid></sd:TilTidspunkt></sd:Virkning><silkdata:AttributNavn>EnhedKode</silkdata:AttributNavn></silkdata:Integration><silkdata:Integration><silkdata:AttributVaerdi>Afdelings-niveau</silkdata:AttributVaerdi><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:TilTidspunkt><sd:TidsstempelDatoTid>9999-12-31T00:00:00.00</sd:TidsstempelDatoTid></sd:TilTidspunkt></sd:Virkning><silkdata:AttributNavn>Niveau</silkdata:AttributNavn></silkdata:Integration></sd:LokalUdvidelse></AttributListe><TilstandListe><orgfaelles:Gyldighed><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:TilTidspunkt><sd:TidsstempelDatoTid>9999-12-31T00:00:00.00</sd:TidsstempelDatoTid></sd:TilTidspunkt></sd:Virkning><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode></orgfaelles:Gyldighed></TilstandListe><RelationListe><sd:LokalUdvidelse></sd:LokalUdvidelse><sd:Overordnet><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:TilTidspunkt><sd:TidsstempelDatoTid>9999-12-31T00:00:00.00</sd:TidsstempelDatoTid></sd:TilTidspunkt></sd:Virkning><sd:ReferenceID><sd:UUIDIdentifikator></sd:UUIDIdentifikator></sd:ReferenceID></sd:Overordnet></RelationListe></Registrering></RegistreringBesked>"""  # noqa


xml_edit_simple = """<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns:sd="urn:oio:sagdok:3.0.0" xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:silkdata="urn:oio:silkdata:1.0.0" xmlns:oio="urn:oio:definitions:1.0.0"><ObjektID><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType><sd:UUIDIdentifikator>12345-22-22-22-12345</sd:UUIDIdentifikator></ObjektID><RelationListe><sd:LokalUdvidelse><silkdata:Lokation></silkdata:Lokation></sd:LokalUdvidelse></RelationListe><Registrering><sd:LivscyklusKode>Rettet</sd:LivscyklusKode><TilstandListe><orgfaelles:Gyldighed><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></orgfaelles:Gyldighed></TilstandListe><sd:FraTidspunkt><sd:TidsstempelDatoTid>2020-01-01T12:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></Registrering><AttributListe><sd:LokalUdvidelse></sd:LokalUdvidelse><Egenskab><sd:EnhedNavn>A-sdm2</sd:EnhedNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></Egenskab></AttributListe></RegistreringBesked>"""  # noqa


xml_edit_address = """<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xmlns:oio="urn:oio:definitions:1.0.0" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:sd="urn:oio:sagdok:3.0.0" xmlns:silkdata="urn:oio:silkdata:1.0.0" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><ObjektID><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType><sd:UUIDIdentifikator>12345-22-22-22-12345</sd:UUIDIdentifikator></ObjektID><RelationListe><sd:LokalUdvidelse><silkdata:Lokation><silkdata:DanskAdresse><silkdata:AdresseNavn>Toftebjerghaven 4</silkdata:AdresseNavn><silkdata:ByNavn>Ballerup</silkdata:ByNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:PostKodeIdentifikator>2750</silkdata:PostKodeIdentifikator></silkdata:DanskAdresse><silkdata:Kontakt><silkdata:LokalTelefonnummerIdentifikator>12345678</silkdata:LokalTelefonnummerIdentifikator><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></silkdata:Kontakt><silkdata:ProduktionEnhed><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:ProduktionEnhedIdentifikator>0123456789</silkdata:ProduktionEnhedIdentifikator></silkdata:ProduktionEnhed></silkdata:Lokation></sd:LokalUdvidelse></RelationListe><AttributListe><Egenskab><sd:EnhedNavn>A-sdm2</sd:EnhedNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></Egenskab><sd:LokalUdvidelse></sd:LokalUdvidelse></AttributListe><Registrering><TilstandListe><orgfaelles:Gyldighed><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode></orgfaelles:Gyldighed></TilstandListe><sd:LivscyklusKode>Rettet</sd:LivscyklusKode><sd:FraTidspunkt><sd:TidsstempelDatoTid>2020-01-01T12:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></Registrering></RegistreringBesked>"""  # noqa


xml_edit_integration_values = """<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xmlns:sd="urn:oio:sagdok:3.0.0" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns:oio="urn:oio:definitions:1.0.0" xmlns:silkdata="urn:oio:silkdata:1.0.0" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd urn:oio:silkdata:1.0.0 SDObjekt.xsd" xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Registrering><sd:LivscyklusKode>Rettet</sd:LivscyklusKode><sd:FraTidspunkt><sd:TidsstempelDatoTid>2020-01-01T12:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><TilstandListe><orgfaelles:Gyldighed><orgfaelles:GyldighedStatusKode>Aktiv</orgfaelles:GyldighedStatusKode><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></orgfaelles:Gyldighed></TilstandListe></Registrering><RelationListe><sd:LokalUdvidelse><silkdata:Lokation></silkdata:Lokation></sd:LokalUdvidelse></RelationListe><ObjektID><sd:UUIDIdentifikator>12345-33-33-33-12345</sd:UUIDIdentifikator><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType></ObjektID><AttributListe><sd:LokalUdvidelse><silkdata:Integration><silkdata:AttributNavn>FunktionKode</silkdata:AttributNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>Formål1</silkdata:AttributVaerdi></silkdata:Integration><silkdata:Integration><silkdata:AttributNavn>SkoleKode</silkdata:AttributNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>Skole1</silkdata:AttributVaerdi></silkdata:Integration><silkdata:Integration><silkdata:AttributNavn>Tidsregistrering</silkdata:AttributNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning><silkdata:AttributVaerdi>Arbejdstidsplaner</silkdata:AttributVaerdi></silkdata:Integration></sd:LokalUdvidelse><Egenskab><sd:EnhedNavn>A-sdm3</sd:EnhedNavn><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt></sd:Virkning></Egenskab></AttributListe></RegistreringBesked>"""  # noqa


xml_move = """<?xml version="1.0" encoding="utf-8"?>
<RegistreringBesked xsi:schemaLocation="urn:oio:sagdok:organisation:organisationenhed:2.0.0 OrganisationEnhedRegistrering.xsd" xmlns:cvr="http://rep.oio.dk/cvr.dk/xml/schemas/2005/03/22/" xmlns="urn:oio:sagdok:organisation:organisationenhed:2.0.0" xmlns:itst1="http://rep.oio.dk/itst.dk/xml/schemas/2005/06/24/" xmlns:dkcc2="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2005/03/15/" xmlns:sd="urn:oio:sagdok:3.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dkcc1="http://rep.oio.dk/ebxml/xml/schemas/dkcc/2003/02/13/" xmlns:sd20070301="http://rep.oio.dk/sd.dk/xml.schema/20070301/" xmlns:oio="urn:oio:definitions:1.0.0" xmlns:orgfaelles="urn:oio:sagdok:organisation:2.0.0"><ObjektID><sd:UUIDIdentifikator>12345-22-22-22-12345</sd:UUIDIdentifikator><sd:IdentifikatorType>OrganisationEnhed</sd:IdentifikatorType></ObjektID><Registrering><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:LivscyklusKode>Flyttet</sd:LivscyklusKode><sd:BrugerRef><sd:UUIDIdentifikator>3bb66b0d-132d-4b98-a903-ea29f6552d53</sd:UUIDIdentifikator><sd:IdentifikatorType>AD</sd:IdentifikatorType></sd:BrugerRef><AttributListe><sd:LokalUdvidelse></sd:LokalUdvidelse><Egenskab><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:TilTidspunkt><sd:TidsstempelDatoTid>9999-12-31T00:00:00.00</sd:TidsstempelDatoTid></sd:TilTidspunkt></sd:Virkning><sd:EnhedNavn>A-sdm2</sd:EnhedNavn></Egenskab></AttributListe><TilstandListe></TilstandListe><RelationListe><sd:LokalUdvidelse></sd:LokalUdvidelse><sd:Overordnet><sd:Virkning><sd:FraTidspunkt><sd:TidsstempelDatoTid>2019-07-01T00:00:00.00</sd:TidsstempelDatoTid></sd:FraTidspunkt><sd:TilTidspunkt><sd:TidsstempelDatoTid>9999-12-31T00:00:00.00</sd:TidsstempelDatoTid></sd:TilTidspunkt></sd:Virkning><sd:ReferenceID><sd:UUIDIdentifikator>12345-11-11-11-12345</sd:UUIDIdentifikator></sd:ReferenceID></sd:Overordnet></RelationListe></Registrering></RegistreringBesked>"""  # noqa


unit_parent = {
    "name": "A-sdm1",
    "uuid": "12345-11-11-11-12345",
    "org_unit_type": {"uuid": "uuid-a"},
    "org_unit_level": {"uuid": "uuid-b"},
    "user_key": "user-key-11111",
}


mox_cfg = {
    "TR_DANNES_IKKE": "uuid-tr-dannes-ikke",
    "TR_ARBEJDSTIDSPLANER": "uuid-tr-arbejdstidsplaner",
    "TR_TJENESTETID": "uuid-tr-tjenestetid",
    "AMQP_USER": "example",
    "AMQP_HOST": "example.com",
    "AMQP_PORT": 2222,
    "AMQP_PASSWORD": "",
    "AMQP_CHECK_WAITTIME": 0,
    "AMQP_CHECK_RETRIES": 0,
    "VIRTUAL_HOST": "example.com",
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
        "INSTITUTION_IDENTIFIER": "",
    },
    "sd_unit_levels": {"Afdelings-niveau": "uuid-b"},
    "arbtid_by_uuid": {
        "uuid-tr-arbejdstidsplaner": "Arbejdstidsplaner",
    },
}


class Tests(unittest.TestCase):
    def setUp(self):
        from_date = datetime.datetime(2019, 7, 1, 0, 0)
        self.mox = sd_mox.sdMox(from_date, **mox_cfg)

    def test_grouped_adresses(self):
        addresses = [
            {
                "address_type": {"scope": "DAR", "user_key": "dar-key-1"},
                "value": "0a3f507b-6331-32b8-e044-0003ba298018",
            },
            {
                "address_type": {"scope": "DAR", "user_key": "dar-key-2"},
                "value": "0a3f507b-7750-32b8-e044-0003ba298018",
            },
            {
                "address_type": {"scope": "PHONE", "user_key": "phn-key-1"},
                "value": "12345678",
            },
            {
                "address_type": {"scope": "PNUMBER", "user_key": "pnum-key-1"},
                "value": "0123456789",
            },
        ]
        scoped, keyed = self.mox.grouped_addresses(addresses)

        self.assertEqual(
            {
                "DAR": [
                    "Banegårdspladsen 1, 2750 Ballerup",
                    "Toftebjerghaven 4, 2750 Ballerup",
                ],
                "PHONE": ["12345678"],
                "PNUMBER": ["0123456789"],
            },
            scoped,
        )

        self.assertEqual(
            {
                "dar-key-1": ["0a3f507b-6331-32b8-e044-0003ba298018"],
                "dar-key-2": ["0a3f507b-7750-32b8-e044-0003ba298018"],
                "phn-key-1": ["12345678"],
                "pnum-key-1": ["0123456789"],
            },
            keyed,
        )

    def test_payload_create(self):
        pc = self.mox.payload_create(
            unit_uuid="12345-22-22-22-12345",
            unit={
                "name": "A-sdm2",
                # "uuid": "12345-22-22-22-12345",
                "org_unit_type": {"uuid": "uuid-a"},
                "org_unit_level": {"uuid": "uuid-b"},
                "user_key": "user-key-22222",
            },
            parent=unit_parent,
        )

        self.assertEqual(
            {
                "unit_name": "A-sdm2",
                "parent": {
                    "level": "Afdelings-niveau",
                    "unit_code": "user-key-11111",
                    "uuid": "12345-11-11-11-12345",
                },
                "unit_code": "user-key-22222",
                "unit_level": "Afdelings-niveau",
                "unit_uuid": "12345-22-22-22-12345",
            },
            pc,
        )

        expected = xmltodict.parse(xml_create, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_import(
                unit_name=pc["unit_name"],
                unit_uuid=pc["unit_uuid"],
                unit_code=pc["unit_code"],
                unit_level=pc["unit_level"],
                parent=pc["parent"]["uuid"],
            )
            # print(actual)
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))

    def test_payload_edit_simple(self):
        pe = self.mox.payload_edit(
            unit_uuid="12345-22-22-22-12345",
            unit={
                "name": "A-sdm2",
                "org_unit_type": {"uuid": "uuid-a"},
                "user_key": "user-key-22222",
            },
            addresses=[],
        )

        self.assertEqual(
            {
                "unit_name": "A-sdm2",
                "unit_code": "user-key-22222",
                "phone": None,
                "adresse": None,
                "pnummer": None,
                "integration_values": {
                    "formaalskode": None,
                    "skolekode": None,
                    "time_planning": None,
                },
                "unit_uuid": "12345-22-22-22-12345",
            },
            pe,
        )

        expected = xmltodict.parse(xml_edit_simple, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_ret(**pe)
            # print(actual)
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
                {
                    "address_type": {
                        "scope": "DAR",
                        "user_key": "dar-userkey-not-used",
                    },
                    "value": "0a3f507b-7750-32b8-e044-0003ba298018",
                },
                {
                    "address_type": {
                        "scope": "PHONE",
                        "user_key": "phone-user-key-not-used",
                    },
                    "value": "12345678",
                },
                {
                    "address_type": {
                        "scope": "PNUMBER",
                        "user_key": "pnummer-user-key-not-used",
                    },
                    "value": "0123456789",
                },
            ],
        )

        self.assertEqual(
            {
                "unit_name": "A-sdm2",
                "unit_code": "user-key-22222",
                "phone": "12345678",
                "adresse": {
                    "silkdata:AdresseNavn": "Toftebjerghaven 4",
                    "silkdata:ByNavn": "Ballerup",
                    "silkdata:PostKodeIdentifikator": "2750",
                },
                "pnummer": "0123456789",
                "integration_values": {
                    "formaalskode": None,
                    "skolekode": None,
                    "time_planning": None,
                },
                "unit_uuid": "12345-22-22-22-12345",
            },
            pe,
        )

        expected = xmltodict.parse(xml_edit_address, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_ret(**pe)
            # print(actual)
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))

    def test_payload_edit_integration_values(self):
        pe = self.mox.payload_edit(
            unit_uuid="12345-33-33-33-12345",
            unit={
                "name": "A-sdm3",
                "org_unit_type": {"uuid": "uuid-a"},
                "user_key": "user-key-33333",
                "time_planning": {"uuid": "uuid-tr-arbejdstidsplaner"},
            },
            addresses=[
                {
                    "address_type": {"scope": "TEXT", "user_key": "Formålskode"},
                    "name": "fkode-name-not-used",
                    "value": "Formål1",
                },
                {
                    "address_type": {"scope": "TEXT", "user_key": "Skolekode"},
                    "value": "Skole1",
                },
            ],
        )

        self.assertEqual(
            {
                "unit_name": "A-sdm3",
                "unit_code": "user-key-33333",
                "phone": None,
                "adresse": None,
                "pnummer": None,
                "integration_values": {
                    "formaalskode": "Formål1",
                    "skolekode": "Skole1",
                    "time_planning": "Arbejdstidsplaner",
                },
                "unit_uuid": "12345-33-33-33-12345",
            },
            pe,
        )

        expected = xmltodict.parse(xml_edit_integration_values, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_ret(**pe)
            # print(actual)
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))

    def test_payload_move_orgunit(self):
        pc = self.mox.payload_create(
            unit_uuid="12345-22-22-22-12345",
            unit={
                "name": "A-sdm2",
                # "uuid": "12345-22-22-22-12345",
                "org_unit_type": {"uuid": "uuid-a"},
                "org_unit_level": {"uuid": "uuid-b"},
                "user_key": "user-key-22222",
            },
            parent=unit_parent,
        )

        self.assertEqual(
            {
                "unit_name": "A-sdm2",
                "parent": {
                    "level": "Afdelings-niveau",
                    "unit_code": "user-key-11111",
                    "uuid": "12345-11-11-11-12345",
                },
                "unit_code": "user-key-22222",
                "unit_level": "Afdelings-niveau",
                "unit_uuid": "12345-22-22-22-12345",
            },
            pc,
        )

        expected = xmltodict.parse(xml_move, dict_constructor=dict)
        with freezegun.freeze_time("2020-01-01 12:00:00"):
            actual = self.mox._create_xml_flyt(
                unit_name=pc["unit_name"],
                unit_uuid=pc["unit_uuid"],
                unit_code=pc["unit_code"],
                unit_level=pc["unit_level"],
                parent_unit_uuid=pc["parent"]["uuid"],
            )
            # print(actual)
            self.assertEqual(expected, xmltodict.parse(actual, dict_constructor=dict))
