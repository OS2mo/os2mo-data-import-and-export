#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import asyncio
import datetime
import logging
import pprint
from collections import OrderedDict
from operator import itemgetter

import pika
import requests
import xmltodict
from os2mo_helpers.mora_helpers import MoraHelper

import sd_mox_payloads as smp
from config import get_settings
from util import get_mora_helper, get_organisation_uuid

from SDConnector import SDConnector

logger = logging.getLogger("sdMox")
logger.setLevel(logging.DEBUG)


class SDMoxError(Exception):
    def __init__(self, message):
        logger.exception(str(message))
        Exception.__init__(self, "SD-Mox: " + str(message))


class SDMox(object):
    def __init__(self, from_date=None, to_date=None, overrides=None, settings=None):
        # Load settings and overrides
        overrides = overrides or {}
        self.settings = settings or get_settings(**overrides)

        self.sd_connector = SDConnector(
            self.settings.sd_institution,
            self.settings.sd_username,
            self.settings.sd_password,
            self.settings.sd_base_url
        )

        # Fetch levels from MO
        self.sd_levels = self._read_ou_levelkeys()
        self.level_by_uuid = {v: k for k, v in self.sd_levels.items()}
        self.arbtid_by_uuid = self._read_arbtid_by_uuid()

        # AMQP exchange
        self.exchange_name = "org-struktur-changes-topic"

        if from_date:
            self._update_virkning(from_date)

    def _get_mora_helper(self):
        mora_helper = get_mora_helper(self.settings.mora_url)
        return mora_helper

    def _read_ou_levelkeys(self):
        mora_helpers = self._get_mora_helper()

        classes, _ = mora_helpers.read_classes_in_facet("org_unit_level")
        classes = dict(map(itemgetter("user_key", "uuid"), classes))
        return OrderedDict(
            map(lambda key: (key, classes[key]), self.settings.ou_levelkeys)
        )

    def _read_arbtid_by_uuid(self):
        mora_helpers = self._get_mora_helper()

        classes, _ = mora_helpers.read_classes_in_facet("time_planning")
        classes = dict(map(itemgetter("user_key", "uuid"), classes))

        arbtid_by_uuid = {}
        for key, sd_value in self.settings.ou_time_planning_mo_vs_sd.items():
            arbtid_by_uuid[classes[key]] = sd_value
        return arbtid_by_uuid

    def amqp_connect(self):
        """Establish a connection to the AMQP broker."""
        credentials = pika.PlainCredentials(
            self.settings.amqp_username,
            self.settings.amqp_password
        )
        parameters = pika.ConnectionParameters(
            host=self.settings.amqp_host,
            port=self.settings.amqp_port,
            virtual_host=self.settings.amqp_virtual_host,
            credentials=credentials,
        )
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()
        result = self.channel.queue_declare("", exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
        )
        # auto_ack=True

    def on_response(self, ch, method, props, body):
        logger.error(body)
        raise SDMoxError("Uventet svar fra SD AMQP")

    def call(self, xml):
        logger.info("Calling SD-Mox AMQP")
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key="#",
            properties=pika.BasicProperties(reply_to=self.callback_queue),
            body=xml,
        )
        # Todo: We should to a lookup at verify actual unit
        # matches the expected result
        return True

    def _update_virkning(self, from_date, to_date=None):
        self.virkning = smp.sd_virkning(from_date, to_date)
        if to_date is None:
            to_date = datetime.date(9999, 12, 31)
        if not from_date.day == 1:
            raise SDMoxError("Startdato skal altid være den første i en måned")
        self._times = {
            "virk_from": from_date.strftime("%Y-%m-%dT00:00:00.00"),
            "virk_to": to_date.strftime("%Y-%m-%dT00:00:00.00"),
        }

    def read_parent(self, unit_uuid=None):
        from_date = self.virkning["sd:FraTidspunkt"]["sd:TidsstempelDatoTid"][0:10]
        params = {"EffectiveDate": from_date, "DepartmentUUIDIdentifier": unit_uuid}
        logger.debug("Read parent, params: {}".format(params))
        parent = self.sd.lookup("GetDepartmentParent20190701", params)
        parent_info = parent.get("DepartmentParent", None)
        return parent_info

    async def read_department(self, unit_code=None, unit_uuid=None, unit_level=None):
        from_date = self.virkning["sd:FraTidspunkt"]["sd:TidsstempelDatoTid"][0:10]
        from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
        department = await self.sd_connector.getDepartment(
            department_identifier=(unit_uuid or unit_code),
            department_level_identifier=unit_level,
            start_date = from_date,
            end_date = from_date,
        )
        department_info = department.get("Department", None)
        logger.debug(
            "Read department, department_info: {}".format(
                pprint.pformat(department_info)
            )
        )

        if isinstance(department_info, list):
            msg = "Afdeling ikke unik. Code {}, uuid {}, level {}".format(
                unit_code, unit_uuid, unit_level
            )
            logger.error(msg)
            logger.error("Number units: {}".format(len(department_info)))
            raise SDMoxError(msg)
        return department_info

    async def _check_department(
        self,
        unit_name=None,
        unit_code=None,
        unit_uuid=None,
        unit_level=None,
        phone=None,
        pnummer=None,
        adresse=None,
        parent=None,
        integration_values=None,
        operation=None,
    ):
        """
        Verify that an SD department contains what we think it should contain.
        Besides the supplied parameters, the activation date is also checked
        against the global from_date.
        :param unit_name: Expected name or None.
        :param unit_code: Expected unit code or None.
        :param unit_uuid: Expected unit uuid or None. Also used to look up dept.
        :param unit_level: Expected unit level or None. Also used to look up dept.
        :param phone: Expected phone or None.
        :param pnummer: Expected pnummer or None.
        :param adresse: Expected address or None.
        :param parent: Expected uuid of the parent or None,
        :param integration_values: This is currently ignored, as it can't be checked
        :param operation: flyt, ret, import
        :return: Returns list errors, empty list if no errors.
        """
        errors = []

        def compare(actual, expected, error):
            if expected is not None and actual != expected:
                errors.append(error)

        department = await self.read_department(
            unit_code=unit_code, unit_uuid=unit_uuid, unit_level=unit_level
        )
        if department is None:
            return None, ["Unit"]

        from_date = self.virkning["sd:FraTidspunkt"]["sd:TidsstempelDatoTid"][0:10]
        if operation in ("ret", "import"):
            compare(department.get("ActivationDate"), from_date, "Activation Date")
        compare(department.get("DepartmentName"), unit_name, "Name")
        compare(department.get("DepartmentIdentifier"), unit_code, "Unit code")
        compare(department.get("DepartmentUUIDIdentifier"), unit_uuid, "UUID")
        compare(department.get("DepartmentLevelIdentifier"), unit_level, "Level")
        compare(
            department.get("ContactInformation", {}).get(
                "TelephoneNumberIdentifier", [None]
            )[0],
            phone,
            "Phone",
        )
        compare(department.get("ProductionUnitIdentifier"), pnummer, "Pnummer")
        if adresse:
            actual = department.get("PostalAddress", {})
            compare(
                actual.get("StandardAddressIdentifier"),
                adresse.get("silkdata:AdresseNavn"),
                "Address",
            )
            compare(
                actual.get("PostalCode"),
                adresse.get("silkdata:PostKodeIdentifikator"),
                "Zip code",
            )
            compare(
                actual.get("DistrictName"),
                adresse.get("silkdata:ByNavn"),
                "Postal Area",
            )
        if parent is not None:
            parent_uuid = parent["uuid"]
            actual = self.read_parent(unit_uuid)
            if actual is not None:
                compare(actual.get("DepartmentUUIDIdentifier"), parent_uuid, "Parent")
            else:
                errors.append("Parent")
        if not errors:
            logger.info("SD-Mox succeess on %s", unit_uuid)

        return department, errors

    def _create_xml_ret(
        self,
        unit_uuid,
        unit_code=None,
        unit_name=None,
        pnummer=None,
        phone=None,
        adresse=None,
        integration_values=None,
    ):
        value_dict = {
            "RelationListe": smp.relations_ret(
                self.virkning,
                pnummer=pnummer,
                phone=phone,
                adresse=adresse,
            ),
            "AttributListe": smp.attributes_ret(
                self.virkning,
                funktionskode=integration_values["formaalskode"],
                skolekode=integration_values["skolekode"],
                tidsregistrering=integration_values["time_planning"],
                unit_name=unit_name,
            ),
            "Registrering": smp.create_registrering(
                self.virkning, registry_type="Rettet"
            ),
            "ObjektID": smp.create_objekt_id(unit_uuid),
        }
        edit_dict = {"RegistreringBesked": value_dict}
        edit_dict["RegistreringBesked"].update(smp.boilerplate)
        xml = xmltodict.unparse(edit_dict)
        return xml

    def _create_xml_import(self, **payload):
        payload.update(self._times)
        import_dict = smp.import_xml_dict(**payload)
        xml = xmltodict.unparse(import_dict)
        return xml

    def _create_xml_flyt(self, **payload):
        payload.update(self._times)
        flyt_dict = smp.flyt_xml_dict(**payload)
        xml = xmltodict.unparse(flyt_dict)
        return xml

    def _validate_unit_code(self, unit_code, unit_level=None, can_exist=False):
        logger.info("Validating unit code {}".format(unit_code))
        code_errors = []
        if unit_code is None:
            code_errors.append("Enhedsnummer ikke angivet")
        else:
            if len(unit_code) < 2:
                code_errors.append("Enhedsnummer for kort")
            elif len(unit_code) > 4:
                code_errors.append("Enhedsnummer for langt")
            if not unit_code.isalnum():
                code_errors.append("Ugyldigt tegn i enhedsnummer")
            if unit_code.upper() != unit_code:
                code_errors.append("Enhedsnummer skal være store bogstaver")

        if not code_errors:
            # TODO: Ignore duplicates as we lookup using UUID elsewhere
            #       Only check for duplicates on new creations
            # customers expect unique unit_codes globally
            department = self.read_department(unit_code=unit_code)
            if department is not None and not can_exist:
                code_errors.append("Enhedsnummer er i brug")
        return code_errors

    def _mo_to_sd_address(self, address):
        if address is None:
            return None
        street, zip_code, city = address.rsplit(" ", maxsplit=2)
        if street.endswith(","):
            street = street[:-1]
        sd_address = {
            "silkdata:AdresseNavn": street.strip(),
            "silkdata:PostKodeIdentifikator": zip_code.strip(),
            "silkdata:ByNavn": city.strip(),
        }
        return sd_address

    def create_unit(
        self, unit_name, unit_code, parent, unit_level, unit_uuid=None, test_run=True
    ):
        """
        Create a new unit in SD.
        :param unit_name: Unit name.
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
        code_errors = self._validate_unit_code(unit_code)

        if code_errors:
            raise SDMoxError(", ".join(code_errors))

        # Verify the parent department actually exist
        parent_department = self.read_department(
            unit_code=parent["unit_code"], unit_level=parent["level"]
        )
        if not parent_department:
            raise SDMoxError("Forældrenheden findes ikke")

        unit_index = list(self.sd_levels.keys()).index(unit_level)
        parent_index = list(self.sd_levels.keys()).index(
            parent_department["DepartmentLevelIdentifier"]
        )

        if not unit_index > parent_index:
            raise SDMoxError("Enhedstypen passer ikke til forældreenheden")

        xml = self._create_xml_import(
            unit_name=unit_name,
            unit_uuid=unit_uuid,
            unit_code=unit_code,
            unit_level=unit_level,
            parent_unit_uuid=parent["uuid"],
        )

        logger.debug("Create unit xml: {}".format(xml))
        if not test_run:
            logger.info(
                "Create unit {}, {}, {}".format(unit_name, unit_code, unit_uuid)
            )
            self.call(xml)
        return unit_uuid

    async def rename_unit(self, unit_uuid, new_unit_name, at, dry_run=False):
        mora_helpers = self._get_mora_helper()

        # Fetch old ou data
        unit = mora_helpers.read_ou(unit_uuid, at=at)
        # Change to add our new data
        unit["name"] = new_unit_name

        # doing a read department here will give the non-unique error
        # here - where we still have access to the mo-error reporting
        code_errors = self._validate_unit_code(unit["user_key"], can_exist=True)
        if code_errors:
            raise SDMoxError(", ".join(code_errors))

        addresses = mora_helpers.read_ou_address(
            unit_uuid, at=at, scope=None, return_all=True, reformat=False
        )
        payload = self.payload_edit(unit_uuid, unit, addresses)

        self.edit_unit(test_run=dry_run, **payload)
        return await self.check_unit(operation="ret", **payload)

    async def move_unit(self, unit_uuid, new_parent_uuid, at, dry_run=False):
        settings = load_settings()
        mora_helpers = MoraHelper(hostname=settings["mora.base"])

        # Fetch old ou data
        unit = mora_helpers.read_ou(unit_uuid, at=at)

        # doing a read department here will give the non-unique error
        # here - where we still have access to the mo-error reporting
        code_errors = self._validate_unit_code(unit["user_key"], can_exist=True)
        if code_errors:
            raise sd_mox.SDMoxError(", ".join(code_errors))

        # Fetch the new parent
        new_parent_unit = mora_helpers.read_ou(new_parent_uuid)

        payload = mox.payload_create(unit_uuid, unit, new_parent_unit)
        operation = "flyt"
        self._move_unit(test_run=dry_run, **payload)

        # when moving, do not check against name
        payload["unit_name"] = None
        return await self.check_unit(operation="flyt", **payload)

    def edit_unit(self, test_run=True, **payload):
        xml = self._create_xml_ret(**payload)
        logger.debug("Edit unit xml: {}".format(xml))
        if not test_run:
            logger.info("Edit unit {!r}".format(payload))
            self.call(xml)
        return payload["unit_uuid"]

    def _move_unit(
        self, unit_name, unit_code, parent, unit_level, unit_uuid=None, test_run=True
    ):

        code_errors = self._validate_unit_code(unit_code, can_exist=True)
        if code_errors:
            raise SDMoxError(", ".join(code_errors))

        # Verify the parent department actually exist
        parent_department = self.read_department(
            unit_code=parent["unit_code"], unit_level=parent["level"]
        )
        if not parent_department:
            raise SDMoxError("Forældrenheden findes ikke")

        unit_index = list(self.sd_levels.keys()).index(unit_level)
        parent_index = list(self.sd_levels.keys()).index(
            parent_department["DepartmentLevelIdentifier"]
        )
        if not unit_index > parent_index:
            raise SDMoxError("Enhedstypen passer ikke til forældreenheden")

        xml = self._create_xml_flyt(
            unit_name=unit_name,
            unit_uuid=unit_uuid,
            unit_code=unit_code,
            unit_level=unit_level,
            parent=parent["uuid"],
            parent_unit_uuid=parent["uuid"],
        )
        logger.debug("Move unit xml: {}".format(xml))
        if not test_run:
            self.call(xml)
        return unit_uuid

    async def check_unit(self, **payload):
        """Try to have the unit retrieved and compared to the
        values at hand for as many times
        as specified in self.amqp_check_retries and return the unit.

        Raise an sdMoxError if the unit could not be found or did not have
        the expected attribute values. This error will be shown in the UI
        """
        unit = None
        errors = None
        for i in range(self.settings.amqp_check_retries):
            await asyncio.sleep(self.settings.amqp_check_waittime)
            unit, errors = await self._check_department(**payload)
            if unit is not None:
                break
        if unit is None:
            raise SDMoxError("Afdeling ikke fundet: %s" % payload["unit_uuid"])
        elif errors:
            errstr = ", ".join(errors)
            raise SDMoxError(
                "Følgende felter kunne " "ikke opdateres i SD: %s" % errstr
            )
        return unit

    def payload_create(self, unit_uuid, unit, parent):
        unit_level = self.level_by_uuid.get(unit["org_unit_level"]["uuid"])
        if not unit_level:
            raise SDMoxError("Enhedstype er ikke et kendt NY-niveau")

        parent_level = self.level_by_uuid.get(parent["org_unit_level"]["uuid"])
        if not parent_level:
            raise SDMoxError(
                "Forældreenhedens enhedstype er " "ikke et kendt NY-niveau"
            )

        return {
            "unit_name": unit["name"],
            "parent": {
                "unit_code": parent["user_key"],
                "uuid": parent["uuid"],
                "level": parent_level,
            },
            "unit_code": unit["user_key"],
            "unit_level": unit_level,
            "unit_uuid": unit_uuid,
        }

    def get_dar_address(self, addrid):
        for addrtype in (
            "adresser",
            "adgangsadresser",
            "historik/adresser",
            "historik/adgangsadresser",
        ):
            try:
                r = requests.get(
                    "https://dawa.aws.dk/" + addrtype,
                    params=[
                        ("id", addrid),
                        ("noformat", "1"),
                        ("struktur", "mini"),
                    ],
                )
                addrobjs = r.json()
                r.raise_for_status()
                if addrobjs:
                    # found, escape loop!
                    break
            except Exception as e:
                raise SDMoxError("Fejlende opslag i DAR for " + addrid) from e
        else:
            raise SDMoxError("Addresse ikke fundet i DAR: {!r}".format(addrid))

        return addrobjs.pop()["betegnelse"]

    def grouped_addresses(self, details):
        keyed, scoped = {}, {}
        for d in details:
            scope, key = d["address_type"]["scope"], d["address_type"]["user_key"]
            if scope == "DAR":
                scoped.setdefault(scope, []).append(self.get_dar_address(d["value"]))
            else:
                scoped.setdefault(scope, []).append(d["value"])
            keyed.setdefault(key, []).append(d["value"])
        return scoped, keyed

    def payload_edit(self, unit_uuid, unit, addresses):
        scoped, keyed = self.grouped_addresses(addresses)
        if "PNUMBER" in scoped and "DAR" not in scoped:
            # it has proven difficult to deal with pnumber before postal address
            raise SDMoxError("Opret postaddresse før pnummer")

        # if time planning exists, it must be in self.arbtitd
        time_planning = unit.get("time_planning", None)
        if time_planning:
            time_planning = self.arbtid_by_uuid[time_planning["uuid"]]

        return {
            "unit_name": unit["name"],
            "unit_code": unit["user_key"],
            "unit_uuid": unit_uuid,
            "phone": scoped.get("PHONE", [None])[0],
            "pnummer": scoped.get("PNUMBER", [None])[0],
            "adresse": self._mo_to_sd_address(scoped.get("DAR", [None])[0]),
            "integration_values": {
                "time_planning": time_planning,
                "formaalskode": keyed.get("Formålskode", [None])[0],
                "skolekode": keyed.get("Skolekode", [None])[0],
            },
        }
