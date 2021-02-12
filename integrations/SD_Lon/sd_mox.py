#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import datetime
import logging
import pprint
import time
from collections import OrderedDict
from operator import itemgetter

import click
import pika
import requests
import xmltodict
from integrations.SD_Lon.mora_helpers import MoraHelper

from integrations.SD_Lon import sd_mox_payloads as smp
from integrations.SD_Lon.sd import SD
from integrations.SD_Lon.sd_common import load_settings

logger = logging.getLogger("sdMox")
logger.setLevel(logging.DEBUG)


def read_sdmox_config():
    settings = load_settings()
    sdmox_config = {
        "AMQP_USER": settings["integrations.SD_Lon.sd_mox.AMQP_USER"],
        "AMQP_PASSWORD": settings["integrations.SD_Lon.sd_mox.AMQP_PASSWORD"],
        "AMQP_HOST": settings.get(
            "integrations.SD_Lon.sd_mox.AMQP_HOST", "msg-amqp.silkeborgdata.dk"
        ),
        "AMQP_PORT": settings.get("integrations.SD_Lon.sd_mox.AMQP_PORT", 5672),
        "AMQP_CHECK_WAITTIME": settings.get(
            "integrations.SD_Lon.sd_mox.AMQP_CHECK_WAITTIME", 3
        ),
        "AMQP_CHECK_RETRIES": settings.get(
            "integrations.SD_Lon.sd_mox.AMQP_CHECK_RETRIES", 6
        ),
        "VIRTUAL_HOST": settings["integrations.SD_Lon.sd_mox.VIRTUAL_HOST"],
        "OS2MO_SERVICE": settings["mora.base"] + "/service/",
        "OS2MO_TOKEN": settings.get("crontab.SAML_TOKEN"),
        "OS2MO_VERIFY": settings.get("mora.verify", True),
        "TRIGGERED_UUIDS": settings["integrations.SD_Lon.sd_mox.TRIGGERED_UUIDS"],
        "OU_LEVELKEYS": settings["integrations.SD_Lon.sd_mox.OU_LEVELKEYS"],
        "OU_TIME_PLANNING_MO_VS_SD": settings[
            "integrations.SD_Lon.sd_mox.OU_TIME_PLANNING_MO_VS_SD"
        ],
        "sd_unit_levels": [],
        "arbtid_by_uuid": {},
        "sd_common": {
            "USE_PICKLE_CACHE": False,  # force no caching for sd
            "SD_USER": settings["integrations.SD_Lon.sd_user"],
            "SD_PASSWORD": settings["integrations.SD_Lon.sd_password"],
            "INSTITUTION_IDENTIFIER": settings[
                "integrations.SD_Lon.institution_identifier"
            ],
            "BASE_URL": settings["integrations.SD_Lon.base_url"],
        },
    }

    # Add settings from MO
    mora_helpers = MoraHelper(hostname=settings["mora.base"])

    mora_org = sdmox_config.get("ORG_UUID")
    if mora_org is None:
        sdmox_config["ORG_UUID"] = mora_helpers.read_organisation()

    classes, _ = mora_helpers.read_classes_in_facet("org_unit_level")
    classes = dict(map(itemgetter("user_key", "uuid"), classes))
    for key in sdmox_config["OU_LEVELKEYS"]:
        sdmox_config["sd_unit_levels"].append((key, classes[key]))

    classes, _ = mora_helpers.read_classes_in_facet("time_planning")
    classes = dict(map(itemgetter("user_key", "uuid"), classes))
    for key, sd_value in sdmox_config["OU_TIME_PLANNING_MO_VS_SD"].items():
        sdmox_config["arbtid_by_uuid"][classes[key]] = sd_value

    return sdmox_config


class SdMoxError(Exception):
    def __init__(self, message):
        logger.exception(str(message))
        Exception.__init__(self, "SD-Mox: " + str(message))


class sdMox(object):
    def __init__(self, from_date=None, to_date=None, **kwargs):
        self.config = kwargs
        self.sd = SD(**self.config["sd_common"])

        try:
            self.amqp_user = self.config["AMQP_USER"]
            self.amqp_password = self.config["AMQP_PASSWORD"]
            self.virtual_host = self.config["VIRTUAL_HOST"]
            self.amqp_host = self.config["AMQP_HOST"]
            self.amqp_port = self.config["AMQP_PORT"]
            self.amqp_check_waittime = self.config["AMQP_CHECK_WAITTIME"]
            self.amqp_check_retries = self.config["AMQP_CHECK_RETRIES"]
        except Exception:
            raise SdMoxError("SD AMQP credentials mangler")

        try:
            self.sd_levels = OrderedDict(self.config["sd_unit_levels"])
            self.level_by_uuid = {v: k for k, v in self.sd_levels.items()}
            self.arbtid_by_uuid = self.config["arbtid_by_uuid"]
        except Exception:
            raise SdMoxError(
                "Klasse-uuider for conf af Ny-Niveauer "
                "eller Tidsregistrering mangler"
            )

        if from_date:
            self._update_virkning(from_date)

    @staticmethod
    def create(from_date=None, to_date=None, overrides=None):
        sdmox_config = read_sdmox_config()
        if overrides:
            sdmox_config.update(overrides)
        mox = sdMox(from_date=from_date, to_date=to_date, **sdmox_config)
        mox.amqp_connect()
        return mox

    def amqp_connect(self):
        self.exchange_name = "org-struktur-changes-topic"
        credentials = pika.PlainCredentials(self.amqp_user, self.amqp_password)
        parameters = pika.ConnectionParameters(
            host=self.amqp_host,
            port=self.amqp_port,
            virtual_host=self.virtual_host,
            credentials=credentials,
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare("", exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
        )
        # auto_ack=True

    def on_response(self, ch, method, props, body):
        logger.error(body)
        raise SdMoxError("Uventet svar fra SD AMQP")

    def call(self, xml):
        logger.info("Calling SD-Mox amqp")
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
            raise SdMoxError("Startdato skal altid være den første i en måned")
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

    def read_department(self, unit_code=None, unit_uuid=None, unit_level=None):
        from_date = self.virkning["sd:FraTidspunkt"]["sd:TidsstempelDatoTid"][0:10]
        params = {
            "ActivationDate": from_date,
            "DeactivationDate": from_date,
            "ContactInformationIndicator": "true",
            "DepartmentNameIndicator": "true",
            "PostalAddressIndicator": "true",
            "ProductionUnitIndicator": "true",
            "UUIDIndicator": "true",
            "EmploymentDepartmentIndicator": "false",
        }
        if unit_uuid:
            params["DepartmentUUIDIdentifier"] = unit_uuid
        elif unit_code:
            params["DepartmentIdentifier"] = unit_code
        if unit_level:
            params["DepartmentLevelIdentifier"] = unit_level
        logger.debug("Read department, params: {}".format(params))
        department = self.sd.lookup("GetDepartment20111201", params)
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
            raise SdMoxError(msg)
        return department_info

    def _check_department(
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

        department = self.read_department(
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

    def _validate_unit_code(self, unit_code, unit_level=None, read_department=True):
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

        if not code_errors and read_department:
            # TODO: Ignore duplicates as we lookup using UUID elsewhere
            #       Only check for duplicates on new creations
            # customers expect unique unit_codes globally
            department = self.read_department(unit_code=unit_code)
            if department is not None:
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
            raise SdMoxError(", ".join(code_errors))

        # Verify the parent department actually exist
        parent_department = self.read_department(
            unit_code=parent["unit_code"], unit_level=parent["level"]
        )
        if not parent_department:
            raise SdMoxError("Forældrenheden findes ikke")

        unit_index = list(self.sd_levels.keys()).index(unit_level)
        parent_index = list(self.sd_levels.keys()).index(
            parent_department["DepartmentLevelIdentifier"]
        )

        if not unit_index > parent_index:
            raise SdMoxError("Enhedstypen passer ikke til forældreenheden")

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

    def rename_unit(self, unit_uuid, new_unit_name, at, dry_run=False):
        settings = load_settings()
        mora_helpers = MoraHelper(hostname=settings["mora.base"])

        # Fetch old ou data
        unit = mora_helpers.read_ou(unit_uuid, at=at)
        # Change to add our new data
        unit["name"] = new_unit_name

        # doing a read department here will give the non-unique error
        # here - where we still have access to the mo-error reporting
        code_errors = self._validate_unit_code(unit["user_key"], read_department=False)
        if code_errors:
            raise sd_mox.SdMoxError(", ".join(code_errors))

        addresses = mora_helpers.read_ou_address(
            unit_uuid, at=at, scope=None, return_all=True, reformat=False
        )
        payload = self.payload_edit(unit_uuid, unit, addresses)

        self.edit_unit(test_run=dry_run, **payload)
        return self.check_unit(operation="ret", **payload)

    def edit_unit(self, test_run=True, **payload):
        xml = self._create_xml_ret(**payload)
        logger.debug("Edit unit xml: {}".format(xml))
        if not test_run:
            logger.info("Edit unit {!r}".format(payload))
            self.call(xml)
        return payload["unit_uuid"]

    def move_unit(
        self, unit_name, unit_code, parent, unit_level, unit_uuid=None, test_run=True
    ):

        code_errors = self._validate_unit_code(unit_code, read_department=False)
        if code_errors:
            raise SdMoxError(", ".join(code_errors))

        # Verify the parent department actually exist
        parent_department = self.read_department(
            unit_code=parent["unit_code"], unit_level=parent["level"]
        )
        if not parent_department:
            raise SdMoxError("Forældrenheden findes ikke")

        unit_index = list(self.sd_levels.keys()).index(unit_level)
        parent_index = list(self.sd_levels.keys()).index(
            parent_department["DepartmentLevelIdentifier"]
        )
        if not unit_index > parent_index:
            raise SdMoxError("Enhedstypen passer ikke til forældreenheden")

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

    def check_unit(self, **payload):
        """Try to have the unit retrieved and compared to the
        values at hand for as many times
        as specified in self.amqp_check_retries and return the unit.

        Raise an sdMoxError if the unit could not be found or did not have
        the expected attribute values. This error will be shown in the UI
        """
        unit = None
        errors = None
        for i in range(self.amqp_check_retries):
            time.sleep(self.amqp_check_waittime)
            unit, errors = self._check_department(**payload)
            if unit is not None:
                break
        if unit is None:
            raise SdMoxError("Afdeling ikke fundet: %s" % payload["unit_uuid"])
        elif errors:
            errstr = ", ".join(errors)
            raise SdMoxError(
                "Følgende felter kunne " "ikke opdateres i SD: %s" % errstr
            )
        return unit

    def payload_create(self, unit_uuid, unit, parent):
        unit_level = self.level_by_uuid.get(unit["org_unit_level"]["uuid"])
        if not unit_level:
            raise SdMoxError("Enhedstype er ikke et kendt NY-niveau")

        parent_level = self.level_by_uuid.get(parent["org_unit_level"]["uuid"])
        if not parent_level:
            raise SdMoxError(
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
                raise SdMoxError("Fejlende opslag i DAR for " + addrid) from e
        else:
            raise SdMoxError("Addresse ikke fundet i DAR: {!r}".format(addrid))

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
            raise SdMoxError("Opret postaddresse før pnummer")

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

    def create_unit_from_mo(self, unit_uuid, test_run=True):

        # TODO: This url is hard-codet
        from os2mo_data_import.os2mo_helpers.mora_helpers import MoraHelper

        mh = MoraHelper(hostname="http://localhost:5000")

        logger.info("Create {} from MO, test run: {}".format(unit_uuid, test_run))
        unit_info = mox.mh.read_ou(unit_uuid)
        logger.debug("Unit info: {}".format(unit_info))
        from_date = datetime.datetime.strptime(
            unit_info["validity"]["from"], "%Y-%m-%d"
        )
        self._update_virkning(from_date)

        unit_create_payload = self.payload_create(
            unit_uuid, unit_info, unit_info["parent"]
        )

        try:
            ret_uuid = self.create_unit(test_run=test_run, **unit_create_payload)
            if test_run:
                logger.info("dry-run succeeded: {}".format(ret_uuid))
            else:
                logger.info("amqp-call succeeded: {}".format(ret_uuid))
        except Exception as e:
            print("Error: {}".format(e))
            msg = "Test for unit {} failed: {}".format(unit_uuid, e)
            if test_run:
                logger.info(msg)
            else:
                logger.error(msg)
            return False

        integration_addresses = mh._mo_lookup(unit_uuid, "ou/{}/details/address")
        unit_edit_payload = self.payload_edit(
            unit_uuid, unit_info, integration_addresses
        )

        if not test_run:
            # Running test-run for this edit is a bit more tricky
            self.edit_unit(**unit_edit_payload)
        return True


def today():
    today = datetime.date.today()
    return today


def first_of_month():
    first_day_of_this_month = today().replace(day=1)
    return first_day_of_this_month


clickDate = click.DateTime(formats=["%Y-%m-%d"])


@click.group()
@click.option(
    "--from-date",
    type=clickDate,
    default=str(first_of_month()),
    help="Start date of the validity of the change.",
    show_default=True,
)
@click.option(
    "--to-date",
    type=clickDate,
    help="End date of the validity of the change (can be None for infinite).",
)
@click.option(
    "--overrides",
    multiple=True,
    help="List of overrides to apply to SDMox's configuration.",
)
@click.pass_context
def sd_mox_cli(ctx, from_date, to_date, overrides):
    """Tool to make changes in SD."""

    from_date = from_date.date()
    to_date = to_date.date() if to_date else None
    if to_date and from_date > to_date:
        raise click.ClickException("from_date must be smaller than to_date")

    overrides = dict(override.split("=") for override in overrides)

    sdmox = sdMox.create(from_date, to_date, overrides)

    ctx.ensure_object(dict)
    ctx.obj["sdmox"] = sdmox
    ctx.obj["from_date"] = from_date
    ctx.obj["to_date"] = to_date


@sd_mox_cli.command()
@click.pass_context
@click.option(
    "--unit-uuid",
    type=click.UUID,
    required=True,
    help="UUID of the organizational unit to check.",
)
@click.option(
    "--print-department",
    is_flag=True,
    default=False,
    help="Whether to debug print the raw department data.",
)
@click.option(
    "--unit-name",
    required=True,
    help="The expected name of the organization unit in SD.",
)
def check_name(ctx, unit_uuid, print_department, unit_name):
    mox = ctx.obj["sdmox"]

    unit_uuid = str(unit_uuid)
    department, errors = mox._check_department(
        unit_uuid=unit_uuid,
        unit_name=unit_name,
    )
    if print_department:
        import json

        print(json.dumps(department, indent=4))

    if errors:
        click.echo("Mismatches found for:")
        for error in errors:
            click.echo("* " + click.style(error, fg="red"))


@sd_mox_cli.command()
@click.pass_context
@click.option(
    "--unit-uuid",
    type=click.UUID,
    required=True,
    help="UUID of the organizational unit to modify.",
)
@click.option(
    "--new-unit-name",
    required=True,
    help="The new name to apply to the organization unit in SD.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Whether to dry-run (i.e. make no actual changes).",
)
def set_name(ctx, unit_uuid, new_unit_name, dry_run):
    unit_uuid = str(unit_uuid)

    mox = ctx.obj["sdmox"]
    mox.rename_unit(unit_uuid, new_unit_name, at=ctx.obj["from_date"], dry_run=dry_run)


if __name__ == "__main__":
    sd_mox_cli()
