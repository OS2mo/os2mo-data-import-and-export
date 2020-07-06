#
# Copyright (c) 2019, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import csv
import json
import logging
import os
import pathlib
import sys
from logging.handlers import RotatingFileHandler

import requests

from os2mo_tools import mo_api


cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
if not cfg_file.is_file():
    raise Exception("No setting file")
SETTINGS = json.loads(cfg_file.read_text())

logger = logging.getLogger(__name__)

EMPLOYEE_MAPPING_PATH = os.environ.get("MOX_ROLLE_MAPPING")
AD_SYSTEM_NAME = SETTINGS["exporters.os2rollekatalog.ad_system_name"]
OS2MO_URL = SETTINGS["mora.base"]
OS2MO_API_KEY = os.environ.get("SAML_TOKEN")
ROLLEKATALOG_URL = SETTINGS["exporters.os2rollekatalog.rollekatalog.url"]
ROLLEKATALOG_API_KEY = SETTINGS["exporters.os2rollekatalog.rollekatalog.api_token"]
LOG_PATH = os.environ.get("MOX_ROLLE_LOG_FILE")
# Rollekataloget only supports one root org unit.
# All other root org units in OS2mo will be made children of this one
MAIN_ROOT_ORG_UNIT = SETTINGS["exporters.os2rollekatalog.main_root_org_unit"]


MAPPING = {}


def get_employee_from_map(employee_uuid):
    mapping = get_employee_mapping()

    if employee_uuid not in mapping:
        logger.critical(
            "Unable to find employee in mapping with UUID {}".format(employee_uuid)
        )
        sys.exit(3)
    return mapping[employee_uuid]


def init_log():
    logging.getLogger("urllib3").setLevel(logging.INFO)

    log_format = logging.Formatter(
        "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    )

    stdout_log_handler = logging.StreamHandler()
    stdout_log_handler.setFormatter(log_format)
    stdout_log_handler.setLevel(logging.DEBUG)  # this can be higher
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(stdout_log_handler)

    # The activity log is for everything that isn't debug information. Only
    # write single lines and no exception tracebacks here as it is harder to
    # parse.
    try:
        log_file_handler = RotatingFileHandler(filename=LOG_PATH, maxBytes=1000000)
    except OSError as err:
        logger.critical("MOX_ROLLE_LOG_FILE: %s: %r", err.strerror, err.filename)
        sys.exit(3)

    log_file_handler.setFormatter(log_format)
    log_file_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(log_file_handler)


def get_employee_mapping():
    global MAPPING
    if not MAPPING:
        mapping_path = EMPLOYEE_MAPPING_PATH
        try:
            with open(mapping_path) as f:
                csv_reader = csv.DictReader(f, delimiter=";")
                content = {
                    line["mo_uuid"]: (line["ad_guid"], line["sam_account_name"])
                    for line in csv_reader
                }
        except FileNotFoundError as err:
            logger.critical("%s: %r", err.strerror, err.filename)
            sys.exit(3)
        MAPPING = content
    return MAPPING


def get_org_units(connector):
    org_units = connector.get_ous()

    converted_org_units = []
    for org_unit in org_units:

        org_unit_uuid = org_unit["uuid"]
        # Fetch the OU again, as the 'parent' field is missing in the data
        # when listing all org units
        ou_present = connector.get_ou_connector(org_unit_uuid, validity='present')
        ou_future = connector.get_ou_connector(org_unit_uuid, validity='future')
        ou_connectors = (ou_present, ou_future)

        def get_parent_org_unit_uuid(ou):
            parent = ou.json["parent"]
            if parent:
                return parent["uuid"]
            # Rollekataloget only support one root org unit, so all other org
            # units get put under the main one
            elif ou.uuid != MAIN_ROOT_ORG_UNIT:
                return MAIN_ROOT_ORG_UNIT

            return None

        def get_manager(*ou_connectors):
            managers = []
            for ou in ou_connectors:
                managers.extend(ou.manager)
            if not managers:
                return None
            if len(managers) > 1:
                logger.warning(
                    "More than one manager exists for {}".format(org_unit_uuid)
                )
            manager = managers[0]

            person = manager.get("person")
            if not person:
                return None

            ad_guid, sam_account_name = get_employee_from_map(person["uuid"])
            if not ad_guid or not sam_account_name:
                # Only import users who are in AD
                return {}

            return {"uuid": manager["uuid"], "userId": sam_account_name}

        payload = {
            "uuid": org_unit_uuid,
            "name": org_unit["name"],
            "parentOrgUnitUuid": get_parent_org_unit_uuid(ou_present),
            "manager": get_manager(*ou_connectors),
        }
        converted_org_units.append(payload)

    return converted_org_units


def get_users(connector):
    # read mapping
    employees = connector.get_employees()

    converted_users = []
    for employee in employees:

        employee_uuid = employee["uuid"]
        e_present = connector.get_employee_connector(employee_uuid, validity='present')
        e_future = connector.get_employee_connector(employee_uuid, validity='future')
        e_connectors = (e_present, e_future)

        def get_employee_email(*engagement_connectors):
            addresses = []
            for e in engagement_connectors:
                addresses.extend(e.address)

            emails = list(
                filter(
                    lambda address: address["address_type"]["scope"] == "EMAIL",
                    addresses,
                )
            )

            if emails:
                if len(emails) > 1:
                    logger.warning(
                        "More than one email exists for user {}".format(employee_uuid)
                    )
                return emails[0]["value"]
            return None

        def get_employee_positions(*engagement_connectors):
            engagements = []
            for e in engagement_connectors:
                engagements.extend(e.engagement)

            converted_positions = []
            for engagement in engagements:
                converted_positions.append(
                    {
                        "name": engagement["job_function"]["name"],
                        "orgUnitUuid": engagement["org_unit"]["uuid"],
                    }
                )
            return converted_positions

        ad_guid, sam_account_name = get_employee_from_map(employee_uuid)

        if not ad_guid or not sam_account_name:
            # Only import users who are in AD
            continue

        payload = {
            "extUuid": ad_guid,
            "userId": sam_account_name,
            "name": employee["name"],
            "email": get_employee_email(*e_connectors),
            "positions": get_employee_positions(*e_connectors),
        }
        converted_users.append(payload)

    return converted_users


def main():
    """Main function - download from OS2MO and export to OS2Rollekatalog."""
    init_log()

    try:
        service_url = OS2MO_URL + "/service"
        mo_connector = mo_api.Connector(service_url, api_token=OS2MO_API_KEY)
    except requests.RequestException:
        logger.exception("An error occurred connecting to OS2mo")
        sys.exit(3)

    try:
        logger.info("Reading organisation")
        org_units = get_org_units(mo_connector)
    except requests.RequestException:
        logger.exception("An error occurred trying to fetch org units")
        sys.exit(3)
    logger.info("Found {} org units".format(len(org_units)))

    try:
        logger.info("Reading employees")
        users = get_users(mo_connector)
    except requests.RequestException:
        logger.exception("An error occurred trying to fetch employees")
        sys.exit(3)
    logger.info("Found {} employees".format(len(users)))

    payload = {"orgUnits": org_units, "users": users}

    try:
        logger.info("Writing to Rollekataloget")
        result = requests.post(
            ROLLEKATALOG_URL,
            json=payload,
            headers={"ApiKey": ROLLEKATALOG_API_KEY},
            verify=False,
        )
        logger.info(result.json())
        result.raise_for_status()
    except requests.RequestException:
        logger.exception("An error occurred when writing to Rollekataloget")
        sys.exit(3)


if __name__ == "__main__":
    main()
