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
import sys
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from uuid import UUID

import click
import requests
from os2mo_tools import mo_api
from ra_utils.headers import TokenSettings
from ra_utils.load_settings import load_setting
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def get_employee_mapping(mapping_path_str: str) -> Dict[str, Tuple[str, str]]:
    mapping_path = Path(mapping_path_str)
    if not mapping_path.is_file():
        logger.critical("Mapping file does not exist: %s", mapping_path)
        sys.exit(3)

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
    return content


def get_employee_from_map(
    employee_uuid: str, mapping_file_path: str
) -> Tuple[str, str]:
    mapping = get_employee_mapping(mapping_file_path)

    if employee_uuid not in mapping:
        logger.critical(
            "Unable to find employee in mapping with UUID {}".format(employee_uuid)
        )
        sys.exit(3)
    return mapping[employee_uuid]


def init_log(log_path: str) -> None:
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
        log_file_handler = RotatingFileHandler(filename=log_path, maxBytes=1000000)
    except OSError as err:
        logger.critical("MOX_ROLLE_LOG_FILE: %s: %r", err.strerror, err.filename)
        sys.exit(3)

    log_file_handler.setFormatter(log_format)
    log_file_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(log_file_handler)


def get_parent_org_unit_uuid(
    ou, ou_filter: bool, mo_root_org_unit: UUID
) -> Optional[str]:

    if str(ou.uuid) == str(mo_root_org_unit):
        # This is the root, there are no parents
        return None

    parent = ou.json["parent"]
    if ou_filter:
        assert parent, f"The org_unit {ou.uuid} should have been filtered"

    if parent:
        return parent["uuid"]
    # Rollekataloget only support one root org unit, so all other root org
    # units get put under the main one, if filtering is not active.
    return str(mo_root_org_unit)


def get_org_units(
    connector: mo_api.Connector,
    mo_root_org_unit: UUID,
    ou_filter: bool,
    mapping_file_path: str,
) -> Dict[str, Dict[str, Any]]:
    org_units = connector.get_ous(root=mo_root_org_unit)

    converted_org_units = {}
    for org_unit in org_units:

        org_unit_uuid = org_unit["uuid"]
        # Fetch the OU again, as the 'parent' field is missing in the data
        # when listing all org units
        ou_present = connector.get_ou_connector(org_unit_uuid, validity="present")
        ou_future = connector.get_ou_connector(org_unit_uuid, validity="future")
        ou_connectors = (ou_present, ou_future)

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

            ad_guid, sam_account_name = get_employee_from_map(
                person["uuid"], mapping_file_path
            )
            # Only import users who are in AD
            if not ad_guid or not sam_account_name:
                return {}

            return {"uuid": person["uuid"], "userId": sam_account_name}

        payload = {
            "uuid": org_unit_uuid,
            "name": org_unit["name"],
            "parentOrgUnitUuid": get_parent_org_unit_uuid(
                ou_present, ou_filter, mo_root_org_unit
            ),
            "manager": get_manager(*ou_connectors),
        }
        converted_org_units[org_unit_uuid] = payload

    return converted_org_units


def get_users(
    connector: mo_api.Connector,
    mapping_file_path: str,
    org_unit_uuids: Set[str],
    ou_filter: bool,
    use_nickname: bool = False,
) -> List[Dict[str, Any]]:
    # read mapping
    employees = connector.get_employees()

    converted_users = []
    for employee in employees:
        employee_uuid = employee["uuid"]

        ad_guid, sam_account_name = get_employee_from_map(
            employee_uuid, mapping_file_path
        )

        # Only import users who are in AD
        if not ad_guid or not sam_account_name:
            continue

        e_present = connector.get_employee_connector(employee_uuid, validity="present")
        e_future = connector.get_employee_connector(employee_uuid, validity="future")
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

        # read positions first to filter any persons with engagements
        # in organisations not in org_unit_uuids
        positions = get_employee_positions(*e_connectors)
        if ou_filter:
            positions = list(
                filter(
                    lambda position: position["orgUnitUuid"] in org_unit_uuids,
                    positions,
                )
            )
            if not positions:
                continue

        def get_employee_name(employee: dict) -> str:
            name = employee["name"]
            if not use_nickname:
                return name
            nickname = employee.get("nickname")
            return nickname or name

        payload = {
            "extUuid": employee["uuid"],
            "userId": sam_account_name,
            "name": get_employee_name(employee),
            "email": get_employee_email(*e_connectors),
            "positions": positions,
        }
        converted_users.append(payload)

    return converted_users


@click.command()
@click.option(
    "--mora-base",
    default=load_setting("mora.base", "http://localhost:5000"),
    help="URL for OS2mo.",
)
@click.option(
    "--rollekatalog-url",
    default=load_setting("exporters.os2rollekatalog.rollekatalog.url"),
    help="URL for Rollekataloget.",
    required=True,
)
@click.option(
    "--rollekatalog-api-key",
    default=load_setting("exporters.os2rollekatalog.rollekatalog.api_token"),
    type=click.UUID,
    required=True,
    help="API key to write to Rollekataloget.",
)
@click.option(
    "--mo-root-org-unit",
    default=load_setting("exporters.os2rollekatalog.main_root_org_unit"),
    type=click.UUID,
    required=True,
    help=(
        "Root uuid in os2mo"
        "Also root in rollekataloget unless rollekatalog_root_uuid is specified."
        "Rollekataloget only supports one root org unit. "
        "All other root org units in OS2mo will be made children of this one."
        "Unless they are filtered by setting ou_filter=true"
    ),
)
@click.option(
    "--ou-filter",
    default=load_setting("exporters.os2rollekatalog.ou_filter", False),
    type=click.BOOL,
    help=(
        "Option to filter by mo_root_org_unit."
        "Only get org_units below mo_root_org_unit and employees in these org units."
        "Defaults to false (select every org unit and put other root units "
        "below main_root)"
    ),
)
@click.option(
    "--rollekatalog-root-uuid",
    default=load_setting("exporters.os2rollekatalog.rollekatalog_root_uuid", None),
    type=click.UUID,
    required=False,
    help=(
        "Root uuid in rollekataloget"
        "Optional setting if the root uuid in rollekataloget is different to MO"
    ),
)
@click.option(
    "--log-file-path",
    default="exports_mox_rollekatalog.log",
    type=click.Path(),
    help="Path to write log file.",
    envvar="MOX_ROLLE_LOG_FILE",
)
@click.option(
    "--mapping-file-path",
    default="cpr_mo_ad_map.csv",
    type=click.Path(exists=True),
    help="Path to the cpr_mo_ad_map.csv file.",
    envvar="MOX_ROLLE_MAPPING",
)
@click.option(
    "--use-nickname",
    default=load_setting("exporters.os2rollekatalog.use_nickname", False),
    type=click.BOOL,
    required=False,
    help=(
        "Use employee nicknames if available. Will use name if nickname is unavailable"
    ),
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dump payload, rather than writing to rollekataloget.",
)
def main(
    mora_base: str,
    rollekatalog_url: str,
    rollekatalog_api_key: UUID,
    mo_root_org_unit: UUID,
    ou_filter: bool,
    rollekatalog_root_uuid: UUID,
    log_file_path: str,
    mapping_file_path: str,
    use_nickname: bool,
    dry_run: bool,
):
    """OS2Rollekatalog exporter.

    Reads data from OS2mo and exports it to OS2Rollekatalog.
    Depends on cpr_mo_ad_map.csv from cpr_uuid.py to check users against AD.
    """
    # AD_SYSTEM_NAME = SETTINGS["exporters.os2rollekatalog.ad_system_name"]

    init_log(log_file_path)

    try:
        service_url = mora_base + "/service"
        mo_connector = mo_api.Connector(service_url, org_uuid=True)
        # Hack to introduce retrying into os2mo_tools
        mo_connector.mo_get = retry(
            reraise=True,
            wait=wait_exponential(multiplier=2, min=1),
            stop=stop_after_attempt(7),
        )(mo_connector.mo_get)
        # Hack in auth headers (keycloak) into os2mo_tools
        mo_connector.session.headers = TokenSettings().get_headers()
        # Must re-read org-uuid, as we passed True instead in constructor
        mo_connector.org_id = mo_connector._get_org()
    except requests.RequestException:
        logger.exception("An error occurred connecting to OS2mo")
        sys.exit(3)

    try:
        logger.info("Reading organisation")
        org_units = get_org_units(
            mo_connector, mo_root_org_unit, ou_filter, mapping_file_path
        )
    except requests.RequestException:
        logger.exception("An error occurred trying to fetch org units")
        sys.exit(3)
    logger.info("Found {} org units".format(len(org_units)))
    # Create a set of uuids for all org_units
    org_unit_uuids = set(org_units.keys())

    try:
        logger.info("Reading employees")
        users = get_users(
            mo_connector, mapping_file_path, org_unit_uuids, ou_filter, use_nickname
        )
    except requests.RequestException:
        logger.exception("An error occurred trying to fetch employees")
        sys.exit(3)
    logger.info("Found {} employees".format(len(users)))

    payload = {"orgUnits": list(org_units.values()), "users": users}
    # Option to replace root organisations uuid with one given in settings
    if rollekatalog_root_uuid:
        p = json.dumps(payload)
        p = p.replace(str(mo_root_org_unit), str(rollekatalog_root_uuid))
        payload = json.loads(p)

    if dry_run:
        print(json.dumps(payload, indent=4))
        sys.exit(0)

    try:
        logger.info("Writing to Rollekataloget")
        result = requests.post(
            rollekatalog_url,
            json=payload,
            headers={"ApiKey": str(rollekatalog_api_key)},
            verify=False,
        )
        logger.info(result.json())
        result.raise_for_status()
    except requests.RequestException:
        logger.exception("An error occurred when writing to Rollekataloget")
        sys.exit(3)


if __name__ == "__main__":
    main()
