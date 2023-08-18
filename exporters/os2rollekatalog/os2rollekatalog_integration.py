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
from operator import itemgetter
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Tuple
from uuid import UUID

import click
import requests
from more_itertools import bucket
from os2mo_helpers.mora_helpers import MoraHelper

from .config import RollekatalogSettings
from .titles import export_titles


logger = logging.getLogger(__name__)


class RollekatalogsExporter:
    def __init__(self, settings: RollekatalogSettings) -> None:
        self.settings = settings
        self.mh = self._get_mora_helper(settings.mora_base)

    def _get_mora_helper(self, mora_base):
        return MoraHelper(hostname=mora_base, export_ansi=False)

    @lru_cache(maxsize=None)
    def get_employee_mapping(
        self,
    ) -> Dict[str, Tuple[str, str]]:
        mapping_path = Path(self.settings.exporters_os2rollekatalog_mapping_file_path)
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

    def get_employee_from_map(self, employee_uuid: str) -> Tuple[str, str]:
        mapping = self.get_employee_mapping()

        if employee_uuid not in mapping:
            logger.critical(
                "Unable to find employee in mapping with UUID {}".format(employee_uuid)
            )
            sys.exit(3)
        return mapping[employee_uuid]

    def get_parent_org_unit_uuid(self, ou: dict) -> Optional[str]:

        if (
            UUID(ou["uuid"])
            == self.settings.exporters_os2rollekatalog_main_root_org_unit
        ):
            # This is the root, there are no parents
            return None

        parent = ou["parent"]
        if self.settings.exporters_os2rollekatalog_ou_filter:
            assert parent, f"The org_unit {ou['uuid']} should have been filtered"

        if parent:
            return parent["uuid"]
        # Rollekataloget only support one root org unit, so all other root org
        # units get put under the main one, if filtering is not active.
        return str(self.settings.exporters_os2rollekatalog_main_root_org_unit)

    def get_org_units(self) -> Dict[str, Dict[str, Any]]:
        org = self.mh.read_organisation()
        search_root = (
            self.settings.exporters_os2rollekatalog_main_root_org_unit
            if self.settings.exporters_os2rollekatalog_ou_filter
            else None
        )
        org_units = self.mh.read_ou_root(org, search_root)

        converted_org_units = {}
        for org_unit in org_units:

            org_unit_uuid = org_unit["uuid"]
            # Fetch the OU again, as the 'parent' field is missing in the data
            # when listing all org units
            ou = self.mh.read_ou(org_unit_uuid)

            def get_manager(org_unit_uuid):

                present = self.mh._mo_lookup(
                    org_unit_uuid, "ou/{}/details/manager?validity=present"
                )
                future = self.mh._mo_lookup(
                    org_unit_uuid, "ou/{}/details/manager?validity=future"
                )
                managers = present + future

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

                ad_guid, sam_account_name = self.get_employee_from_map(person["uuid"])
                # Only import users who are in AD
                if not ad_guid or not sam_account_name:
                    return {}

                return {"uuid": person["uuid"], "userId": sam_account_name}

            def get_kle(org_unit_uuid: str) -> Tuple[List[str], List[str]]:
                present = self.mh._mo_lookup(
                    org_unit_uuid, "ou/{}/details/kle?validity=present"
                )
                future = self.mh._mo_lookup(
                    org_unit_uuid, "ou/{}/details/kle?validity=future"
                )
                kles = present + future

                def get_kle_tuples(
                    kles: List[dict],
                ) -> Generator[Tuple[str, str], None, None]:
                    for kle in kles:
                        number = kle["kle_number"]["user_key"]
                        for aspect in kle["kle_aspect"]:
                            yield number, aspect["scope"]

                kle_tuples = get_kle_tuples(kles)
                buckets = bucket(kle_tuples, key=itemgetter(1))

                interest = map(itemgetter(0), buckets["INDSIGT"])
                performing = map(itemgetter(0), buckets["UDFOERENDE"])

                return list(interest), list(performing)

            kle_performing, kle_interest = get_kle(org_unit_uuid)

            payload = {
                "uuid": org_unit_uuid,
                "name": ou["name"],
                "parentOrgUnitUuid": self.get_parent_org_unit_uuid(ou),
                "manager": get_manager(org_unit_uuid),
                "klePerforming": kle_performing,
                "kleInterest": kle_interest,
            }
            converted_org_units[org_unit_uuid] = payload

        return converted_org_units

    def get_employee_engagements(self, employee_uuid):
        present = self.mh._mo_lookup(
            employee_uuid, "e/{}/details/engagement?validity=present"
        )
        future = self.mh._mo_lookup(
            employee_uuid, "e/{}/details/engagement?validity=future"
        )
        return present + future

    def get_employee_email(self, employee_uuid):
        present = self.mh._mo_lookup(
            employee_uuid, "e/{}/details/address?validity=present"
        )
        future = self.mh._mo_lookup(
            employee_uuid, "e/{}/details/address?validity=future"
        )
        addresses = present + future
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

    def convert_position(self, e: Dict):
        position = {
            "name": e["job_function"]["name"],
            "orgUnitUuid": e["org_unit"]["uuid"],
        }
        if self.settings.exporters_os2rollekatalog_sync_titles:
            position["titleUuid"] = e["job_function"]["uuid"]
        return position

    def get_users(self, org_unit_uuids) -> List[Dict[str, Any]]:
        # read mapping
        employees = self.mh.read_all_users()

        converted_users = []

        for employee in employees:
            employee_uuid = employee["uuid"]

            ad_guid, sam_account_name = self.get_employee_from_map(employee_uuid)

            # Only import users who are in AD
            if not ad_guid or not sam_account_name:
                continue

            # Read positions first to filter any persons with engagements
            # in organisations not in org_unit_uuids
            engagements = self.get_employee_engagements(employee_uuid)

            # Convert MO engagements to Rollekatalog positions
            converted_positions = map(self.convert_position, engagements)

            # Filter positions outside relevant orgunits
            positions = list(
                filter(
                    lambda position: position["orgUnitUuid"] in org_unit_uuids,
                    converted_positions,
                )
            )
            if not positions:
                continue

            def get_employee_name(employee: dict) -> str:
                name = employee["name"]
                if not self.settings.exporters_os2rollekatalog_use_nickname:
                    return name
                nickname = employee.get("nickname")
                return nickname or name

            payload = {
                "extUuid": employee["uuid"],
                "userId": sam_account_name,
                "name": get_employee_name(employee),
                "email": self.get_employee_email(employee_uuid),
                "positions": positions,
            }
            converted_users.append(payload)

        return converted_users


@click.command()
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Dump payload, rather than writing to rollekataloget.",
)
def main(
    dry_run: bool,
):
    """OS2Rollekatalog exporter.

    Reads data from OS2mo and exports it to OS2Rollekatalog.
    Depends on cpr_mo_ad_map.csv from cpr_uuid.py to check users against AD.
    """
    settings = RollekatalogSettings()
    settings.start_logging_based_on_settings()

    rollekatalog_exporter = RollekatalogsExporter(settings=settings)

    if settings.exporters_os2rollekatalog_sync_titles:
        export_titles(
            mora_base=settings.mora_base,
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            auth_realm=settings.auth_realm,
            auth_server=settings.auth_server,
            rollekatalog_url=settings.exporters_os2rollekatalog_rollekatalog_url,
            rollekatalog_api_key=settings.exporters_os2rollekatalog_rollekatalog_api_key,
            dry_run=dry_run,
        )

    try:
        logger.info("Reading organisation")
        org_units = rollekatalog_exporter.get_org_units()
    except requests.RequestException:
        logger.exception("An error occurred trying to fetch org units")
        sys.exit(3)
    logger.info("Found {} org units".format(len(org_units)))
    # Create a set of uuids for all org_units
    org_unit_uuids = set(org_units.keys())

    try:
        logger.info("Reading employees")
        users = rollekatalog_exporter.get_users(org_unit_uuids)
    except requests.RequestException:
        logger.exception("An error occurred trying to fetch employees")
        sys.exit(3)
    logger.info("Found {} employees".format(len(users)))

    payload = {"orgUnits": list(org_units.values()), "users": users}
    # Option to replace root organisations uuid with one given in settings
    if settings.exporters_os2rollekatalog_rollekatalog_root_uuid:
        p = json.dumps(payload)
        p = p.replace(
            str(settings.exporters_os2rollekatalog_main_root_org_unit),
            str(settings.exporters_os2rollekatalog_rollekatalog_root_uuid),
        )
        payload = json.loads(p)

    if dry_run:
        print(json.dumps(payload, indent=4))
        sys.exit(0)

    try:
        logger.info("Writing to Rollekataloget")
        result = requests.post(
            settings.exporters_os2rollekatalog_rollekatalog_url,
            json=payload,
            headers={
                "ApiKey": str(settings.exporters_os2rollekatalog_rollekatalog_api_key)
            },
            verify=False,
        )
        logger.info(result.json())
        result.raise_for_status()
    except requests.RequestException:
        logger.exception("An error occurred when writing to Rollekataloget")
        sys.exit(3)


if __name__ == "__main__":
    main()
