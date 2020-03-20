import json
import logging
import os
import pathlib
from enum import Enum

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Aspects(Enum):
    Indsigt = 1
    Udfoerende = 2
    Ansvarlig = 3


# Maps between the enum and scopes on the classes from the aspect facet
ASPECT_MAP = {
    "INDSIGT": Aspects.Indsigt,
    "UDFOERENDE": Aspects.Udfoerende,
    "ANSVARLIG": Aspects.Ansvarlig,
}


class OpgavefordelerImporter:
    def __init__(self):
        cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
        if not cfg_file.is_file():
            raise Exception("No setting file")
        self.settings = json.loads(cfg_file.read_text())

        self.mora_base = self.settings.get("mora.base")
        self.mora_session = self._get_mora_session(token=os.environ.get("SAML_TOKEN"))

        self.opgavefordeler_url = self.settings.get(
            "integrations.os2opgavefordeler.url"
        )
        self.opgavefordeler_session = self._get_opgavefordeler_session(
            token=self.settings.get("integrations.os2opgavefordeler.token")
        )
        self.org_uuid = self._get_mo_org_uuid()

    def _get_mora_session(self, token) -> requests.Session:
        s = requests.Session()
        s.headers.update({"SESSION": token})
        return s

    def _get_opgavefordeler_session(self, token) -> requests.Session:
        s = requests.Session()
        s.headers.update({"Authorization": "Basic {}".format(token)})
        return s

    def _get_mo_org_uuid(self) -> str:
        """
        Get the UUID of the organisation configured in OS2mo
        :return:
        """
        logger.info("Fetching Organisation UUID from OS2mo")
        r = requests.get("{}/service/o/".format(self.mora_base))
        r.raise_for_status()
        return r.json()[0]["uuid"]

    def get_kle_classes_from_mo(self) -> list:
        """Get all of the kle_number 'klasse' objects from OS2mo"""
        logger.info("Fetching KLE numbers from OS2mo")
        url = "{}/service/o/{}/f/kle_number"
        r = requests.get(url.format(self.mora_base, self.org_uuid))
        r.raise_for_status()

        items = r.json()["data"]["items"]
        logger.info("Found {} items".format(len(items)))
        return items

    def get_aspect_classes_from_mo(self) -> list:
        """Get all of the kle_aspect 'klasse' objects from OS2mo"""
        logger.info("Fetching KLE aspect classes from OS2mo")
        url = "{}/service/o/{}/f/kle_aspect"
        r = requests.get(url.format(self.mora_base, self.org_uuid))
        r.raise_for_status()

        items = r.json()["data"]["items"]
        logger.info("Found {} items".format(len(items)))
        return items

    def get_kle_from_opgavefordeler(self, kle_numbers: list) -> list:
        """
        Get all KLE-number info from OS2opgavefordeler

        This will give information on which unit is 'Ansvarlig' for a certain
        KLE-number.
        The API will perform inheritance and deduce the unit logically responsible
        for a certain number if no unit is directly responsible,
        so the result is filtered of all duplicates
        """
        logger.info("Fetching KLE info from OS2opgavefordeler")

        url = "{}/TopicRouter/api".format(self.opgavefordeler_url)
        s = self.opgavefordeler_session

        unit_data = []
        for key in kle_numbers:
            try:
                r = s.get(url, params={"kle": key})
                r.raise_for_status()
                unit_data.append(r.json())
            except requests.exceptions.HTTPError:
                logger.warning("KLE number '{}' not found".format(key))

        seen_keys = set()
        filtered = []
        for item in unit_data:
            key = item["kle"]["number"]
            if key not in seen_keys:
                filtered.append(item)
                seen_keys.add(key)

        logger.info("Found {} items".format(len(filtered)))
        return filtered

    def get_org_units_from_mo(self) -> list:
        """Get a list of all units from OS2mo"""
        logger.info("Fetching all org units from OS2mo")
        url = "{}/service/o/{}/ou".format(self.mora_base, self.org_uuid)
        r = requests.get(url)
        r.raise_for_status()
        units = [unit["uuid"] for unit in r.json()["items"]]

        logger.info("Found {} units".format(len(units)))
        return units

    def get_org_unit_info_from_opgavefordeler(self, org_units_uuids: list) -> list:
        """
        Get all org-unit info from OS2opgavefordeler

        This will give information about which KLE-numbers the unit has a
        'Udførende' and 'Indsigt' relationship with

        Empty results are filtered
        """
        logger.info("Fetching org unit info from OS2opgavefordeler")
        url = "{}/TopicRouter/api/ou/{}"
        s = self.opgavefordeler_session
        org_unit_info = {}
        for uuid in org_units_uuids:
            try:
                r = s.get(url.format(self.opgavefordeler_url, uuid))
                r.raise_for_status()
                org_unit_info[uuid] = r.json()
                logger.debug("Adding {}".format(uuid))
            except requests.exceptions.HTTPError:
                continue

        def filter_empty(item):
            info = item[1]
            return info["INTEREST"] or info["PERFORMING"]

        filtered = list(filter(filter_empty, org_unit_info.items()))

        logger.info("Found {} items".format(len(filtered)))
        return filtered

    def add_indsigt_and_udfoerer(self, org_unit_map: dict, org_unit_info: list):
        """Add 'Indsigt' and 'Udførende' to the org unit map"""
        logger.info('Adding "Indsigt" and "Udførende"')
        for item in org_unit_info:
            org_unit_uuid, info = item
            org_unit = org_unit_map.setdefault(org_unit_uuid, {})
            for key in info["PERFORMING"]:
                values = org_unit.setdefault(key, set())
                values.add(Aspects.Udfoerende)
            for key in info["INTEREST"]:
                values = org_unit.setdefault(key, set())
                values.add(Aspects.Indsigt)

    def add_ansvarlig(self, org_unit_map: dict, kle_info: list):
        """Add ansvarlig to the org unit map"""
        logger.info('Adding "Ansvarlig"')
        for item in kle_info:
            key = item["kle"]["number"]
            org_unit_uuid = item["org"]["businessKey"]
            org_unit = org_unit_map.setdefault(org_unit_uuid, {})
            values = org_unit.setdefault(key, set())
            values.add(Aspects.Ansvarlig)

    def create_payloads(
        self, org_unit_map: dict, kle_classes: list, aspect_classes: list
    ) -> list:
        """Given the org unit map, create a list of OS2mo payloads"""
        logger.info("Creating payloads")

        kle_uuid_map = {item["user_key"]: item["uuid"] for item in kle_classes}
        aspect_map = {
            ASPECT_MAP[clazz["scope"]]: clazz["uuid"] for clazz in aspect_classes
        }
        payloads = []
        for unit, info in org_unit_map.items():
            for number, aspects in info.items():

                kle_uuid = kle_uuid_map.get(number)
                if not kle_uuid:
                    logger.warning("KLE number '{}' doesn't exist".format(number))
                    continue

                aspects_uuids = [aspect_map[aspect] for aspect in aspects]

                payload = {
                    "type": "kle",
                    "org_unit": {"uuid": unit},
                    "kle_aspect": [{"uuid": uuid} for uuid in aspects_uuids],
                    "kle_number": {"uuid": kle_uuid_map[number]},
                    "validity": {"from": "1920-01-01", "to": None},
                }
                payloads.append(payload)

        return payloads

    def post_payloads_to_mo(self, payloads: list):
        """Submit a list of details payloads to OS2mo"""
        logger.info("Posting payloads to OS2mo ")
        url = "{}/service/details/create".format(self.mora_base)

        r = requests.post(url, json=payloads, params={"force": 1})
        r.raise_for_status()

    def run(self):
        logger.info("Starting import")

        # Map of org_units to KLE-numbers, divided in the three sub-categories
        org_unit_map = {}
        kle_classes = self.get_kle_classes_from_mo()

        # Ansvarlig
        kle_numbers = [item["user_key"] for item in kle_classes]
        kle_info = self.get_kle_from_opgavefordeler(kle_numbers)
        self.add_ansvarlig(org_unit_map, kle_info)

        # Indsigt og Udfører
        org_units = self.get_org_units_from_mo()
        org_unit_info = self.get_org_unit_info_from_opgavefordeler(org_units)
        self.add_indsigt_and_udfoerer(org_unit_map, org_unit_info)

        # Insert into MO
        aspect_classes = self.get_aspect_classes_from_mo()
        payloads = self.create_payloads(org_unit_map, kle_classes, aspect_classes)
        self.post_payloads_to_mo(payloads)

        logger.info("Done")


if __name__ == "__main__":
    importer = OpgavefordelerImporter()
    importer.run()
