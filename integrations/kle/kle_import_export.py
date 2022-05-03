import functools
import json
import logging
import os
import pathlib
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
import datetime

import requests
from sqlalchemy.orm import sessionmaker

from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import KLE

from ra_utils.load_settings import load_settings
from os2mo_data_import.helpers import MoraHelper
LOG_FILE = 'opgavefordeler.log'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename=LOG_FILE)


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


def get_session(engine):
    return sessionmaker(bind=engine, autoflush=False)()


class KLEAnnotationIntegration(ABC):
    """Import and export of KLE annotation from or to an external source."""

    # XXX: This uses a simple inheritance based pattern. We might want to use
    # something like a Strategy here. However, maybe YAGNI.

    def __init__(self):
        
        self.settings = load_settings()

        self.mora_base = self.settings.get("mora.base")
        self.mora_session = self._get_mora_session(token=os.environ.get("SAML_TOKEN"))
        self.helper = MoraHelper(hostname=self.settings.get('mora.base'))
        self.org_uuid = self.helper.read_organisation()

        kle_classes = self.get_kle_classes_from_mo()
        self.kle_uuid_map = {item["user_key"]: item["uuid"] for item in kle_classes}

        aspect_classes = self.get_aspect_classes_from_mo()
        self.aspect_map = {
            ASPECT_MAP[clazz["scope"]]: clazz["uuid"] for clazz in aspect_classes
        }

    def _get_mora_session(self, token) -> requests.Session:
        s = requests.Session()
        if token is not None:
            s.headers.update({"SESSION": token})
        s.headers.update({"SESSION": token})
        s.verify = False
        return s

    def _get_mo_org_uuid(self) -> str:
        """
        Get the UUID of the organisation configured in OS2mo
        :return:
        """
        logger.info("Fetching Organisation UUID from OS2mo")
        r = self.mora_session.get("{}/service/o/".format(self.mora_base))
        r.raise_for_status()
        return r.json()[0]["uuid"]

    def get_kle_classes_from_mo(self) -> list:
        """Get all of the kle_number 'klasse' objects from OS2mo"""
        logger.info("Fetching KLE numbers from OS2mo")
        kle_numbers,  _ = self.helper.read_classes_in_facet('kle_number')
        return kle_numbers

    def get_aspect_classes_from_mo(self) -> list:
        """Get all of the kle_aspect 'klasse' objects from OS2mo"""
        logger.info("Fetching KLE aspect classes from OS2mo")
        kle_aspects,  _ = self.helper.read_classes_in_facet('kle_aspect')
        return kle_aspects

    def get_all_org_units_from_mo(self) -> list:
        """Get a list of all units from OS2mo"""
        logger.info("Fetching all org units from OS2mo")
        url = "{}/service/o/{}/ou/".format(self.mora_base, self.org_uuid)
        r = self.mora_session.get(url)
        r.raise_for_status()
        units = r.json()["items"]

        logger.info("Found {} units".format(len(units)))
        return units

    def post_payloads_to_mo(self, payloads: list):
        """Submit a list of details payloads to OS2mo"""
        logger.info("Posting payloads to OS2mo ")
        url = "{}/service/details/create".format(self.mora_base)

        r = self.mora_session.post(url, json=payloads, params={"force": 1})
        r.raise_for_status()

    @abstractmethod
    def run(self):
        """Implement this, normally to execute import or export."""
        pass


class OpgavefordelerImporter(KLEAnnotationIntegration):
    def __init__(self):
        super().__init__()
        self.opgavefordeler_url = self.settings.get(
            "integrations.os2opgavefordeler.url"
        )
        self.opgavefordeler_session = self._get_opgavefordeler_session(
            token=self.settings.get("integrations.os2opgavefordeler.token")
        )
        self.lc_session = get_session(get_engine())

    def _get_opgavefordeler_session(self, token) -> requests.Session:
        s = requests.Session()
        s.headers.update({"Authorization": "Basic {}".format(token)})
        return s

    def get_kle_info_from_opgavefordeler(self, kle_numbers: list) -> list:
        """Get all KLE-number info from OS2opgavefordeler

        This will give information on which unit is 'Ansvarlig' for a certain
        KLE-number.
        The API will perform inheritance and deduce the unit logically responsible
        for a certain number if no unit is directly responsible,
        so the result is filtered of all duplicates

        The resulting data has one entry per KLE-number and is on the form:
        [
          {
            "kle": {
              "number": "81",
              "name": "Kommunens personale"
            },
            "org": {
              "manager": {
                "name": "...",
                "email": "...",
                "esdhId": "c162842f-8036-411c-95ac-9c81042e9530",
                "esdhName": null,
                "initials": "...",
                "jobTitle": "..."
              },
              "businessKey": "99e2521a-c52d-4dfa-838f-b205184f3c00",
              "name": "...",
              "esdhId": "99e2521a-c52d-4dfa-838f-b205184f3c00",
              "esdhName": "...",
              "email": null,
              "phone": null,
              "foreignKeys": {}
            },
            "employee": null
          },
        ]
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

    def get_org_unit_info_from_opgavefordeler(self, org_units_uuids: list) -> list:
        """Get all org-unit info from OS2opgavefordeler

        This will give information about which KLE-numbers the unit has a
        'Udførende' and 'Indsigt' relationship with.

        Empty results are filtered

        The resulting data is on the form:
        [
          [
            "5d6fe93d-2cf0-4bc7-b29a-68c14f9ae681",
            {
              "PERFORMING": [
                "15"
              ],
              "INTEREST": [
                "15"
              ]
            }
          ],
          ...
        ]
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

    def get_ansvarlig_tuples(self):
        """Fetch KLE info from opgavefordeler and generate list of tuples
        mapping org unit uuids to KLE number uuids for the 'ansvarlig' aspect"""

        opgavefordeler_ansvarlig = self.get_kle_info_from_opgavefordeler(
            list(self.kle_uuid_map.keys())
        )
        return [
            (item["org"]["businessKey"], self.kle_uuid_map.get(item["kle"]["number"]))
            for item in opgavefordeler_ansvarlig
        ]

    def get_indsigt_and_udfoerende_tuples(self):
        """Fetch Org unit info from opgavefordeler and generate lists of tuples
        mapping org unit uuids to KLE number uuids for the 'indsigt' and
        'udfoerende' aspects"""

        org_unit_uuids = [unit["uuid"] for unit in self.get_all_org_units_from_mo()]
        org_unit_info = self.get_org_unit_info_from_opgavefordeler(org_unit_uuids)

        indsigt = [
            (org_unit_uuid, self.kle_uuid_map.get(kle))
            for org_unit_uuid, info in org_unit_info
            for kle in info.get("INTEREST")
            if kle in self.kle_uuid_map
        ]

        udfoerende = [
            (org_unit_uuid, self.kle_uuid_map.get(kle))
            for org_unit_uuid, info in org_unit_info
            for kle in info.get("PERFORMING")
            if kle in self.kle_uuid_map
        ]

        return indsigt, udfoerende

    def build_org_unit_maps(self):
        """Build a map of opgavefordeler data

        For each of Ansvarlig, Indsigt and Udførende, generate a map between
        org units and KLE-numbers they are associated to
        """

        def tuple_list_to_dict(d: dict, t: tuple):
            org_uuid, kle_uuid = t
            d.setdefault(org_uuid, []).append(kle_uuid)
            return d

        ansvarlig = self.get_ansvarlig_tuples()
        indsigt, udfoerende = self.get_indsigt_and_udfoerende_tuples()

        ansvarlig, indsigt, udfoerende = map(
            lambda x: functools.reduce(tuple_list_to_dict, x, dict()),
            [ansvarlig, indsigt, udfoerende],
        )

        return ansvarlig, indsigt, udfoerende

    def build_delete_payload(self, mo_uuid):
        return {
            "uuid": mo_uuid,
            "validity": {"from": "1920-01-01", "to": str(datetime.date.today())},
            "type": "kle",
        }

    def build_create_payload(self, org_unit_uuid, kle_number, kle_aspects):
        kle_payload = self.build_kle_payload(org_unit_uuid, kle_number, kle_aspects)

        payload = {
            **kle_payload,
            "type": "kle",
        }
        return payload

    def build_edit_payload(self, org_unit_uuid, kle_number, kle_aspects, obj_uuid):
        kle_payload = self.build_kle_payload(org_unit_uuid, kle_number, kle_aspects)

        payload = {
            "data": {
                **kle_payload,
            },
            "uuid": obj_uuid,
            "type": "kle",
        }
        return payload

    def build_kle_payload(self, org_unit_uuid, kle_uuid, kle_aspects):

        aspects_uuids = [self.aspect_map[aspect] for aspect in kle_aspects]

        payload = {
            "org_unit": {"uuid": org_unit_uuid},
            "kle_aspect": [{"uuid": uuid} for uuid in aspects_uuids],
            "kle_number": {"uuid": kle_uuid},
            "validity": {"from": "1920-01-01", "to": None},
        }
        return payload

    def convert_mo_kle_to_org_unit_map(self, mo_kle):
        """Convert KLE objects from SQL cache to a similar org_unit_map
        format as Opgavefordeler data, with added OS2mo object UUIDs

        The general structure is listed below

        {
            <ORG_UNIT_UUID>: {
                <KLE_NUMBER_UUID>: (
                    <MO_OBJECT_UUID>,
                    {
                        Aspects.XXX,
                        Aspects.YYY,
                    }
                ),
                ...
            },
            ...
        }
        """

        org_unit_map = {}

        aspect_map = {
            'Udførende': Aspects.Udfoerende,
            'Indsigt': Aspects.Indsigt,
            'Ansvarlig': Aspects.Ansvarlig,
        }

        for row in mo_kle:
            org_unit = org_unit_map.setdefault(row.enhed_uuid, {})
            _, aspects = org_unit.setdefault(row.kle_nummer_uuid, (row.uuid, set()))
            aspect = aspect_map[row.kle_aspekt_titel]
            aspects.add(aspect)

        return org_unit_map

    def convert_opgavefordeler_to_org_unit_map(
        self, ansvarlig: dict, indsigt: dict, udfoerende: dict
    ):
        """Convert the three aspect mappings to a map between org unit uuids,
        kle uuids and the associated aspects
        {
            <ORG_UNIT_UUID>: {
                <KLE_NUMBER_UUID>: {
                    Aspects.XXX,
                    Aspects.YYY,
                }
                ...
            },
            ...
        }
        """
        aspect_maps = {
            Aspects.Ansvarlig: ansvarlig,
            Aspects.Indsigt: indsigt,
            Aspects.Udfoerende: udfoerende,
        }

        org_unit_map = {}
        for aspect, aspect_map in aspect_maps.items():
            for org_uuid, kle_uuids in aspect_map.items():
                for kle_uuid in kle_uuids:
                    org_unit_map.setdefault(org_uuid, {}).setdefault(
                        kle_uuid, set()
                    ).add(aspect)

        return org_unit_map

    def create_diff(self, ansvarlig, indsigt, udfoerende):
        """Compare Opgavefordeler data with existing OS2mo data and
        determine what should be deleted, created and updated"""

        mo_kle = self.lc_session.query(KLE).all()
        mo_kle_org_unit_map = self.convert_mo_kle_to_org_unit_map(mo_kle)

        org_unit_map = self.convert_opgavefordeler_to_org_unit_map(
            ansvarlig, indsigt, udfoerende
        )

        # Compare every existing OS2mo KLE object with opgavefordeler
        # to see if an entry exists for every org_unit/kle_number pair.
        # If no longer in map, mark object UUID for deletion
        deleted_uuids = {
            row.uuid
            for row in mo_kle
            if not org_unit_map.get(row.enhed_uuid, {}).get(row.kle_nummer_uuid)
        }
        deleted = [self.build_delete_payload(uuid) for uuid in deleted_uuids]

        new = []
        updated = []

        # Compare every org_unit/kle_number pair in opgavefordeler map, with
        # OS2mo data.
        # If no data exists in OS2mo, mark as new
        # If data exists, but is different set of aspects, mark as update
        # else skip
        for unit, kle_info in org_unit_map.items():
            for kle_number, aspects in kle_info.items():
                mo_info_tuple = mo_kle_org_unit_map.get(unit, {}).get(kle_number)
                if not mo_info_tuple:
                    # New object
                    new.append(self.build_create_payload(unit, kle_number, aspects))
                else:
                    mo_obj_uuid, mo_aspects = mo_info_tuple
                    if mo_aspects != aspects:
                        # Updated object
                        updated.append(
                            self.build_edit_payload(
                                unit, kle_number, aspects, mo_obj_uuid
                            )
                        )

        return deleted, new, updated

    def handle_new(self, create_payloads: list):
        """Create KLE org functions"""

        logger.info("{} new KLE objects".format(len(create_payloads)))
        url = "{}/service/details/create".format(self.mora_base)

        r = self.mora_session.post(url, json=create_payloads, params={"force": 1})
        r.raise_for_status()

    def handle_update(self, edit_payloads: list):
        """Edit existing KLE org functions"""
        logger.info("{} updated KLE objects".format(len(edit_payloads)))
        url = "{}/service/details/edit".format(self.mora_base)

        r = self.mora_session.post(url, json=edit_payloads, params={"force": 1})
        r.raise_for_status()

    def handle_delete(self, delete_payloads: list):
        """Terminate KLE org functions"""

        logger.info("{} KLE objects to be terminated".format(len(delete_payloads)))
        url = "{}/service/details/terminate".format(self.mora_base)

        r = self.mora_session.post(url, json=delete_payloads, params={"force": 1})
        r.raise_for_status()

    def run(self):
        logger.info("Starting import")

        # Generate a map of data from opgavefordeler
        ansvarlig, indsigt, udfoerende = self.build_org_unit_maps()

        # Generate a diff towards OS2mo
        deleted, new, updated = self.create_diff(ansvarlig, indsigt, udfoerende)

        self.handle_new(new)
        self.handle_update(updated)
        self.handle_delete(deleted)

        logger.info("Done")


if __name__ == "__main__":
    importer = OpgavefordelerImporter()
    importer.run()
