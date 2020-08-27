import json
import logging
import os
import pathlib
from enum import Enum
import collections
import tempfile
import csv
from abc import ABC, abstractmethod

import requests
import pandas as pd
import xlsxwriter
import xlsxwriter.worksheet

LOG_FILE = 'opgavefordeler.log'

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    filename=LOG_FILE
)


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


class KLEAnnotationIntegration(ABC):
    """Import and export of KLE annotation from or to an external source."""

    # XXX: This uses a simple inheritance based pattern. We might want to use
    # something like a Strategy here. However, maybe YAGNI.

    def __init__(self):
        cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
        if not cfg_file.is_file():
            raise Exception("No settings file")
        self.settings = json.loads(cfg_file.read_text())

        self.mora_base = self.settings.get("mora.base")
        self.mora_session = self._get_mora_session(
            token=os.environ.get("SAML_TOKEN")
        )
        self.org_uuid = self._get_mo_org_uuid()

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
        url = "{}/service/o/{}/f/kle_number"
        r = self.mora_session.get(url.format(self.mora_base, self.org_uuid))
        r.raise_for_status()

        items = r.json()["data"]["items"]
        logger.info("Found {} items".format(len(items)))
        return items

    def get_aspect_classes_from_mo(self) -> list:
        """Get all of the kle_aspect 'klasse' objects from OS2mo"""
        logger.info("Fetching KLE aspect classes from OS2mo")
        url = "{}/service/o/{}/f/kle_aspect"
        r = self.mora_session.get(url.format(self.mora_base, self.org_uuid))
        r.raise_for_status()

        items = r.json()["data"]["items"]
        logger.info("Found {} items".format(len(items)))
        return items

    def get_all_org_units_from_mo(self) -> list:
        """Get a list of all units from OS2mo"""
        logger.info("Fetching all org units from OS2mo")
        url = "{}/service/o/{}/ou".format(self.mora_base, self.org_uuid)
        r = self.mora_session.get(url)
        r.raise_for_status()
        units = r.json()["items"]

        logger.info("Found {} units".format(len(units)))
        return units

    @abstractmethod
    def run(self):
        """Implement this, normally to execute import or export."""
        pass


class KLECSVExporter(KLEAnnotationIntegration):
    """Export KLE annotation as CSV files bundled in a spreadsheet."""

    @staticmethod
    def write_rows(worksheet: xlsxwriter.worksheet.Worksheet, data: list):

        for index, row in enumerate(data):
            worksheet.write_row(index, 0, row)

    @staticmethod
    def get_org_unit_validation(column: str):
        return (
            '{0}1:{0}1048576'.format(column),
            {
                'validate': 'list',
                'source': '=Org!$B$2:$B$1048576'
            }
        )

    @staticmethod
    def get_kle_validation(column: str):
        return (
            '{0}1:{0}1048576'.format(column),
            {
                'validate': 'list',
                'source': '=KLE!$C$2:$C$1048576'
            }
        )

    @staticmethod
    def get_column_width(data, field: str):
        field_lengths = [len(row[field]) for row in data]
        return max(field_lengths)

    def add_org_unit_sheet(self, workbook, org_units):
        worksheet = workbook.add_worksheet(name='Org')

        rows = [
            (org_unit['uuid'], org_unit['combined'])
            for org_unit in org_units
        ]

        worksheet.set_column(0, 0, width=self.get_column_width(org_units, 'uuid'))
        worksheet.set_column(1, 1, width=self.get_column_width(org_units, 'combined'))

        rows.insert(0, ('UUID', 'Navn'))

        self.write_rows(worksheet, rows)

    def add_kle_sheet(self, workbook: xlsxwriter.Workbook, kle_numbers: list):
        worksheet = workbook.add_worksheet(name='KLE')

        rows = [
            (kle['uuid'], kle['user_key'], kle['name'])
            for kle in kle_numbers
        ]

        rows.insert(0, ('UUID', 'EmneNr', 'EmneTitel'))

        worksheet.set_column(0, 0, width=self.get_column_width(kle_numbers, 'uuid'))
        worksheet.set_column(1, 1, width=self.get_column_width(kle_numbers, 'user_key'))
        worksheet.set_column(2, 2, width=self.get_column_width(kle_numbers, 'name'))

        self.write_rows(worksheet, rows)

    def add_ansvarlig_sheet(self, workbook, kle_numbers, org_units):
        worksheet = workbook.add_worksheet(name='Ansvarlig')

        def calculate_level(kle_number: str):
            """
            We calculate the level, by how many dots are in the key
            E.g. 00 is 1, 00.01 is 2, 00.01.32 is 3
            """
            return str(kle_number.count('.') + 1)

        rows = [
            (kle['level'], kle['user_key'], kle['name'], '')
            for kle in kle_numbers
        ]
        rows.insert(0, ('Niveau', 'EmneNr', 'EmneTitel', 'EnhedNavn'))

        worksheet.data_validation(
            *self.get_org_unit_validation(column='D')
        )

        worksheet.set_column(1, 1, width=self.get_column_width(kle_numbers, 'user_key'))
        worksheet.set_column(2, 2, width=self.get_column_width(kle_numbers, 'name'))
        worksheet.set_column(3, 3, width=self.get_column_width(org_units, 'combined'))

        self.write_rows(worksheet, rows)

    def add_indsigt_and_udfoerende_sheet(self, workbook, kle_numbers, org_units):
        for sheet_name in ['Indsigt', 'Udførende']:
            worksheet = workbook.add_worksheet(name=sheet_name)

            rows = [
                ('EnhedNavn', 'KLE')
            ]

            worksheet.data_validation(*self.get_org_unit_validation('A'))
            worksheet.data_validation(*self.get_kle_validation('B'))

            worksheet.set_column(0, 0, width=self.get_column_width(org_units,
                                                                   'combined'))
            worksheet.set_column(1, 1, width=self.get_column_width(kle_numbers,
                                                                   'name'))

            self.write_rows(worksheet, rows)

    @staticmethod
    def convert_org_units(org_units):
        return [
            {
                'combined': "{} - {}".format(unit['name'], unit['uuid']),
                **unit,
            }
            for unit in org_units
        ]

    @staticmethod
    def convert_kle_numbers(kle_numbers):
        def calculate_level(kle_number: str):
            """
            We calculate the level, by how many dots are in the key
            E.g. 00 is 1, 00.01 is 2, 00.01.32 is 3
            """
            return str(kle_number.count('.') + 1)

        return [
            {
                'level': calculate_level(kle['user_key']),
                **kle,
            }
            for kle in kle_numbers
        ]

    def run(self):
        xlsx_output = './test.xlsx'
        workbook = xlsxwriter.Workbook(xlsx_output)

        org_units = sorted(self.convert_org_units(self.get_all_org_units_from_mo()), key=lambda x: x['name'])
        kle = sorted(self.convert_kle_numbers(self.get_kle_classes_from_mo()), key=lambda x: x['user_key'])

        self.add_org_unit_sheet(workbook, org_units)
        self.add_kle_sheet(workbook, kle)
        self.add_ansvarlig_sheet(workbook, kle, org_units)
        self.add_indsigt_and_udfoerende_sheet(workbook, kle, org_units)

        # Bold column headers for all sheets
        bold = workbook.add_format({'bold': 1})
        for sheet in workbook.worksheets():
            sheet.set_row(0, cell_format=bold)

        workbook.close()


class KLEAnnotationImporter(KLEAnnotationIntegration, ABC):
    """Import KLE annotation from external source."""

    @abstractmethod
    def get_kle_from_source(self, kle_numbers: list) -> list:
        pass

    @abstractmethod
    def get_org_unit_info_from_source(self, org_units_uuids: list) -> list:
        pass

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

    def run(self):
        logger.info("Starting import")

        # Map of org_units to KLE-numbers, divided in the three sub-categories
        org_unit_map = {}
        kle_classes = self.get_kle_classes_from_mo()

        # Ansvarlig
        kle_numbers = [item["user_key"] for item in kle_classes]
        kle_info = self.get_kle_from_source(kle_numbers)
        self.add_ansvarlig(org_unit_map, kle_info)

        # Indsigt og Udfører
        org_units = self.get_all_org_units_from_mo()
        org_unit_uuids = [unit['uuid'] for unit in org_units]
        org_unit_info = self.get_org_unit_info_from_source(org_unit_uuids)
        self.add_indsigt_and_udfoerer(org_unit_map, org_unit_info)

        # Insert into MO
        aspect_classes = self.get_aspect_classes_from_mo()
        payloads = self.create_payloads(org_unit_map, kle_classes, aspect_classes)
        self.post_payloads_to_mo(payloads)

        logger.info("Done")

    def post_payloads_to_mo(self, payloads: list):
        """Submit a list of details payloads to OS2mo"""
        logger.info("Posting payloads to OS2mo ")
        url = "{}/service/details/create".format(self.mora_base)

        r = self.mora_session.post(url, json=payloads, params={"force": 1})
        r.raise_for_status()


class KLECSVImporter(KLEAnnotationImporter):

    def get_org_unit_info_from_source(self, org_units_uuids: list) -> list:
        pass

    def get_kle_from_source(self, kle_numbers: list) -> list:
        pass

    def run(self):

        # Read sheet
        xlsx_file = pd.ExcelFile('test.xslx')

        sheets = {
            sheet_name: xlsx_file.parse(sheet_name)
            for sheet_name in xlsx_file.sheet_names
        }



class OpgavefordelerImporter(KLEAnnotationImporter):
    def __init__(self):
        super().__init__()
        self.opgavefordeler_url = self.settings.get(
            "integrations.os2opgavefordeler.url"
        )
        self.opgavefordeler_session = self._get_opgavefordeler_session(
            token=self.settings.get("integrations.os2opgavefordeler.token")
        )

    def _get_opgavefordeler_session(self, token) -> requests.Session:
        s = requests.Session()
        s.headers.update({"Authorization": "Basic {}".format(token)})
        return s

    def get_kle_from_source(self, kle_numbers: list) -> list:
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

    def get_org_unit_info_from_source(self, org_units_uuids: list) -> list:
        """
        Get all org-unit info from OS2opgavefordeler

        This will give information about which KLE-numbers the unit has a
        'Udførende' and 'Indsigt' relationship with.

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


if __name__ == "__main__":
    # importer = OpgavefordelerImporter()
    # importer.run()

    # exporter = KLECSVExporter()
    # exporter.run()

    importer = KLECSVImporter()
    importer.run()
