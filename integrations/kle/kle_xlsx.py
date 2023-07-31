from abc import ABC

import click
import pandas as pd
import xlsxwriter.worksheet
from click_option_group import optgroup
from click_option_group import RequiredMutuallyExclusiveOptionGroup

from integrations.kle.kle_import_export import ASPECT_MAP
from integrations.kle.kle_import_export import Aspects
from integrations.kle.kle_import_export import KLEAnnotationIntegration


class KLEXLSXIntegration(KLEAnnotationIntegration, ABC):
    def __init__(self):
        super().__init__()

        self.xlsx_file = self.settings.integrations_kle_xlsx_file_path


class KLEXLSXExporter(KLEXLSXIntegration):
    """Export KLE annotation as CSV files bundled in a spreadsheet."""

    def __init__(self, multiple_responsible=False):
        self.multiple_responsible = multiple_responsible
        super().__init__()

    @staticmethod
    def write_rows(worksheet: xlsxwriter.worksheet.Worksheet, data: list):

        for index, row in enumerate(data):
            worksheet.write_row(index, 0, row)

    @staticmethod
    def get_org_unit_validation(column: str):
        return (
            "{0}1:{0}1048576".format(column),
            {"validate": "list", "source": "=Org!$B$2:$B$1048576"},
        )

    @staticmethod
    def get_kle_validation(column: str):
        return (
            "{0}1:{0}1048576".format(column),
            {"validate": "list", "source": "=KLE!$C$2:$C$1048576"},
        )

    @staticmethod
    def get_column_width(data, field: str):
        field_lengths = [len(row[field]) for row in data]
        return max(field_lengths)

    def add_org_unit_sheet(self, workbook, org_units):
        worksheet = workbook.add_worksheet(name="Org")

        rows = [(org_unit["uuid"], org_unit["combined"]) for org_unit in org_units]

        worksheet.set_column(0, 0, width=self.get_column_width(org_units, "uuid"))
        worksheet.set_column(1, 1, width=self.get_column_width(org_units, "combined"))

        rows.insert(0, ("UUID", "Navn"))

        self.write_rows(worksheet, rows)

    def add_kle_sheet(self, workbook: xlsxwriter.Workbook, kle_numbers: list):
        worksheet = workbook.add_worksheet(name="KLE")

        rows = [(kle["uuid"], kle["user_key"], kle["name"]) for kle in kle_numbers]

        rows.insert(0, ("UUID", "EmneNr", "EmneTitel"))

        worksheet.set_column(0, 0, width=self.get_column_width(kle_numbers, "uuid"))
        worksheet.set_column(1, 1, width=self.get_column_width(kle_numbers, "user_key"))
        worksheet.set_column(2, 2, width=self.get_column_width(kle_numbers, "name"))

        self.write_rows(worksheet, rows)

    def add_ansvarlig_sheet(self, workbook, kle_numbers, org_units):
        worksheet = workbook.add_worksheet(name="Ansvarlig")

        def calculate_level(kle_number: str):
            """
            We calculate the level, by how many dots are in the key
            E.g. 00 is 1, 00.01 is 2, 00.01.32 is 3
            """
            return str(kle_number.count(".") + 1)

        rows = [(kle["level"], kle["user_key"], kle["name"], "") for kle in kle_numbers]
        rows.insert(0, ("Niveau", "EmneNr", "EmneTitel", "EnhedNavn"))

        worksheet.data_validation(*self.get_org_unit_validation(column="D"))

        worksheet.set_column(1, 1, width=self.get_column_width(kle_numbers, "user_key"))
        worksheet.set_column(2, 2, width=self.get_column_width(kle_numbers, "name"))
        worksheet.set_column(3, 3, width=self.get_column_width(org_units, "combined"))

        self.write_rows(worksheet, rows)

    def add_kle_relation_sheet(self, sheet_name, workbook, kle_numbers, org_units):
        worksheet = workbook.add_worksheet(name=sheet_name)

        rows = [("EnhedNavn", "KLE")]

        worksheet.data_validation(*self.get_org_unit_validation("A"))
        worksheet.data_validation(*self.get_kle_validation("B"))

        worksheet.set_column(0, 0, width=self.get_column_width(org_units, "combined"))
        worksheet.set_column(1, 1, width=self.get_column_width(kle_numbers, "name"))

        self.write_rows(worksheet, rows)

    @staticmethod
    def convert_org_units(org_units):
        return [
            {
                "combined": "{} - {}".format(unit["name"], unit["uuid"]),
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
            return str(kle_number.count(".") + 1)

        return [
            {
                "level": calculate_level(kle["user_key"]),
                **kle,
            }
            for kle in kle_numbers
        ]

    def run(self):
        workbook = xlsxwriter.Workbook(self.xlsx_file)

        org_units = sorted(
            self.convert_org_units(self.get_all_org_units_from_mo()),
            key=lambda x: x["name"],
        )
        kle = sorted(
            self.convert_kle_numbers(self.get_kle_classes_from_mo()),
            key=lambda x: x["user_key"],
        )

        self.add_org_unit_sheet(workbook, org_units)
        self.add_kle_sheet(workbook, kle)

        if self.multiple_responsible:
            self.add_kle_relation_sheet("Ansvarlig", workbook, kle, org_units)
        else:
            self.add_ansvarlig_sheet(workbook, kle, org_units)

        self.add_kle_relation_sheet("Indsigt", workbook, kle, org_units)
        self.add_kle_relation_sheet("Udførende", workbook, kle, org_units)

        # Bold column headers for all sheets
        bold = workbook.add_format({"bold": 1})
        for sheet in workbook.worksheets():
            sheet.set_row(0, cell_format=bold)

        workbook.close()


class KLEXLSXImporter(KLEXLSXIntegration):
    def handle_sheet(
        self,
        sheet,
        org_unit_field,
        kle_field,
        data_map,
        org_unit_map,
        kle_map,
        kle_aspect,
    ):
        for row in sheet.iterrows():
            index, data = row

            kle_uuid = kle_map.get(data.__getattr__(kle_field))
            org_unit_uuid = org_unit_map.get(data.__getattr__(org_unit_field))

            if not kle_uuid or not org_unit_uuid:
                continue

            org_unit = data_map.setdefault(org_unit_uuid, {})
            org_unit.setdefault(kle_uuid, set()).add(kle_aspect)

    def run(self):

        # Read sheet
        xlsx_file = pd.ExcelFile(self.xlsx_file)

        sheets = {
            sheet_name: xlsx_file.parse(sheet_name)
            for sheet_name in xlsx_file.sheet_names
        }

        org_unit_map = {row.Navn: row.UUID for index, row in sheets["Org"].iterrows()}
        kle_map = {row.EmneTitel: row.UUID for index, row in sheets["KLE"].iterrows()}

        data_map = {}

        self.handle_sheet(
            sheets["Ansvarlig"],
            org_unit_field="EnhedNavn",
            kle_field="KLE",
            data_map=data_map,
            org_unit_map=org_unit_map,
            kle_map=kle_map,
            kle_aspect=Aspects.Ansvarlig,
        )
        self.handle_sheet(
            sheets["Indsigt"],
            org_unit_field="EnhedNavn",
            kle_field="KLE",
            data_map=data_map,
            org_unit_map=org_unit_map,
            kle_map=kle_map,
            kle_aspect=Aspects.Indsigt,
        )
        self.handle_sheet(
            sheets["Udførende"],
            org_unit_field="EnhedNavn",
            kle_field="KLE",
            data_map=data_map,
            org_unit_map=org_unit_map,
            kle_map=kle_map,
            kle_aspect=Aspects.Udfoerende,
        )

        payloads = self.generate_payloads(data_map)

        self.post_payloads_to_mo(payloads)

    def generate_payloads(self, data):
        aspect_classes = self.get_aspect_classes_from_mo()
        aspect_map = {
            ASPECT_MAP[clazz["scope"]]: clazz["uuid"] for clazz in aspect_classes
        }

        payloads = []
        for unit, info in data.items():
            for kle_uuid, aspects in info.items():

                aspect_uuids = [aspect_map[aspect] for aspect in aspects]

                payload = {
                    "type": "kle",
                    "org_unit": {"uuid": unit},
                    "kle_aspect": [{"uuid": uuid} for uuid in aspect_uuids],
                    "kle_number": {"uuid": kle_uuid},
                    "validity": {"from": "1920-01-01", "to": None},
                }
                payloads.append(payload)

        return payloads


@click.command()
@optgroup.group("Action", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option("--import", is_flag=True)
@optgroup.option("--export", is_flag=True)
@click.option("--multiple-responsible", is_flag=True)
def cli(**args):
    if args["import"]:
        importer = KLEXLSXImporter()
        importer.run()

    if args["export"]:
        exporter = KLEXLSXExporter(args["multiple_responsible"])
        exporter.run()


if __name__ == "__main__":
    cli()
