import xlsxwriter
import xlsxwriter.worksheet


class XLSXExporter:
    """Exporter for writing xlsx files with autofilters and columnwidts ajusted to its content

    Accepts data in lists of lists where first lists contains the title of the columns, eg:
    [["Navn", "Email", "Tilknytningstype", "Enhed"]
    ["Fornavn Efternavn", "email@example.com", "Formand", "Enhed"]]
    """

    def __init__(self, xlsx_file: str):
        self.xlsx_file = xlsx_file

    @staticmethod
    def write_rows(worksheet: xlsxwriter.worksheet.Worksheet, data: list):
        for index, row in enumerate(data):
            worksheet.write_row(index, 0, row)

    @staticmethod
    def get_column_width(data, field: str):
        field_length = max(len(row[field]) for row in data)
        return field_length

    def add_sheet(self, workbook, sheet: str, data: list):
        worksheet = workbook.add_worksheet(name=sheet)
        worksheet.autofilter("A1:D51")

        for index, key in enumerate(data[0]):
            worksheet.set_column(
                index,
                index,
                width=max(len(val[index]) for val in data if val[index]),
            )

        bold = workbook.add_format({"bold": 1})
        worksheet.set_row(0, cell_format=bold)

        self.write_rows(worksheet, data)
