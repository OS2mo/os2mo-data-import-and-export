# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Dette job skal læse alle brugere der har tilknytning til Medarbejder-organisationen og skrive en raport i en xlsx fil.
# Det er lavet til Frederikshavns kommune der gerne vil se navn, email, rolle samt udvalg.
import json
import logging
import pathlib

import xlsxwriter
import xlsxwriter.worksheet
from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import (Adresse, Bruger, Enhed,
                                                 Tilknytning)
                                                 
from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker

settings = json.loads((pathlib.Path(".") / "settings/settings.json").read_text())

# logger = logging.getLogger("Frederikshavn_MED")
# for name in logging.root.manager.loggerDict:
#     if name in ('Frederikshavn_MED', ):
#         logging.getLogger(name).setLevel(logging.DEBUG)
#     else:
#         logging.getLogger(name).setLevel(logging.ERROR)

# logging.basicConfig(
#     format='%(levelname)s %(asctime)s %(name)s %(message)s',
#     level=logging.DEBUG
# )


def get_session(engine):
    return sessionmaker(bind=engine, autoflush=False)()


class XLSXExporter():
    # Lånt og tilrettet fra os2mo-data-import-and-export/integrations/kle/kle_xlsx.py
    def __init__(self, xlsx_file: str):
        self.xlsx_file = xlsx_file

    @staticmethod
    def write_rows(worksheet: xlsxwriter.worksheet.Worksheet, data: list):
        for index, row in enumerate(data):
            worksheet.write_row(index, 0, row.values())

    @staticmethod
    def get_column_width(data, field: str):
        field_length = max(len(row[field]) for row in data)
        return field_length

    def add_sheet(self, workbook, sheet: str, data: list):
        worksheet = workbook.add_worksheet(name=sheet)
        worksheet.autofilter('A1:D51')
        data.insert(0, {key: key for key in data[0].keys()})

        for index, key in enumerate(data[0].keys()):
            worksheet.set_column(
                index, index, width=self.get_column_width(data, key))

        bold = workbook.add_format({'bold': 1})
        worksheet.set_row(0, cell_format=bold)

        self.write_rows(worksheet, data)


class Report_MED:

    def __init__(self, session):
        self.session = session

    def run(self):
        # Find MED organisation
        hoved_enhed = self.session.query(Enhed).filter(
            Enhed.navn == "Hoved-MED").one()
        # Find under-enheder og læg deres uuid'er i et 2 sæt, et til at finde de næste underenheder og et til at samle alle
        under_enheder = self.session.query(Enhed).filter(
            Enhed.forældreenhed_uuid == hoved_enhed.uuid).all()
        under_enheder = set(enheder.uuid for enheder in under_enheder)
        # One list to rule them all
        enhedslisten = under_enheder.copy()

        # Så længe der kan findes nye underenheder lægges de i enhedslisten
        while len(under_enheder):
            under_enheder = self.session.query(Enhed).filter(
                Enhed.forældreenhed_uuid.in_(under_enheder)).all()
            under_enheder = set(enheder.uuid for enheder in under_enheder)
            enhedslisten.update(under_enheder)

        # Så slår vi op i databasen på alle de relevante tabeller og knytter dem sammen med filtre.
        # Desuden filtreres på uuid'erne fundet ovenfor, samt adressetype så vi kun får email.
        query = self.session.query(Enhed, Tilknytning, Bruger)\
            .filter(Enhed.uuid == Tilknytning.enhed_uuid)\
            .filter(Tilknytning.enhed_uuid.in_(enhedslisten))\
            .filter(Tilknytning.bruger_uuid == Bruger.uuid)\
            .order_by(Bruger.efternavn)

        data = []
        for row in query.all():
            email = self.session.query(Adresse).filter(
                Adresse.bruger_uuid == row.Bruger.uuid,
                or_(Adresse.synlighed_titel == None, Adresse.synlighed_titel != "Hemmelig")).first()
            if email is not None:
                email = email.værdi
            else:
                email = ''
            # Hardcoded format som det skal stå i excel filen.
            data.append({"Navn": "{} {}".format(row.Bruger.fornavn, row.Bruger.efternavn),
                         "Email": email,
                         "Tilknytningstype": row.Tilknytning.tilknytningstype_titel,
                         "Enhed": row.Enhed.navn
                         })

        return data


if __name__ == '__main__':

    session = get_session(get_engine())
    data = Report_MED(session).run()

    xlsx_file = 'Frederikshavn_MED.xlsx'
    workbook = xlsxwriter.Workbook(xlsx_file)
    excel = XLSXExporter(xlsx_file)
    excel.add_sheet(workbook, 'MED', data)
    workbook.close()
