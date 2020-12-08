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
from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker

from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import Adresse, Bruger, Enhed, Tilknytning

logger = logging.getLogger("Frederikshavn_MED")
for name in logging.root.manager.loggerDict:
    if name in ("Frederikshavn_MED",):
        logging.getLogger(name).setLevel(logging.DEBUG)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)
logging.basicConfig(
    format="%(levelname)s %(asctime)s %(name)s %(message)s", level=logging.DEBUG
)


def get_session(engine):
    return sessionmaker(bind=engine, autoflush=False)()


class XLSXExporter:
    # Lånt og tilrettet fra os2mo-data-import-and-export/integrations/kle/kle_xlsx.py
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
                index, index, width=max(len(val[index]) for val in data)
            )

        bold = workbook.add_format({"bold": 1})
        worksheet.set_row(0, cell_format=bold)

        self.write_rows(worksheet, data)


class Report_MED:
    def __init__(self, session, org_name: str):
        self.session = session
        self.org_name = org_name

    def run(self) -> list:
        # Find MED organisation
        hoved_enhed = (
            self.session.query(Enhed).filter(Enhed.navn == self.org_name).one()
        )
        # Find under-enheder og læg deres uuid'er i et 2 sæt, et til at finde de næste underenheder og et til at samle alle
        under_enheder = (
            self.session.query(Enhed)
            .filter(Enhed.forældreenhed_uuid == hoved_enhed.uuid)
            .all()
        )
        under_enheder = set(enheder.uuid for enheder in under_enheder)

        alle_MED_enheder = under_enheder.copy()

        # Så længe der kan findes nye underenheder lægges de i alle_MED_enheder
        while len(under_enheder):
            under_enheder = (
                self.session.query(Enhed)
                .filter(Enhed.forældreenhed_uuid.in_(under_enheder))
                .all()
            )
            under_enheder = set(enheder.uuid for enheder in under_enheder)
            alle_MED_enheder.update(under_enheder)

        # Så slår vi op i databasen på alle de relevante tabeller og knytter dem sammen med filtre.
        # Desuden filtreres på uuid'erne fundet ovenfor.
        query = (
            self.session.query(Enhed, Tilknytning, Bruger)
            .filter(Enhed.uuid == Tilknytning.enhed_uuid)
            .filter(Tilknytning.enhed_uuid.in_(alle_MED_enheder))
            .filter(Tilknytning.bruger_uuid == Bruger.uuid)
            .order_by(Bruger.efternavn)
        )

        # Nu laves en liste med lister hvori data placeres. For hver bruger laves et opslag for at finde email.
        data = []
        for i, row in enumerate(query.all()):
            email = (
                self.session.query(Adresse)
                .filter(
                    Adresse.bruger_uuid == row.Bruger.uuid,
                    Adresse.adressetype_titel == "Email",
                    or_(
                        Adresse.synlighed_titel == None,
                        Adresse.synlighed_titel != "Hemmelig",
                    ),
                )
                .first()
            )
            if email is not None:
                email = email.værdi
            else:
                email = ""

            data.append([])
            data[i] = [
                "{} {}".format(row.Bruger.fornavn, row.Bruger.efternavn),
                email,
                row.Tilknytning.tilknytningstype_titel,
                row.Enhed.navn,
            ]
        # indsæt titel række
        data.insert(0, ["Navn", "Email", "Tilknytningstype", "Enhed"])

        return data


if __name__ == "__main__":
    # Læs fra settins
    settings = json.loads((pathlib.Path(".") / "settings/settings.json").read_text())
    org_name = settings["report.MED_org_name"]
    xlsx_file = settings["report.MED_members_file"]
    # Lav sqlalchemy session - databasen er sat i settings
    session = get_session(get_engine())
    # Udtræk MED medlemmer fra databasen
    data = Report_MED(session, org_name).run()

    # Skriv MED medlemmernes data i en xlsx fil
    workbook = xlsxwriter.Workbook(xlsx_file)
    excel = XLSXExporter(xlsx_file)
    excel.add_sheet(workbook, "MED", data)
    workbook.close()
