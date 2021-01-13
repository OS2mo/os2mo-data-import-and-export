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
from more_itertools import prepend
from sqlalchemy import case, literal_column, or_
from sqlalchemy.orm import Bundle, sessionmaker

from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import Adresse, Bruger, Enhed, Tilknytning, Engagement
from reports.XLSXExporter import XLSXExporter

logger = logging.getLogger("Frederikshavn_MED")
for name in logging.root.manager.loggerDict:
    if name in ("Frederikshavn_MED",):
        logging.getLogger(name).setLevel(logging.DEBUG)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)
logging.basicConfig(
    format="%(levelname)s %(asctime)s %(name)s %(message)s",
    level=logging.DEBUG,
)


def list_MED_members(session, org_name: str) -> list:
    """Lists all members in organisation.

    returns a list of tuples with titles as first element and data on members in subsequent lists
    [("Navn", "Email", "Tilknytningstype", "Enhed"),
     ("Fornavn Efternavn", "email@example.com", "Formand", "Enhed")]
    """
    # Find MED organisation
    hoved_enhed = session.query(Enhed.uuid).filter(Enhed.navn == org_name).one()[0]
    # Find under-enheder og læg deres uuid'er i et 2 sæt, et til at finde de næste underenheder og et til at samle alle
    def find_children(enheder):
        """Return a set of children under :code:`enheder`."""
        under_enheder = (
            session.query(Enhed.uuid)
            .filter(Enhed.forældreenhed_uuid.in_(enheder))
            .all()
        )
        # query returns a list of tuples like [(uuid2,),(uuid2,)], so extract the first item in each.
        return set(enheder[0] for enheder in under_enheder)

    under_enheder = find_children(set([hoved_enhed]))
    alle_MED_enheder = under_enheder
    # Så længe der kan findes nye underenheder lægges de i alle_MED_enheder
    while under_enheder:
        under_enheder = find_children(under_enheder)
        alle_MED_enheder.update(under_enheder)
    # Så slår vi op i databasen på alle de relevante tabeller og knytter dem sammen med filtre.
    # Desuden filtreres på uuid'erne fundet ovenfor.

    Emails = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel == "Email",
            or_(
                Adresse.synlighed_titel == None,
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )

    query = (
        session.query(
            Bruger.fornavn + " " + Bruger.efternavn,
            Emails.c.værdi,
            Tilknytning.tilknytningstype_titel,
            Enhed.navn,
        )
        .filter(
            Enhed.uuid == Tilknytning.enhed_uuid,
            Tilknytning.enhed_uuid.in_(alle_MED_enheder),
            Tilknytning.bruger_uuid == Bruger.uuid,
        )
        .join(Emails, Emails.c.bruger_uuid == Bruger.uuid, isouter=True)
        .order_by(Bruger.efternavn)
    )
    data = query.all()
    data = list(prepend(("Navn", "Email", "Tilknytningstype", "Enhed"), data))
    return data


def list_employees(session, org_name: str) -> list:
    """Lists all members in organisation.

    returns a list of tuples with titles as first element and data on members in subsequent lists
    [("Navn", "Email", "Tilknytningstype", "Enhed"),
     ("Fornavn Efternavn", "email@example.com", "Formand", "Enhed")]
    """
    # Find MED organisation
    hoved_enhed = session.query(Enhed.uuid).filter(Enhed.navn == org_name).one()[0]
    # Find under-enheder og læg deres uuid'er i et 2 sæt, et til at finde de næste underenheder og et til at samle alle
    def find_children(enheder):
        """Return a set of children under :code:`enheder`."""
        under_enheder = (
            session.query(Enhed.uuid)
            .filter(Enhed.forældreenhed_uuid.in_(enheder))
            .all()
        )
        # query returns a list of tuples like [(uuid2,),(uuid2,)], so extract the first item in each.
        return set(enheder[0] for enheder in under_enheder)

    under_enheder = find_children(set([hoved_enhed]))
    alle_enheder = under_enheder
    # Så længe der kan findes nye underenheder lægges de i alle_enheder
    while under_enheder:
        under_enheder = find_children(under_enheder)
        alle_enheder.update(under_enheder)

    # Så slår vi op i databasen på alle de relevante tabeller og knytter dem sammen med filtre.
    # Desuden filtreres på uuid'erne fundet ovenfor.

    Emails = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
            Adresse.adressetype_titel == "Email",
            or_(
                Adresse.synlighed_titel == None,
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )
    Phonenr = (
        session.query(Adresse.værdi, Adresse.bruger_uuid)
        .filter(
                Adresse.adressetype_titel == "Telefon",
            or_(
                Adresse.synlighed_titel == None,
                Adresse.synlighed_titel != "Hemmelig",
            ),
        )
        .subquery()
    )
    query = (
        session.query(
            Bruger.fornavn + " " + Bruger.efternavn,
            Bruger.cpr,
            Emails.c.værdi,
            Phonenr.c.værdi,
            Enhed.navn,
            Engagement.stillingsbetegnelse_titel
            
        )
        .filter(
            Enhed.uuid == Engagement.enhed_uuid,
            Engagement.enhed_uuid.in_(alle_enheder),
            Engagement.bruger_uuid == Bruger.uuid,
        )
        .join(Emails, Emails.c.bruger_uuid == Bruger.uuid, isouter=True)
        .join(Phonenr, Phonenr.c.bruger_uuid == Bruger.uuid, isouter=True)
        .order_by(Bruger.efternavn)
    )
    data = query.all()
    data = list(prepend(("Navn", "cpr", "Email", "Telefon", "Enhed", "Stilling"), data))
    
    return data


def run_report(reporttype, sheetname: str, org_name:str,  xlsx_file: str):
    
    # Lav sqlalchemy session - databasenavnet hentes fra settings
    session = sessionmaker(bind=get_engine(), autoflush=False)()
    # Udtræk MED medlemmer fra databasen
    data = reporttype(session, org_name)

    # Skriv MED medlemmernes data i en xlsx fil
    workbook = xlsxwriter.Workbook(xlsx_file)
    excel = XLSXExporter(xlsx_file)
    excel.add_sheet(workbook, sheetname, data)
    workbook.close()


if __name__ == "__main__":
    # Læs fra settings
    settings = json.loads((pathlib.Path(".") / "settings/settings.json").read_text())
    org_name = settings["report.MED_org_name"]
    xlsx_file = settings["report.MED_members_file"]
    run_report(list_MED_members, 'MED', org_name, xlsx_file)
    run_report(list_employees, 'Ansatte', 'Frederikshavn Kommune', 'Ansatte.xlsx')
