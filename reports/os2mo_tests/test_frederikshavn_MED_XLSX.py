import tempfile
import unittest

from openpyxl import load_workbook
from sqlalchemy import create_engine

from exporters.sql_export.sql_table_defs import (
    Adresse,
    Base,
    Bruger,
    Enhed,
    Tilknytning,
)
from reports.Frederikshavn_MED import *


class Tests_xlxs(unittest.TestCase):
    def setUp(self):
        f = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        self.xlsfilename = f.name
        self.data = [
            ["Navn", "Email", "Tilknytningstype", "Enhed"],
            ["Fornavn Efternavn", "email@email.com", "Formand", "Testenhed"],
        ]

        workbook = xlsxwriter.Workbook(self.xlsfilename)
        excel = XLSXExporter(self.xlsfilename)
        excel.add_sheet(workbook, "MED", self.data)
        workbook.close()

    def test_read(self):
        wb = load_workbook(filename=self.xlsfilename)
        ws = wb["MED"]
        header = [i[0].value for i in ws.columns]
        content = [i[1].value for i in ws.columns]
        self.assertEqual(header, ["Navn", "Email", "Tilknytningstype", "Enhed"])
        self.assertEqual(
            content, ["Fornavn Efternavn", "email@email.com", "Formand", "Testenhed"]
        )


class Tests_db(unittest.TestCase):
    def setUp(self):
        self.engine = get_engine(dbpath=":memory:")
        self.session = get_session(self.engine)
        # Sikrer at der startes fra en tom database
        Base.metadata.drop_all(self.engine)

        # Lav tables via tabledefs fra LoraCache og fyld dataen ind
        Base.metadata.create_all(self.engine)
        enhed = Enhed(navn="Hoved-MED", uuid="E1", enhedstype_titel="org_unit_type")
        self.session.add(enhed)
        enhed = Enhed(
            navn="Under-MED",
            uuid="E2",
            enhedstype_titel="org_unit_type",
            forældreenhed_uuid="E1",
        )
        self.session.add(enhed)
        enhed = Enhed(
            navn="Under-under-MED",
            uuid="E3",
            enhedstype_titel="org_unit_type",
            forældreenhed_uuid="E2",
        )
        self.session.add(enhed)
        bruger = Bruger(
            fornavn="fornavn",
            efternavn="efternavn",
            uuid="b1",
            bvn="b1bvn",
            cpr="cpr",
        )
        self.session.add(bruger)
        tilknytning = Tilknytning(
            uuid="t1",
            bvn="t1bvn",
            bruger_uuid="b1",
            enhed_uuid="E2",
            tilknytningstype_titel="titel",
        )
        self.session.add(tilknytning)
        bruger = Bruger(
            fornavn="fornavn2",
            efternavn="efternavn2",
            uuid="b2",
            bvn="b2bvn",
            cpr="cpr2",
        )
        self.session.add(bruger)
        tilknytning = Tilknytning(
            uuid="t2",
            bvn="t2bvn",
            bruger_uuid="b2",
            enhed_uuid="E3",
            tilknytningstype_titel="titel2",
        )
        self.session.add(tilknytning)
        adresse = Adresse(
            uuid="A1",
            bruger_uuid="b1",
            adressetype_scope="scope",
            adressetype_titel="Email",
            værdi="test@email.dk",
            synlighed_titel="Hemmelig",
        )
        self.session.add(adresse)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_data(self):
        hoved_enhed = self.session.query(Enhed).all()
        data = Report_MED(self.session, "Hoved-MED").run()
        self.assertEqual(data[0], ["Navn", "Email", "Tilknytningstype", "Enhed"])

        self.assertEqual(data[1], ["fornavn efternavn", "", "titel", "Under-MED"])

        self.assertEqual(
            data[2], ["fornavn2 efternavn2", "", "titel2", "Under-under-MED"]
        )


if __name__ == "__main__":
    unittest.main()
