import tempfile
import unittest

import xlsxwriter
from openpyxl import load_workbook
from sqlalchemy import create_engine

from exporters.sql_export.sql_table_defs import (
    Adresse,
    Base,
    Bruger,
    Engagement,
    Enhed,
    Tilknytning,
)
from reports.query_actualstate import (
    get_engine,
    list_employees,
    list_MED_members,
    sessionmaker,
    set_of_org_units,
)
from reports.XLSXExporter import XLSXExporter


class Tests_db(unittest.TestCase):
    def setUp(self):
        self.engine = get_engine(dbpath=":memory:")
        self.session = sessionmaker(bind=self.engine, autoflush=False)()
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
            cpr="cpr1",
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
        engagement = Engagement(
            uuid="Eng1",
            bvn="Eng1bvn",
            engagementstype_titel="test1",
            primærtype_titel="?",
            bruger_uuid="b1",
            enhed_uuid="E3",
            stillingsbetegnelse_titel="tester1",
        )
        self.session.add(engagement)
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
        engagement = Engagement(
            uuid="Eng2",
            bvn="Eng2bvn",
            engagementstype_titel="test2",
            primærtype_titel="?",
            bruger_uuid="b2",
            enhed_uuid="E2",
            stillingsbetegnelse_titel="tester2",
        )
        self.session.add(engagement)
        adresse = Adresse(
            uuid="A1",
            bruger_uuid="b1",
            adressetype_scope="EMAIL",
            adressetype_bvn="Email",
            adressetype_titel="Email",
            værdi="test@email.dk",
            synlighed_titel="Hemmelig",
        )
        self.session.add(adresse)
        adresse = Adresse(
            uuid="A1",
            bruger_uuid="b1",
            adressetype_scope="EMAIL",
            adressetype_bvn="AD-Email",
            adressetype_titel="AD-Email",
            værdi="AD-email@email.dk",
            synlighed_titel="Offentlig",
        )
        self.session.add(adresse)
        adresse = Adresse(
            uuid="A2",
            bruger_uuid="b1",
            adressetype_scope="PHONE",
            adressetype_bvn="AD-Telefonnummer",
            adressetype_titel="AD-Telefonnummer",
            værdi="12345678",
            synlighed_titel="",
        )
        self.session.add(adresse)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_MED_data(self):
        hoved_enhed = self.session.query(Enhed).all()
        data = list_MED_members(self.session, "Hoved-MED")
        self.assertEqual(data[0], ("Navn", "Email", "Tilknytningstype", "Enhed"))

        self.assertEqual(data[1], ("fornavn efternavn", None, "titel", "Under-MED"))

        self.assertEqual(
            data[2], ("fornavn2 efternavn2", None, "titel2", "Under-under-MED")
        )

    def test_set_of_org_units(self):
        alle_enheder = set_of_org_units(self.session, "Hoved-MED")
        self.assertEqual(alle_enheder, set(["E2", "E3"]))

    def test_EMP_data(self):
        hoved_enhed = self.session.query(Enhed).all()
        data = list_employees(self.session, "Hoved-MED")
        self.assertEqual(
            data[0],
            ("Navn", "cpr", "AD-Email", "AD-Telefonnummer", "Enhed", "Stilling"),
        )

        self.assertEqual(
            data[1],
            (
                "fornavn efternavn",
                "cpr1",
                "AD-email@email.dk",
                "12345678",
                "Under-under-MED",
                "tester1",
            ),
        )

        self.assertEqual(
            data[2],
            ("fornavn2 efternavn2", "cpr2", None, None, "Under-MED", "tester2"),
        )


if __name__ == "__main__":
    unittest.main()
