import unittest

from helpers import dummy_settings
from sqlalchemy.orm import sessionmaker

from constants import AD_it_system
from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import Adresse
from exporters.sql_export.sql_table_defs import Base
from exporters.sql_export.sql_table_defs import Bruger
from exporters.sql_export.sql_table_defs import Engagement
from exporters.sql_export.sql_table_defs import ItForbindelse
from exporters.sql_export.sql_table_defs import ItSystem
from exporters.sql_export.sql_table_defs import Tilknytning
from integrations.os2sync.lcdb_os2mo import get_sts_user
from integrations.os2sync.lcdb_os2mo import try_get_ad_user_key


class Tests_lc_db(unittest.TestCase):
    def setUp(self):
        """
        setup db and populate with quite minimal data
        """
        self.engine = get_engine(dbpath=":memory:")
        self.session = sessionmaker(bind=self.engine, autoflush=False)()
        # Lav tables via tabledefs fra LoraCache og fyld dataen ind
        Base.metadata.create_all(self.engine)
        bruger = Bruger(
            fornavn="fornavn",
            efternavn="efternavn",
            uuid="b1",
            bvn="b1bvn",
            cpr="cpr1",
        )
        self.session.add(bruger)
        it = ItSystem(navn=AD_it_system, uuid="ItSystem1")
        self.session.add(it)
        it = ItSystem(navn="it_navn2", uuid="ItSystem2")
        self.session.add(it)
        it = ItForbindelse(
            id=1,
            uuid="if1",
            it_system_uuid="ItSystem1",
            bruger_uuid="b1",
            enhed_uuid="e1",
            brugernavn="AD-logon",
            startdato="0",
            slutdato="1",
            primær_boolean=True,
        )
        self.session.add(it)
        it = ItForbindelse(
            id=2,
            uuid="if2",
            it_system_uuid="ItSystem2",
            bruger_uuid="b1",
            enhed_uuid="e1",
            brugernavn="if_bvn2",
            startdato="0",
            slutdato="1",
            primær_boolean=True,
        )
        self.session.add(it)
        self.session.commit()

    def setup_wide(self):
        """
        setup a bunch of additional stuff
        """
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
            adressetype_scope="E-mail",
            adressetype_bvn="Email",
            adressetype_titel="Email",
            værdi="test@email.dk",
            synlighed_titel="Hemmelig",
        )
        self.session.add(adresse)
        adresse = Adresse(
            uuid="A1",
            bruger_uuid="b1",
            adressetype_scope="E-mail",
            adressetype_bvn="AD-Email",
            adressetype_titel="AD-Email",
            værdi="AD-email@email.dk",
            synlighed_titel="Offentlig",
        )
        self.session.add(adresse)
        adresse = Adresse(
            uuid="A2",
            bruger_uuid="b1",
            adressetype_scope="Telefon",
            adressetype_bvn="AD-Telefonnummer",
            adressetype_titel="AD-Telefonnummer",
            værdi="12345678",
            synlighed_titel="",
        )

        self.session.add(adresse)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_lcdb_get_ad(self):
        expected = "AD-logon"
        self.assertEqual(expected, try_get_ad_user_key(session=self.session, uuid="b1"))

    def test_lcdb_get_sts_user_default(self):
        self.setup_wide()
        expected = {
            "Email": "test@email.dk",
            "Person": {"Cpr": "cpr1", "Name": "fornavn efternavn"},
            "PhoneNumber": "12345678",
            "UserId": "AD-logon",
            "Uuid": "b1",
            "Positions": [],
        }
        settings = dummy_settings
        settings.os2sync_xfer_cpr = True
        self.assertEqual(
            expected, get_sts_user(self.session, "b1", [], settings=settings)
        )
