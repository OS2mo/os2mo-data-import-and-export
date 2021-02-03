#
# Copyright (c) 2020, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Dette program er en snurrebasse - et dataopretningsprogram
#
# Dette program er i en situation brugt til at rette data op, idet 
# org_unit_levels fejlagtigt var indsat som org_unit_types
# Det løber alle afdelinger i et bestemt deltræ igennem og 
# retter både org_unit_type til 'Enhed' og org_unit_level til det, 
# der tidligere stod i org_unit_type.
#
# Forvent ikke at du kan genbruge andet end strukturen (gennemløb/opretning)

import logging
import pathlib

from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_, and_
from os2mo_helpers.mora_helpers import MoraHelper
from pprint import pprint
from integrations.opus import payloads


from exporters.sql_export.lc_for_jobs_db import get_engine  # noqa
from exporters.sql_export.sql_table_defs import (Adresse, ItForbindelse)
import json

settings = json.loads((pathlib.Path(".") / "settings/settings.json").read_text())

logger = logging.getLogger("snurrebasse")
for name in logging.root.manager.loggerDict:
    if name in ('snurrebasse', ):
        logging.getLogger(name).setLevel(logging.DEBUG)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=logging.DEBUG
)


def get_session(engine):
    return sessionmaker(bind=engine, autoflush=False)()


type_to_level = {
}


class SnurreBasse:

    def __init__(self, session):
        self.session = session
        self.helper = MoraHelper(hostname=settings['mora.base'], use_cache=False)


    def terminate(self, typ, uuid):
        response = self.helper._mo_post('details/terminate', {"type":typ,"uuid":uuid,"validity":{"to":"2020-10-01"}})
        if response.status_code == 400:
            assert(response.text.find('raise to a new registration') > 0)
        else:
            response.raise_for_status()


    def run_it(self):
        for i in session.query(ItForbindelse.uuid).filter(and_(
            ItForbindelse.bruger_uuid != None
        )).all():
            try:
                print(i)
                self.terminate('it', i[0])
            except:
                pass

    def run_adresse(self):
        for i in session.query(Adresse.uuid).filter(and_(
            Adresse.adressetype_scope == "E-mail",
            Adresse.bruger_uuid != None
        )).all():
            try:
                print(i)
                self.terminate('address', i[0])
            except:
                pass

if __name__ == '__main__':

    session = get_session(get_engine())
    SnurreBasse(session).run_adresse()
    SnurreBasse(session).run_it()
