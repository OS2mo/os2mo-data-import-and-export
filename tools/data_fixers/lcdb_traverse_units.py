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
from os2mo_helpers.mora_helpers import MoraHelper
from pprint import pprint
from integrations.opus import payloads


from exporters.sql_export.lc_for_jobs_db import get_engine  # noqa
from exporters.sql_export.sql_table_defs import (Klasse, Enhed, ItForbindelse)
from functools import partial
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

    def _find_classes(self, facet):
        class_types = self.helper.read_classes_in_facet(facet)
        types_dict = {}
        facet = class_types[1]
        for class_type in class_types[0]:
            types_dict[class_type['user_key']] = class_type['uuid']
        return types_dict, facet

    def __init__(self, session):
        self.session = session
        self.top_per_unit = {}
        self.helper = MoraHelper(hostname=settings['mora.base'], use_cache=False)
        self.unit_types, self.unit_type_facet = self._find_classes('org_unit_type')
        self.unit_levels, self.unit_level_facet = self._find_classes('org_unit_level')

    def get_top_unit(self, lc_enhed):
        """
        return the top unit for a unit
        """
        top_unit = self.top_per_unit.get(lc_enhed.uuid)
        if top_unit:
            return top_unit
        branch = [lc_enhed.uuid]

        # walk as far up as necessary
        while lc_enhed.forældreenhed_uuid is not None:
            uuid = lc_enhed.forældreenhed_uuid
            top_unit = self.top_per_unit.get(uuid)
            if top_unit:
                break
            branch.append(uuid)
            lc_enhed = self.session.query(Enhed).filter(Enhed.uuid == uuid).one()
            top_unit = uuid  # last one effective

        # register top unit for all encountered
        for buuid in branch:
            self.top_per_unit[buuid] = top_unit
        return top_unit

    def level_from_type(self, outype_bvn):
        return self.unit_levels[outype_bvn]

    def update_unit(self, lc_enhed):
        mo_unit = self.helper.read_ou(lc_enhed.uuid)
        if mo_unit["org_unit_level"] is not None and mo_unit["org_unit_level"]["user_key"] in self.unit_levels:
            logger.debug("already done: %s", lc_enhed.uuid)
            return

        payload = {
            'type': 'org_unit',
            'data': {
                'uuid': lc_enhed.uuid,
                'org_unit_level': {
                    'uuid': self.level_from_type(mo_unit["org_unit_type"]["user_key"]),
                },
                'org_unit_type': {
                    'uuid': settings["snurrebasser.lcdb_traverse_units.org_unit_type_uuid"],
                },
                'validity': {
                    'from': mo_unit["validity"]["from"],
                    'to': None
                }

            }
        }
        logger.info('Edit unit: {}'.format(payload))
        response = self.helper._mo_post('details/edit', payload)
        if response.status_code == 400 and response.text.find('raise to a new registration') > 0:
            pass
        else:
            response.raise_for_status()


    def run(self):
        def is_relevant(session, lc_enhed):
            top_unit = self.get_top_unit(lc_enhed)
            return top_unit == settings["snurrebasser.lcdb_traverse_units.top_unit_uuid"]

        relevant = partial(is_relevant, session)
        alle_enheder = session.query(Enhed)
        for i in filter(relevant, alle_enheder):
            self.update_unit(i)

if __name__ == '__main__':

    session = get_session(get_engine())
    SnurreBasse(session).run()

