import json
import atexit
import logging
import pathlib
import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from exporters.sql_export.lora_cache import LoraCache
from exporters.sql_export.sql_table_defs import (
    Base,
    Facet, Klasse,
    Bruger, Enhed,
    ItSystem, LederAnsvar,
    Adresse, Engagement, Rolle, Tilknytning, Orlov, ItForbindelse, Leder
)

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'sql_export.log'

logger = logging.getLogger('SqlExport')

for name in logging.root.manager.loggerDict:
    if name in ('LoraCache', 'SqlExport'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.ERROR)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)


class SqlExport(object):
    def __init__(self):
        logger.info('Start SQL export')
        atexit.register(self.at_exit)
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.lc = LoraCache()
        self.lc.populate_cache(dry_run=True)

        db_type = self.settings['exporters.actual_state.type']
        db_name = self.settings['exporters.actual_state.db_name']
        user = self.settings.get('exporters.actual_state.user')
        db_host = self.settings.get('exporters.actual_state.host')
        pw_raw = self.settings.get('exporters.actual_state.password', '')
        pw = urllib.parse.quote_plus(pw_raw)
        if db_type == 'SQLite':
            db_string = 'sqlite:///{}.db'.format(db_name)
        elif db_type == 'MS-SQL':
            db_string = 'mssql+pymssql://{}:{}@{}/{}'.format(
                user, pw, db_host, db_name)
        else:
            raise Exception('Unknown DB type')

        self.engine = create_engine(db_string)

        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine, autoflush=False)
        self.session = Session()

        self._add_classification()
        self._add_users_and_units()
        self._add_addresses()
        self._add_engagements()
        self._add_associactions_leaves_and_roles()
        self._add_managers()
        self._add_it_systems()

    def at_exit(self):
        logger.info('*SQL export ended*')

    def _add_classification(self, output=False):
        logger.info('Add classification')
        print('Add classification')
        logger.info('Add classification')
        for facet, facet_info in self.lc.facets.items():
            sql_facet = Facet(
                uuid=facet,
                bvn=facet_info['user_key'],
            )
            self.session.add(sql_facet)
        self.session.commit()

        for klasse, klasse_info in self.lc.classes.items():
            sql_class = Klasse(
                uuid=klasse,
                bvn=klasse_info['user_key'],
                titel=klasse_info['title'],
                facet_uuid=klasse_info['facet'],
                facet_bvn=self.lc.facets[klasse_info['facet']]['user_key']
            )
            self.session.add(sql_class)
        self.session.commit()

        if output:
            for result in self.engine.execute('select * from facetter limit 4'):
                print(result.items())
            for result in self.engine.execute('select * from klasser limit 4'):
                print(result.items())

    def _add_users_and_units(self, output=False):
        logger.info('Add users and units')
        print('Add users and units')
        for user, user_info in self.lc.users.items():
            sql_user = Bruger(
                uuid=user,
                fornavn=user_info['fornavn'],
                efternavn=user_info['efternavn'],
                cpr=user_info['cpr']
            )
            self.session.add(sql_user)

        responsibility_class = self.settings[
            'exporters.actual_state.manager_responsibility_class']

        for unit, unit_info in self.lc.units.items():
            manager_uuid = None
            acting_manager_uuid = None
            # Find a direct manager, if possible
            for manager, manager_info in self.lc.managers.items():
                if manager_info['unit'] == unit:
                    for resp in manager_info['manager_responsibility']:
                        if resp == responsibility_class:
                            manager_uuid = manager
                            acting_manager_uuid = manager

            location = ''
            current_unit = unit_info
            while current_unit:
                location = current_unit['name'] + "\\" + location
                current_parent = current_unit.get('parent')
                if current_parent is not None:
                    current_unit = self.lc.units[current_parent]
                else:
                    current_unit = None

                # Find the acting manager.
                if acting_manager_uuid is None:
                    for manager, manager_info in self.lc.managers.items():
                        if manager_info['unit'] == current_parent:
                            for resp in manager_info['manager_responsibility']:
                                if resp == responsibility_class:
                                    acting_manager_uuid = manager
            location = location[:-1]

            enhedsniveau_titel = ''
            if unit_info['level']:
                enhedsniveau_titel = self.lc.classes[unit_info['level']]['title']
            sql_unit = Enhed(
                uuid=unit,
                navn=unit_info['name'],
                forældreenhed_uuid=unit_info['parent'],
                enhedstype_uuid=unit_info['unit_type'],
                enhedsniveau_uuid=unit_info['level'],
                organisatorisk_sti=location,
                leder_uuid=manager_uuid,
                fungerende_leder_uuid=acting_manager_uuid,
                enhedstype_titel=self.lc.classes[unit_info['unit_type']]['title'],
                enhedsniveau_titel=enhedsniveau_titel
            )
            self.session.add(sql_unit)
        self.session.commit()

        if output:
            for result in self.engine.execute('select * from brugere limit 5'):
                print(result)
            for result in self.engine.execute('select * from enheder limit 5'):
                print(result)

    def _add_engagements(self, output=False):
        logger.info('Add engagements')
        print('Add engagements')
        user_primary = {}
        for uuid, eng in self.lc.engagements.items():
            primary_type = self.lc.classes[eng['primary_type']]
            primary_scope = int(primary_type['scope'])
            if eng['user'] in user_primary:
                if user_primary[eng['user']][0] < primary_scope:
                    user_primary[eng['user']] = [primary_scope, uuid, None]
            else:
                user_primary[eng['user']] = [primary_scope, uuid, None]

        for engagement, engagement_info in self.lc.engagements.items():
            primary = user_primary[engagement_info['user']][1] == engagement

            sql_engagement = Engagement(
                uuid=engagement,
                enhed_uuid=engagement_info['unit'],
                bruger_uuid=engagement_info['user'],
                bvn=engagement_info['user_key'],
                primærtype_uuid=engagement_info['primary_type'],
                stillingsbetegnelse_uuid=engagement_info['job_function'],
                engagementstype_uuid=engagement_info['engagement_type'],
                primær_boolean=primary,
                arbejdstidsfraktion=engagement_info['fraction'],
                engagementstype_titel=self.lc.classes[
                    engagement_info['engagement_type']]['title'],
                stillingsbetegnelse_titel=self.lc.classes[
                    engagement_info['job_function']]['title'],
                primærtype_titel=self.lc.classes[
                    engagement_info['primary_type']]['title'],
                **engagement_info['extentions']
            )
            self.session.add(sql_engagement)
        self.session.commit()

        if output:
            for result in self.engine.execute('select * from engagementer limit 5'):
                print(result.items())

    def _add_addresses(self, output=False):
        logger.info('Add addresses')
        print('Add addresses')
        for address, address_info in self.lc.addresses.items():
            visibility_text = None
            if address_info['visibility'] is not None:
                visibility_text = self.lc.classes[
                    address_info['visibility']]['title']

            sql_address = Adresse(
                uuid=address,
                enhed_uuid=address_info['unit'],
                bruger_uuid=address_info['user'],
                værdi=address_info['value'],
                dar_uuid=address_info['dar_uuid'],
                adressetype_uuid=address_info['adresse_type'],
                adressetype_scope=address_info['scope'],
                synlighed_uuid=address_info['visibility'],
                synlighed_titel=visibility_text,
                adressetype_titel=self.lc.classes[
                    address_info['adresse_type']]['title']
            )
            self.session.add(sql_address)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from adresser limit 10'):
                print(result.items())

    def _add_associactions_leaves_and_roles(self, output=False):
        logger.info('Add associactions leaves and roles')
        print('Add associactions leaves and roles')
        for association, association_info in self.lc.associations.items():
            sql_association = Tilknytning(
                uuid=association,
                bruger_uuid=association_info['user'],
                enhed_uuid=association_info['unit'],
                bvn=association_info['user_key'],
                tilknytningstype_uuid=association_info['association_type'],
                tilknytningstype_titel=self.lc.classes[
                    association_info['association_type']]['title']
            )
            self.session.add(sql_association)

        for role, role_info in self.lc.roles.items():
            sql_role = Rolle(
                uuid=role,
                bruger_uuid=role_info['user'],
                enhed_uuid=role_info['unit'],
                rolletype_uuid=role_info['role_type'],
                rolletype_titel=self.lc.classes[role_info['role_type']]['title']
                # start_date, # TODO
                # end_date # TODO
            )
            self.session.add(sql_role)

        for leave, leave_info in self.lc.leaves.items():
            sql_leave = Orlov(
                uuid=leave,
                bvn=leave_info['user_key'],
                bruger_uuid=leave_info['user'],
                orlovstype_uuid=leave_info['leave_type'],
                orlovstype_titel=self.lc.classes[leave_info['leave_type']]['title'],
                # start_date # TODO
                # end_date # TODO
            )
            self.session.add(sql_leave)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from tilknytninger limit 4'):
                print(result.items())
            for result in self.engine.execute('select * from orlover limit 4'):
                print(result.items())
            for result in self.engine.execute('select * from roller limit 4'):
                print(result.items())

    def _add_it_systems(self, output=False):
        logger.info('Add IT systems')
        print('Add IT systems')
        for itsystem, itsystem_info in self.lc.itsystems.items():
            sql_itsystem = ItSystem(
                uuid=itsystem,
                navn=itsystem_info['name']
            )
            self.session.add(sql_itsystem)

        for it_connection, it_connection_info in self.lc.it_connections.items():
            sql_it_connection = ItForbindelse(
                uuid=it_connection,
                it_system_uuid=it_connection_info['itsystem'],
                bruger_uuid=it_connection_info['user'],
                enhed_uuid=it_connection_info['unit'],
                brugernavn=it_connection_info['username']
            )
            self.session.add(sql_it_connection)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from it_systemer limit 2'):
                print(result.items())

            for result in self.engine.execute(
                    'select * from it_forbindelser limit 2'):
                print(result.items())

    def _add_managers(self, output=False):
        logger.info('Add managers')
        print('Add managers')
        for manager, manager_info in self.lc.managers.items():
            sql_manager = Leder(
                uuid=manager,
                bruger_uuid=manager_info['user'],
                enhed_uuid=manager_info['unit'],
                niveautype_uuid=manager_info['manager_level'],
                ledertype_uuid=manager_info['manager_type'],
                niveautype_titel=self.lc.classes[
                    manager_info['manager_level']]['title'],
                ledertype_titel=self.lc.classes[
                    manager_info['manager_type']]['title']
            )
            self.session.add(sql_manager)

            for responsibility in manager_info['manager_responsibility']:
                sql_responsibility = LederAnsvar(
                    leder_uuid=manager,
                    lederansvar_uuid=responsibility,
                    lederansvar_titel=self.lc.classes[responsibility]['title']
                )
                self.session.add(sql_responsibility)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from ledere limit 10'):
                print(result.items())
            for result in self.engine.execute('select * from leder_ansvar limit 10'):
                print(result.items())


if __name__ == '__main__':
    sql_export = SqlExport()
