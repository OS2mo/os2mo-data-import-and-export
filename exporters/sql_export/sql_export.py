import json
import atexit
import logging
import pathlib
import argparse
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
    def __init__(self, force_sqlite=False):
        logger.info('Start SQL export')
        atexit.register(self.at_exit)
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        db_type = self.settings.get('exporters.actual_state.type')
        if force_sqlite:
            db_type = 'SQLite'

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

    def perform_export(self, historic=False, resolve_dar=True, dry_run=False):
        if historic:
            self.lc = LoraCache(resolve_dar=resolve_dar, full_history=True)
            self.lc.populate_cache(dry_run=dry_run)
        else:
            self.lc = LoraCache(resolve_dar=resolve_dar)
            self.lc.populate_cache(dry_run=dry_run)
            self.lc.calculate_derived_unit_data()
            self.lc.calculate_primary_engagements()

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

        for unit, unit_validities in self.lc.units.items():
            for unit_info in unit_validities:
                location = unit_info.get('location')
                manager_uuid = unit_info.get('manager_uuid')
                acting_manager_uuid = unit_info.get('acting_manager_uuid')

                unit_type = unit_info['unit_type']
                enhedsniveau_titel = ''
                if unit_info['level']:
                    enhedsniveau_titel = self.lc.classes[unit_info['level']]['title']
                sql_unit = Enhed(
                    uuid=unit,
                    navn=unit_info['name'],
                    forældreenhed_uuid=unit_info['parent'],
                    enhedstype_uuid=unit_type,
                    enhedsniveau_uuid=unit_info['level'],
                    organisatorisk_sti=location,
                    leder_uuid=manager_uuid,
                    fungerende_leder_uuid=acting_manager_uuid,
                    enhedstype_titel=self.lc.classes[unit_type]['title'],
                    enhedsniveau_titel=enhedsniveau_titel,
                    startdato=unit_info['from_date'],
                    slutdato=unit_info['to_date']
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
        for engagement, engagement_validity in self.lc.engagements.items():
            for engagement_info in engagement_validity:
                if engagement_info['primary_type'] is not None:
                    primærtype_titel = self.lc.classes[
                        engagement_info['primary_type']]['title']
                else:
                    primærtype_titel = ''

                sql_engagement = Engagement(
                    uuid=engagement,
                    enhed_uuid=engagement_info['unit'],
                    bruger_uuid=engagement_info['user'],
                    bvn=engagement_info['user_key'],
                    primærtype_uuid=engagement_info['primary_type'],
                    stillingsbetegnelse_uuid=engagement_info['job_function'],
                    engagementstype_uuid=engagement_info['engagement_type'],
                    primær_boolean=engagement_info.get('primary_boolean'),
                    arbejdstidsfraktion=engagement_info['fraction'],
                    engagementstype_titel=self.lc.classes[
                        engagement_info['engagement_type']]['title'],
                    stillingsbetegnelse_titel=self.lc.classes[
                        engagement_info['job_function']]['title'],
                    primærtype_titel=primærtype_titel,
                    startdato=engagement_info['from_date'],
                    slutdato=engagement_info['to_date'],
                    **engagement_info['extensions']
                )
                self.session.add(sql_engagement)
        self.session.commit()

        if output:
            for result in self.engine.execute('select * from engagementer limit 5'):
                print(result.items())

    def _add_addresses(self, output=False):
        logger.info('Add addresses')
        print('Add addresses')
        for address, address_validities in self.lc.addresses.items():
            for address_info in address_validities:
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
                        address_info['adresse_type']]['title'],
                    startdato=address_info['from_date'],
                    slutdato=address_info['to_date']
                )
                self.session.add(sql_address)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from adresser limit 10'):
                print(result.items())

    def _add_associactions_leaves_and_roles(self, output=False):
        logger.info('Add associactions leaves and roles')
        print('Add associactions leaves and roles')
        for association, association_validity in self.lc.associations.items():
            for association_info in association_validity:
                sql_association = Tilknytning(
                    uuid=association,
                    bruger_uuid=association_info['user'],
                    enhed_uuid=association_info['unit'],
                    bvn=association_info['user_key'],
                    tilknytningstype_uuid=association_info['association_type'],
                    tilknytningstype_titel=self.lc.classes[
                        association_info['association_type']]['title'],
                    startdato=association_info['from_date'],
                    slutdato=association_info['to_date']
                )
                self.session.add(sql_association)

        for role, role_validity in self.lc.roles.items():
            for role_info in role_validity:
                sql_role = Rolle(
                    uuid=role,
                    bruger_uuid=role_info['user'],
                    enhed_uuid=role_info['unit'],
                    rolletype_uuid=role_info['role_type'],
                    rolletype_titel=self.lc.classes[role_info['role_type']]['title'],
                    startdato=role_info['from_date'],
                    slutdato=role_info['to_date']
                )
                self.session.add(sql_role)

        for leave, leave_validity in self.lc.leaves.items():
            for leave_info in leave_validity:
                leave_type = leave_info['leave_type']
                sql_leave = Orlov(
                    uuid=leave,
                    bvn=leave_info['user_key'],
                    bruger_uuid=leave_info['user'],
                    orlovstype_uuid=leave_type,
                    orlovstype_titel=self.lc.classes[leave_type]['title'],
                    startdato=leave_info['from_date'],
                    slutdato=leave_info['to_date']
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

        for it_connection, it_connection_validity in self.lc.it_connections.items():
            for it_connection_info in it_connection_validity:
                sql_it_connection = ItForbindelse(
                    uuid=it_connection,
                    it_system_uuid=it_connection_info['itsystem'],
                    bruger_uuid=it_connection_info['user'],
                    enhed_uuid=it_connection_info['unit'],
                    brugernavn=it_connection_info['username'],
                    startdato=it_connection_info['from_date'],
                    slutdato=it_connection_info['to_date']
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
        for manager, manager_validity in self.lc.managers.items():
            for manager_info in manager_validity:
                sql_manager = Leder(
                    uuid=manager,
                    bruger_uuid=manager_info['user'],
                    enhed_uuid=manager_info['unit'],
                    niveautype_uuid=manager_info['manager_level'],
                    ledertype_uuid=manager_info['manager_type'],
                    niveautype_titel=self.lc.classes[
                        manager_info['manager_level']]['title'],
                    ledertype_titel=self.lc.classes[
                        manager_info['manager_type']]['title'],
                    startdato=manager_info['from_date'],
                    slutdato=manager_info['to_date']
                )
                self.session.add(sql_manager)

                for responsibility in manager_info['manager_responsibility']:
                    sql_responsibility = LederAnsvar(
                        leder_uuid=manager,
                        lederansvar_uuid=responsibility,
                        lederansvar_titel=self.lc.classes[responsibility]['title'],
                        startdato=manager_info['from_date'],
                        slutdato=manager_info['to_date']
                    )
                    self.session.add(sql_responsibility)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from ledere limit 10'):
                print(result.items())
            for result in self.engine.execute('select * from leder_ansvar limit 10'):
                print(result.items())


def cli():
    """
    Command line interface.
    """
    parser = argparse.ArgumentParser(description='SQL export')
    parser.add_argument('--resolve-dar', action='store_true')
    parser.add_argument('--historic', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--force-sqlite', action='store_true')

    args = vars(parser.parse_args())

    sql_export = SqlExport(
        force_sqlite=args.get('force_sqlite')
    )

    sql_export.perform_export(
        resolve_dar=args.get('resolve_dar'),
        historic=args.get('historic'),
        dry_run=args.get('dry_run')
    )


if __name__ == '__main__':
    cli()
