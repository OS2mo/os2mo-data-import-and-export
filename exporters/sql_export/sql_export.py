import json
import atexit
import logging
import pathlib
import argparse
import urllib.parse
import datetime

from sqlalchemy import create_engine, Index
from sqlalchemy.orm import sessionmaker

from exporters.sql_export.lora_cache import LoraCache
from exporters.sql_export.sql_table_defs import (
    Base,
    Facet, Klasse,
    Bruger, Enhed,
    ItSystem, LederAnsvar, KLE,
    Adresse, Engagement, Rolle, Tilknytning, Orlov, ItForbindelse, Leder,
    Kvittering, Enhedssammenkobling, DARAdresse
)

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'sql_export.log'

logger = logging.getLogger('SqlExport')


class SqlExport(object):
    def __init__(self, force_sqlite=False, historic=False, settings=None):
        logger.info('Start SQL export')
        atexit.register(self.at_exit)
        self.historic = historic
        self.settings = settings
        if self.settings is None:
            raise Exception('No settings provided')

        if self.historic:
            db_type = self.settings.get('exporters.actual_state_historic.type')
            db_name = self.settings.get('exporters.actual_state_historic.db_name')
        else:
            db_type = self.settings.get('exporters.actual_state.type')
            db_name = self.settings.get('exporters.actual_state.db_name')

        if force_sqlite:
            db_type = 'SQLite'

        if None in [db_type, db_name]:
            msg = 'Configuration error, missing db name or type'
            logger.error(msg)
            raise Exception(msg)

        user = self.settings.get('exporters.actual_state.user')
        db_host = self.settings.get('exporters.actual_state.host')
        pw_raw = self.settings.get('exporters.actual_state.password', '')
        pw = urllib.parse.quote_plus(pw_raw)
        engine_settings = {"pool_pre_ping": True}
        if db_type == 'SQLite':
            db_string = 'sqlite:///{}.db'.format(db_name)
        elif db_type == 'MS-SQL':
            db_string = 'mssql+pymssql://{}:{}@{}/{}'.format(
                user, pw, db_host, db_name)
        elif db_type == 'MS-SQL-ODBC':
            quoted = urllib.parse.quote_plus((
                'DRIVER=libtdsodbc.so;Server={};Database={};UID={};' +
                'PWD={};TDS_Version=8.0;Port=1433;').format(
                    db_host, db_name, user, pw_raw)
                )
            db_string = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
        elif db_type == "Mysql":
            engine_settings.update({"pool_recycle": 3600})
            db_string = 'mysql+mysqldb://{}:{}@{}/{}'.format(
                user, pw, db_host, db_name)

        else:
            raise Exception('Unknown DB type')

        self.engine = create_engine(db_string, **engine_settings)

    def perform_export(self, resolve_dar=True, use_pickle=False):
        def timestamp():
            return datetime.datetime.now()

        trunc_tables=dict(Base.metadata.tables)
        trunc_tables.pop("kvittering")

        Base.metadata.drop_all(self.engine, tables=trunc_tables.values())
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine, autoflush=False)
        self.session = Session()

        query_time = timestamp()
        kvittering = self._add_receipt(query_time)
        if self.historic:
            self.lc = LoraCache(resolve_dar=resolve_dar, full_history=True)
            self.lc.populate_cache(dry_run=use_pickle)
        else:
            self.lc = LoraCache(resolve_dar=resolve_dar)
            self.lc.populate_cache(dry_run=use_pickle)
            self.lc.calculate_derived_unit_data()
            self.lc.calculate_primary_engagements()

        start_delivery_time = timestamp()
        self._update_receipt(kvittering, start_delivery_time)

        self._add_classification()
        self._add_users_and_units()
        self._add_addresses()
        self._add_dar_addresses()
        self._add_engagements()
        self._add_associactions_leaves_and_roles()
        self._add_managers()
        self._add_it_systems()
        self._add_kles()
        self._add_related()

        end_delivery_time = timestamp()
        self._update_receipt(kvittering, start_delivery_time, end_delivery_time)

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
        for user, user_effects in self.lc.users.items():
            for user_info in user_effects:
                sql_user = Bruger(
                    uuid=user,
                    bvn=user_info['user_key'],
                    fornavn=user_info['fornavn'],
                    efternavn=user_info['efternavn'],
                    kaldenavn_fornavn=user_info['kaldenavn_fornavn'],
                    kaldenavn_efternavn=user_info['kaldenavn_efternavn'],
                    cpr=user_info['cpr'],
                    startdato=user_info['from_date'],
                    slutdato=user_info['to_date'],
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

        # create supplementary index for quick toplevel lookup
        # when rewriting whole table this is quicker than maintaining
        # the index for every row inserted
        organisatorisk_sti_index = Index(
            "organisatorisk_sti_index",
            Enhed.organisatorisk_sti
        )
        organisatorisk_sti_index.create(bind=self.engine)

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

                engagement_type_uuid = engagement_info['engagement_type']
                job_function_uuid = engagement_info['job_function']

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
                    engagementstype_titel=self.lc.classes.get(
                        engagement_type_uuid,
                        {"title": engagement_type_uuid}
                    )['title'],
                    stillingsbetegnelse_titel=self.lc.classes.get(
                        job_function_uuid,
                        {"title": job_function_uuid}
                    )['title'],
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
                visibility_scope = None
                if address_info['visibility'] is not None:
                    visibility_scope = self.lc.classes[
                        address_info['visibility']]['scope']

                sql_address = Adresse(
                    uuid=address,
                    enhed_uuid=address_info['unit'],
                    bruger_uuid=address_info['user'],
                    værdi=address_info['value'],
                    dar_uuid=address_info['dar_uuid'],
                    adressetype_uuid=address_info['adresse_type'],
                    adressetype_bvn=self.lc.classes[
                        address_info['adresse_type']
                    ]['user_key'],
                    adressetype_scope=address_info['scope'],
                    adressetype_titel=self.lc.classes[
                        address_info['adresse_type']
                    ]['title'],
                    synlighed_uuid=address_info['visibility'],
                    synlighed_scope=visibility_scope,
                    synlighed_titel=visibility_text,
                    startdato=address_info['from_date'],
                    slutdato=address_info['to_date']
                )
                self.session.add(sql_address)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from adresser limit 10'):
                print(result.items())

    def _add_dar_addresses(self, output=False):
        logger.info('Add DAR addresses')
        print('Add DAR addresses')
        for address, address_info in self.lc.dar_cache.items():
            sql_address = DARAdresse(
                uuid=address,
                **{key: value for key, value in address_info.items()
                   if key in DARAdresse.__table__.columns.keys() and key != "id"}
            )
            self.session.add(sql_address)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from dar_adresser limit 10'):
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

    def _add_kles(self, output=False):
        logger.info('Add KLES')
        print('Add KLES')
        for kle, kle_validity in self.lc.kles.items():
            for kle_info in kle_validity:
                sql_kle = KLE(
                    uuid=kle,
                    enhed_uuid=kle_info['unit'],
                    kle_aspekt_uuid=kle_info['kle_aspect'],
                    kle_aspekt_titel=self.lc.classes[kle_info['kle_aspect']]['title'],
                    kle_nummer_uuid=kle_info['kle_number'],
                    kle_nummer_titel=self.lc.classes[kle_info['kle_number']]['title'],
                    startdato=kle_info['from_date'],
                    slutdato=kle_info['to_date']
                )
                self.session.add(sql_kle)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from kle limit 10'):
                print(result.items())

    def _add_receipt(self, query_time, start_time=None, end_time=None, output=False):
        logger.info('Add Receipt')
        print('Add Receipt')
        sql_kvittering = Kvittering(
            query_tid=query_time,
            start_levering_tid=start_time,
            slut_levering_tid=end_time,
        )
        self.session.add(sql_kvittering)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from kvittering limit 10'):
                print(result.items())
        return sql_kvittering

    def _update_receipt(self, sql_kvittering, start_time=None, end_time=None, output=False):
        logger.info('Update Receipt')
        print('Update Receipt')
        sql_kvittering.start_levering_tid=start_time
        sql_kvittering.slut_levering_tid=end_time
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from kvittering limit 10'):
                print(result.items())

    def _add_related(self, output=False):
        logger.info('Add Enhedssammenkobling')
        print('Add Enhedssammenkobling')
        for related, related_validity in self.lc.related.items():
            for related_info in related_validity:
                sql_related = Enhedssammenkobling(
                    uuid=related,
                    enhed1_uuid=related_info['unit1_uuid'],
                    enhed2_uuid=related_info['unit2_uuid'],
                    startdato=related_info['from_date'],
                    slutdato=related_info['to_date']
                )
                self.session.add(sql_related)
        self.session.commit()
        if output:
            for result in self.engine.execute('select * from enhedssammenkobling limit 10'):
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
    parser.add_argument('--use-pickle', action='store_true')
    parser.add_argument('--force-sqlite', action='store_true')

    args = vars(parser.parse_args())

    cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
    if not cfg_file.is_file():
        raise Exception('No setting file')
    settings = json.loads(cfg_file.read_text())

    sql_export = SqlExport(
        force_sqlite=args.get('force_sqlite'),
        historic=args.get('historic'),
        settings=settings,
    )

    sql_export.perform_export(
        resolve_dar=args.get('resolve_dar'),
        use_pickle=args.get('use_pickle')
    )


if __name__ == '__main__':

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

    cli()
