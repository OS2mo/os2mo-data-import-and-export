import datetime
import logging
import typing
from typing import Tuple

import click
import ra_utils.ensure_single_run
from alembic.migration import MigrationContext
from alembic.operations import Operations
from ra_utils.ensure_single_run import ensure_single_run
from ra_utils.job_settings import JobSettings
from ra_utils.load_settings import load_settings
from ra_utils.tqdm_wrapper import tqdm
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from .lora_cache import LoraCache
from .sql_table_defs import Adresse
from .sql_table_defs import Base
from .sql_table_defs import Bruger
from .sql_table_defs import DARAdresse
from .sql_table_defs import Engagement
from .sql_table_defs import Enhed
from .sql_table_defs import Enhedssammenkobling
from .sql_table_defs import Facet
from .sql_table_defs import ItForbindelse
from .sql_table_defs import ItSystem
from .sql_table_defs import Klasse
from .sql_table_defs import KLE
from .sql_table_defs import Kvittering
from .sql_table_defs import Leder
from .sql_table_defs import LederAnsvar
from .sql_table_defs import Orlov
from .sql_table_defs import Rolle
from .sql_table_defs import Tilknytning
from .sql_url import DatabaseFunction
from .sql_url import generate_connection_url
from .sql_url import generate_engine_settings


class SqlExportSettings(JobSettings):
    class Config:
        settings_json_prefix = "exporters.actual_state"


logger = logging.getLogger(__name__)


class SqlExport:
    def __init__(self, force_sqlite=False, historic=False, settings=None):
        logger.info("Start SQL export")
        self.force_sqlite = force_sqlite
        self.historic = historic
        self.settings = settings
        self.engine = self._get_engine()
        self.export_cpr = self._get_export_cpr_setting()

    def _get_engine(self) -> Engine:
        database_function = DatabaseFunction.ACTUAL_STATE
        if self.historic:
            database_function = DatabaseFunction.ACTUAL_STATE_HISTORIC
        db_string = generate_connection_url(
            database_function, force_sqlite=self.force_sqlite, settings=self.settings
        )
        engine_settings = generate_engine_settings(
            database_function, force_sqlite=self.force_sqlite, settings=self.settings
        )
        return create_engine(db_string, **engine_settings)

    def _get_export_cpr_setting(self) -> bool:
        return self.settings.get("exporters.actual_state.export_cpr", True)

    def _get_lora_cache(self, resolve_dar, use_pickle) -> LoraCache:
        if self.historic:
            lc = LoraCache(
                resolve_dar=resolve_dar, full_history=True, settings=self.settings
            )
            lc.populate_cache(dry_run=use_pickle)
        else:
            lc = LoraCache(resolve_dar=resolve_dar, settings=self.settings)
            lc.populate_cache(dry_run=use_pickle)
            lc.calculate_derived_unit_data()
            lc.calculate_primary_engagements()
        return lc

    def _get_db_session(self) -> Session:
        Session = sessionmaker(bind=self.engine, autoflush=False)
        return Session()

    def _get_lora_class(self, uuid: str) -> Tuple[str, dict]:
        cls: dict = self.lc.classes.get(uuid) or {"title": uuid}
        return uuid, cls

    def perform_export(self, resolve_dar=True, use_pickle=None):
        def timestamp():
            return datetime.datetime.now()

        trunc_tables = dict(Base.metadata.tables)
        trunc_tables.pop("kvittering")

        logger.info("Dropping tables")
        Base.metadata.drop_all(self.engine, tables=trunc_tables.values())
        logger.info("Creating tables")
        Base.metadata.create_all(self.engine)

        self.session = self._get_db_session()

        query_time = timestamp()
        kvittering = self._add_receipt(query_time)
        self.lc = self._get_lora_cache(resolve_dar, use_pickle)

        start_delivery_time = timestamp()
        self._update_receipt(kvittering, start_delivery_time)

        tasks = [
            self._add_classification,
            self._add_users_and_units,
            self._add_addresses,
            self._add_dar_addresses,
            self._add_engagements,
            self._add_associations,
            self._add_managers,
            self._add_it_systems,
            self._add_kles,
            self._add_related,
        ]
        for task in tqdm(tasks, desc="SQLExport", unit="task"):
            task()

        end_delivery_time = timestamp()
        self._update_receipt(kvittering, start_delivery_time, end_delivery_time)

    def get_actual_tables(self):
        connection = self.engine.connect()
        inspector = Inspector.from_engine(connection)
        actual_tables = inspector.get_table_names()
        return set(actual_tables)

    def swap_tables(self):
        """Swap tables around to present the exported data.

        Swaps the current tables to old tables, then swaps write tables to current.
        Finally drops the old tables leaving just the current tables.
        """
        logger.info("Swapping tables")
        connection = self.engine.connect()
        ctx = MigrationContext.configure(connection)
        op = Operations(ctx)

        def gen_table_names(write_table):
            """Generate current and old table names from write tables."""
            # Current tables do not have the prefix 'w'
            current_table = write_table[1:]
            old_table = current_table + "_old"
            return write_table, current_table, old_table

        tables = dict(Base.metadata.tables)
        tables.pop("kvittering")
        tables = tables.keys()
        tables = list(map(gen_table_names, tables))

        # Drop any left-over old tables that may exist
        with ctx.begin_transaction():
            actual_tables = self.get_actual_tables()
            for _, _, old_table in tables:
                if old_table in actual_tables:
                    op.drop_table(old_table)

        # Rename current to old and write to current
        with ctx.begin_transaction():
            actual_tables = self.get_actual_tables()
            for write_table, current_table, old_table in tables:
                if current_table in actual_tables:
                    op.rename_table(current_table, old_table)
                # Rename write table to current table
                op.rename_table(write_table, current_table)

        # Drop any old tables that may exist
        with ctx.begin_transaction():
            actual_tables = self.get_actual_tables()
            for _, _, old_table in tables:
                if old_table in actual_tables:
                    op.drop_table(old_table)

    def _add_classification(self, output=False):
        logger.info("Add classification")
        for facet, facet_info in tqdm(
            self.lc.facets.items(), desc="Export facet", unit="facet"
        ):
            sql_facet = Facet(
                uuid=facet,
                bvn=facet_info["user_key"],
            )
            self.session.add(sql_facet)
        self.session.commit()

        for klasse, klasse_info in tqdm(
            self.lc.classes.items(), desc="Export class", unit="class"
        ):
            sql_class = Klasse(
                uuid=klasse,
                bvn=klasse_info["user_key"],
                titel=klasse_info["title"],
                facet_uuid=klasse_info["facet"],
                facet_bvn=self.lc.facets[klasse_info["facet"]]["user_key"],
            )
            self.session.add(sql_class)
        self.session.commit()

        if output:
            for result in self.engine.execute("select * from facetter limit 4"):
                print(result.items())
            for result in self.engine.execute("select * from klasser limit 4"):
                print(result.items())

    def _add_users_and_units(self, output=False):
        logger.info("Add users and units")
        for user, user_effects in tqdm(
            self.lc.users.items(), desc="Export user", unit="user"
        ):
            for user_info in user_effects:
                sql_user = Bruger(
                    uuid=user,
                    bvn=user_info["user_key"],
                    fornavn=user_info["fornavn"],
                    efternavn=user_info["efternavn"],
                    kaldenavn_fornavn=user_info["kaldenavn_fornavn"],
                    kaldenavn_efternavn=user_info["kaldenavn_efternavn"],
                    cpr=user_info["cpr"] if self.export_cpr else "",
                    startdato=user_info["from_date"],
                    slutdato=user_info["to_date"],
                )
                self.session.add(sql_user)
            self.session.commit()

        for unit, unit_validities in tqdm(
            self.lc.units.items(), desc="Export unit", unit="unit"
        ):
            for unit_info in unit_validities:
                location = unit_info.get("location")
                manager_uuid = unit_info.get("manager_uuid")
                acting_manager_uuid = unit_info.get("acting_manager_uuid")

                unit_type = unit_info["unit_type"]

                enhedsniveau_titel = ""
                if unit_info["level"]:
                    enhedsniveau_titel = self.lc.classes[unit_info["level"]]["title"]

                (
                    org_unit_hierarchy_uuid,
                    org_unit_hierarchy_class,
                ) = self._get_lora_class(unit_info["org_unit_hierarchy"])

                sql_unit = Enhed(
                    uuid=unit,
                    navn=unit_info["name"],
                    bvn=unit_info["user_key"],
                    forældreenhed_uuid=unit_info["parent"],
                    enhedstype_uuid=unit_type,
                    enhedsniveau_uuid=unit_info["level"],
                    organisatorisk_sti=location,
                    leder_uuid=manager_uuid,
                    fungerende_leder_uuid=acting_manager_uuid,
                    enhedstype_titel=self.lc.classes[unit_type]["title"],
                    enhedsniveau_titel=enhedsniveau_titel,
                    opmærkning_uuid=org_unit_hierarchy_uuid,
                    opmærkning_titel=org_unit_hierarchy_class["title"],
                    startdato=unit_info["from_date"],
                    slutdato=unit_info["to_date"],
                )
                self.session.add(sql_unit)
            self.session.commit()

        if output:
            for result in self.engine.execute("select * from brugere limit 5"):
                print(result)
            for result in self.engine.execute("select * from enheder limit 5"):
                print(result)

    def _add_engagements(self, output=False):
        logger.info("Add engagements")
        for engagement, engagement_validity in tqdm(
            self.lc.engagements.items(), desc="Export engagement", unit="engagement"
        ):
            for engagement_info in engagement_validity:
                if engagement_info["primary_type"] is not None:
                    primærtype_titel = self.lc.classes[engagement_info["primary_type"]][
                        "title"
                    ]
                else:
                    primærtype_titel = ""

                engagement_type_uuid = engagement_info["engagement_type"]
                job_function_uuid, job_function_class = self._get_lora_class(
                    engagement_info["job_function"]
                )

                sql_engagement = Engagement(
                    uuid=engagement,
                    enhed_uuid=engagement_info["unit"],
                    bruger_uuid=engagement_info["user"],
                    bvn=engagement_info["user_key"],
                    engagementstype_uuid=engagement_info["engagement_type"],
                    primær_boolean=engagement_info.get("primary_boolean"),
                    arbejdstidsfraktion=engagement_info["fraction"],
                    engagementstype_titel=self.lc.classes.get(
                        engagement_type_uuid, {"title": engagement_type_uuid}
                    )["title"],
                    primærtype_titel=primærtype_titel,
                    stillingsbetegnelse_uuid=job_function_uuid,
                    stillingsbetegnelse_titel=job_function_class["title"],
                    primærtype_uuid=engagement_info["primary_type"],
                    startdato=engagement_info["from_date"],
                    slutdato=engagement_info["to_date"],
                    **engagement_info["extensions"],
                )
                self.session.add(sql_engagement)
            self.session.commit()

        if output:
            for result in self.engine.execute("select * from engagementer limit 5"):
                print(result.items())

    def _add_addresses(self, output=False):
        logger.info("Add addresses")
        for address, address_validities in tqdm(
            self.lc.addresses.items(), desc="Export address", unit="address"
        ):
            for address_info in address_validities:
                visibility_text = None
                if address_info["visibility"] is not None:
                    visibility_text = self.lc.classes[address_info["visibility"]][
                        "title"
                    ]
                visibility_scope = None
                if address_info["visibility"] is not None:
                    visibility_scope = self.lc.classes[address_info["visibility"]][
                        "scope"
                    ]

                sql_address = Adresse(
                    uuid=address,
                    enhed_uuid=address_info["unit"],
                    bruger_uuid=address_info["user"],
                    værdi=address_info["value"],
                    dar_uuid=address_info["dar_uuid"],
                    adressetype_uuid=address_info["adresse_type"],
                    adressetype_bvn=self.lc.classes[address_info["adresse_type"]][
                        "user_key"
                    ],
                    adressetype_scope=address_info["scope"],
                    adressetype_titel=self.lc.classes[address_info["adresse_type"]][
                        "title"
                    ],
                    synlighed_uuid=address_info["visibility"],
                    synlighed_scope=visibility_scope,
                    synlighed_titel=visibility_text,
                    startdato=address_info["from_date"],
                    slutdato=address_info["to_date"],
                )
                self.session.add(sql_address)
            self.session.commit()
        if output:
            for result in self.engine.execute("select * from adresser limit 10"):
                print(result.items())

    def _add_dar_addresses(self, output=False):
        logger.info("Add DAR addresses")
        for address, address_info in tqdm(
            self.lc.dar_cache.items(), desc="Export DAR", unit="DAR"
        ):
            sql_address = DARAdresse(
                uuid=address,
                **{
                    key: value
                    for key, value in address_info.items()
                    if key in DARAdresse.__table__.columns.keys() and key != "id"
                },
            )
            self.session.add(sql_address)
        self.session.commit()
        if output:
            for result in self.engine.execute("select * from dar_adresser limit 10"):
                print(result.items())

    def _add_associations(self, output=False):
        logger.info("Add associations")
        for association, association_validity in tqdm(
            self.lc.associations.items(), desc="Export association", unit="association"
        ):
            for association_info in association_validity:
                association_type_uuid, association_type_class = self._get_lora_class(
                    association_info["association_type"]
                )
                job_function_uuid, job_function_class = self._get_lora_class(
                    association_info["job_function"]
                )
                sql_association = Tilknytning(
                    uuid=association,
                    bruger_uuid=association_info["user"],
                    enhed_uuid=association_info["unit"],
                    bvn=association_info["user_key"],
                    tilknytningstype_uuid=association_type_uuid,
                    tilknytningstype_titel=association_type_class["title"],
                    startdato=association_info["from_date"],
                    slutdato=association_info["to_date"],
                    it_forbindelse_uuid=association_info["it_user"],
                    stillingsbetegnelse_uuid=job_function_uuid,
                    stillingsbetegnelse_titel=job_function_class["title"],
                    primær_boolean=association_info.get("primary_boolean"),
                )
                self.session.add(sql_association)
            self.session.commit()

        for role, role_validity in tqdm(
            self.lc.roles.items(), desc="Export role", unit="role"
        ):
            for role_info in role_validity:
                sql_role = Rolle(
                    uuid=role,
                    bruger_uuid=role_info["user"],
                    enhed_uuid=role_info["unit"],
                    rolletype_uuid=role_info["role_type"],
                    rolletype_titel=self.lc.classes[role_info["role_type"]]["title"],
                    startdato=role_info["from_date"],
                    slutdato=role_info["to_date"],
                )
                self.session.add(sql_role)
            self.session.commit()

        for leave, leave_validity in tqdm(
            self.lc.leaves.items(), desc="Export leave", unit="leave"
        ):
            for leave_info in leave_validity:
                leave_type = leave_info["leave_type"]
                sql_leave = Orlov(
                    uuid=leave,
                    bvn=leave_info["user_key"],
                    bruger_uuid=leave_info["user"],
                    orlovstype_uuid=leave_type,
                    orlovstype_titel=self.lc.classes[leave_type]["title"],
                    engagement_uuid=leave_info["engagement"],
                    startdato=leave_info["from_date"],
                    slutdato=leave_info["to_date"],
                )
                self.session.add(sql_leave)
            self.session.commit()
        if output:
            for result in self.engine.execute("select * from tilknytninger limit 4"):
                print(result.items())
            for result in self.engine.execute("select * from orlover limit 4"):
                print(result.items())
            for result in self.engine.execute("select * from roller limit 4"):
                print(result.items())

    def _add_it_systems(self, output=False):
        logger.info("Add IT systems")
        for itsystem, itsystem_info in tqdm(
            self.lc.itsystems.items(), desc="Export itsystem", unit="itsystem"
        ):
            sql_itsystem = ItSystem(uuid=itsystem, navn=itsystem_info["name"])
            self.session.add(sql_itsystem)
        self.session.commit()

        for it_connection, it_connection_validity in tqdm(
            self.lc.it_connections.items(),
            desc="Export it connection",
            unit="it connection",
        ):
            for it_connection_info in it_connection_validity:
                sql_it_connection = ItForbindelse(
                    uuid=it_connection,
                    it_system_uuid=it_connection_info["itsystem"],
                    bruger_uuid=it_connection_info["user"],
                    enhed_uuid=it_connection_info["unit"],
                    brugernavn=it_connection_info["username"],
                    startdato=it_connection_info["from_date"],
                    slutdato=it_connection_info["to_date"],
                    primær_boolean=it_connection_info.get("primary_boolean"),
                )
                self.session.add(sql_it_connection)
            self.session.commit()
        if output:
            for result in self.engine.execute("select * from it_systemer limit 2"):
                print(result.items())

            for result in self.engine.execute("select * from it_forbindelser limit 2"):
                print(result.items())

    def _add_kles(self, output=False):
        logger.info("Add KLES")
        for kle, kle_validity in tqdm(
            self.lc.kles.items(), desc="Export KLE", unit="KLE"
        ):
            for kle_info in kle_validity:
                sql_kle = KLE(
                    uuid=kle,
                    enhed_uuid=kle_info["unit"],
                    kle_aspekt_uuid=kle_info["kle_aspect"],
                    kle_aspekt_titel=self.lc.classes[kle_info["kle_aspect"]]["title"],
                    kle_nummer_uuid=kle_info["kle_number"],
                    kle_nummer_titel=self.lc.classes[kle_info["kle_number"]]["title"],
                    startdato=kle_info["from_date"],
                    slutdato=kle_info["to_date"],
                )
                self.session.add(sql_kle)
            self.session.commit()
        if output:
            for result in self.engine.execute("select * from kle limit 10"):
                print(result.items())

    def _add_receipt(self, query_time, start_time=None, end_time=None, output=False):
        logger.info("Add Receipt")
        sql_kvittering = Kvittering(
            query_tid=query_time,
            start_levering_tid=start_time,
            slut_levering_tid=end_time,
        )
        self.session.add(sql_kvittering)
        self.session.commit()
        if output:
            for result in self.engine.execute("select * from kvittering limit 10"):
                print(result.items())
        return sql_kvittering

    def _update_receipt(
        self, sql_kvittering, start_time=None, end_time=None, output=False
    ):
        logger.info("Update Receipt")
        sql_kvittering.start_levering_tid = start_time
        sql_kvittering.slut_levering_tid = end_time
        self.session.commit()
        if output:
            for result in self.engine.execute("select * from kvittering limit 10"):
                print(result.items())

    def _add_related(self, output=False):
        logger.info("Add Enhedssammenkobling")
        for related, related_validity in tqdm(
            self.lc.related.items(), desc="Export related", unit="related"
        ):
            for related_info in related_validity:
                sql_related = Enhedssammenkobling(
                    uuid=related,
                    enhed1_uuid=related_info["unit1_uuid"],
                    enhed2_uuid=related_info["unit2_uuid"],
                    startdato=related_info["from_date"],
                    slutdato=related_info["to_date"],
                )
                self.session.add(sql_related)
            self.session.commit()
        if output:
            for result in self.engine.execute(
                "select * from enhedssammenkobling limit 10"
            ):
                print(result.items())

    def _add_managers(self, output=False):
        logger.info("Add managers")
        for manager, manager_validity in tqdm(
            self.lc.managers.items(), desc="Export manager", unit="manager"
        ):
            for manager_info in manager_validity:
                sql_manager = Leder(
                    uuid=manager,
                    bruger_uuid=manager_info["user"],
                    enhed_uuid=manager_info["unit"],
                    niveautype_uuid=manager_info["manager_level"],
                    ledertype_uuid=manager_info["manager_type"],
                    niveautype_titel=self.lc.classes[manager_info["manager_level"]][
                        "title"
                    ],
                    ledertype_titel=self.lc.classes[manager_info["manager_type"]][
                        "title"
                    ],
                    startdato=manager_info["from_date"],
                    slutdato=manager_info["to_date"],
                )
                self.session.add(sql_manager)

                for responsibility in manager_info["manager_responsibility"]:
                    sql_responsibility = LederAnsvar(
                        leder_uuid=manager,
                        lederansvar_uuid=responsibility,
                        lederansvar_titel=self.lc.classes[responsibility]["title"],
                        startdato=manager_info["from_date"],
                        slutdato=manager_info["to_date"],
                    )
                    self.session.add(sql_responsibility)
            self.session.commit()
        if output:
            for result in self.engine.execute("select * from ledere limit 10"):
                print(result.items())
            for result in self.engine.execute("select * from leder_ansvar limit 10"):
                print(result.items())

    def log_overlapping_runs_aak(self):
        self.engine.execute(
            "INSERT INTO [dbo].[kvittering_afvigelse] "
            "([query_tid],[aarsag]) VALUES (getdate(), "
            "'Time-export: hopper over da foregående "
            "loop stadig kører.')"
        )

    def export(self, resolve_dar: bool, use_pickle: typing.Any) -> None:

        self.perform_export(
            resolve_dar=resolve_dar,
            use_pickle=use_pickle,
        )

        self.swap_tables()


def wrap_export(args: dict, settings: dict) -> None:

    sql_export = SqlExport(
        force_sqlite=args["force_sqlite"],
        historic=args["historic"],
        settings=settings,
    )
    try:
        lock_name = "sql_export_actual"

        if args["historic"]:
            lock_name = "sql_export_historic"

        ensure_single_run(
            func=sql_export.export,
            lock_name=lock_name,
            resolve_dar=args["resolve_dar"],
            use_pickle=args["read_from_cache"],
        )

    except ra_utils.ensure_single_run.LockTaken as name_of_lock:
        logger.warning(f"Lock {name_of_lock} taken, aborting export")
        if "log_overlapping_aak" in settings and settings.get("log_overlapping_aak"):
            sql_export.log_overlapping_runs_aak()


@click.command(help="SQL export")
@click.option("--resolve-dar", is_flag=True)
@click.option("--historic", is_flag=True)
@click.option("--read-from-cache", is_flag=True, envvar="USE_CACHED_LORACACHE")
@click.option("--force-sqlite", is_flag=True)
def cli(**args):
    """
    Command line interface.
    """
    pydantic_settings = SqlExportSettings()
    pydantic_settings.start_logging_based_on_settings()
    logger.info("Command line args: %r", args)

    settings = load_settings()

    wrap_export(args=args, settings=settings)

    logger.info("*SQL export ended*")


if __name__ == "__main__":
    cli()
