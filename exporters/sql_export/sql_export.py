import datetime
import logging
import typing
from typing import Tuple
from typing import Type
from typing import TypeVar
from uuid import UUID

import click
import ra_utils.ensure_single_run
from alembic.migration import MigrationContext
from alembic.operations import Operations
from more_itertools import ichunked
from more_itertools import one
from ra_utils.ensure_single_run import ensure_single_run
from ra_utils.job_settings import JobSettings
from ra_utils.load_settings import load_settings
from ra_utils.tqdm_wrapper import tqdm
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from .gql_lora_cache_async import GQLLoraCache
from .lora_cache import get_cache as LoraCache
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
from .sql_table_defs import sql_type
from .sql_table_defs import Tilknytning
from .sql_table_defs import WAdresse
from .sql_table_defs import WBruger
from .sql_table_defs import WDARAdresse
from .sql_table_defs import WEngagement
from .sql_table_defs import WEnhed
from .sql_table_defs import WEnhedssammenkobling
from .sql_table_defs import WFacet
from .sql_table_defs import WItForbindelse
from .sql_table_defs import WItSystem
from .sql_table_defs import WKlasse
from .sql_table_defs import WKLE
from .sql_table_defs import WLeder
from .sql_table_defs import WLederAnsvar
from .sql_table_defs import WOrlov
from .sql_table_defs import WRolle
from .sql_table_defs import WTilknytning
from .sql_url import DatabaseFunction
from .sql_url import generate_connection_url
from .sql_url import generate_engine_settings


_T_Facet = TypeVar("_T_Facet", Facet, WFacet)
_T_Klasse = TypeVar("_T_Klasse", Klasse, WKlasse)
_T_Bruger = TypeVar("_T_Bruger", Bruger, WBruger)
_T_Enhed = TypeVar("_T_Enhed", Enhed, WEnhed)
_T_Adresse = TypeVar("_T_Adresse", Adresse, WAdresse)
_T_Engagement = TypeVar("_T_Engagement", Engagement, WEngagement)
_T_Rolle = TypeVar("_T_Rolle", Rolle, WRolle)
_T_Tilknytning = TypeVar("_T_Tilknytning", Tilknytning, WTilknytning)
_T_Orlov = TypeVar("_T_Orlov", Orlov, WOrlov)
_T_ItSystem = TypeVar("_T_ItSystem", ItSystem, WItSystem)
_T_ItForbindelse = TypeVar("_T_ItForbindelse", ItForbindelse, WItForbindelse)
_T_Leder = TypeVar("_T_Leder", Leder, WLeder)
_T_LederAnsvar = TypeVar("_T_LederAnsvar", LederAnsvar, WLederAnsvar)
_T_KLE = TypeVar("_T_KLE", KLE, WKLE)
_T_Enhedssammenkobling = TypeVar(
    "_T_Enhedssammenkobling", Enhedssammenkobling, WEnhedssammenkobling
)
_T_DARAdresse = TypeVar("_T_DARAdresse", DARAdresse, WDARAdresse)


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
        self.chunk_size = 5000
        self.lc = None

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

    def _get_lora_cache(self, resolve_dar, use_pickle) -> GQLLoraCache:
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

        tables = dict(Base.metadata.tables)

        logger.info("Dropping work tables")
        Base.metadata.drop_all(
            self.engine,
            tables=[table for name, table in tables.items() if name[0] == "w"],
        )
        logger.info("Ensure work tables and 'kvittering' exists")
        Base.metadata.create_all(
            self.engine,
            tables=[
                table
                for name, table in tables.items()
                if name[0] == "w" or name == "kvittering"
            ],
        )

        self.session = self._get_db_session()

        query_time = timestamp()
        kvittering = self._add_receipt(query_time)
        self.lc = self.lc or self._get_lora_cache(resolve_dar, use_pickle)

        start_delivery_time = timestamp()
        self._update_receipt(kvittering, start_delivery_time)

        tasks = [
            self._add_facets,
            self._add_classes,
            self._add_units,
            self._add_users,
            self._add_addresses,
            self._add_dar_addresses,
            self._add_engagements,
            self._add_associations,
            self._add_roles,
            self._add_leaves,
            self._add_managers,
            self._add_it_systems,
            self._add_it_users,
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
        tables = {t for t in tables if t[0] == "w"}
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

    def _generate_sql_facets(self, uuid, facet_info, model: Type[_T_Facet]) -> _T_Facet:
        return model(
            uuid=str(uuid),
            bvn=facet_info["user_key"],
        )

    def _add_facets(self) -> None:
        logger.info("Add classification")
        facets = tqdm(self.lc.facets.items(), desc="Export facet", unit="facet")
        for chunk in ichunked(facets, self.chunk_size):
            for uuid, facet_info in chunk:
                sql_facet = self._generate_sql_facets(uuid, facet_info, WFacet)
                self.session.add(sql_facet)
            self.session.commit()

    def _generate_sql_classes(
        self, uuid, klasse_info, model: Type[_T_Klasse]
    ) -> _T_Klasse:
        return model(
            uuid=str(uuid),
            bvn=klasse_info["user_key"],
            titel=klasse_info["title"],
            facet_uuid=klasse_info["facet"],
            facet_bvn=self.lc.facets[klasse_info["facet"]]["user_key"],
        )

    def _add_classes(self) -> None:
        classes = tqdm(self.lc.classes.items(), desc="Export class", unit="class")
        for chunk in ichunked(classes, self.chunk_size):
            for uuid, klasse_info in chunk:
                sql_class = self._generate_sql_classes(uuid, klasse_info, WKlasse)
                self.session.add(sql_class)
            self.session.commit()

    def _generate_sql_users(self, uuid, user_info, model: Type[_T_Bruger]) -> _T_Bruger:
        return model(
            uuid=str(uuid),
            bvn=user_info["user_key"],
            fornavn=user_info["fornavn"],
            efternavn=user_info["efternavn"],
            kaldenavn_fornavn=user_info["kaldenavn_fornavn"],
            kaldenavn_efternavn=user_info["kaldenavn_efternavn"],
            cpr=user_info["cpr"] if self.export_cpr else "",
            startdato=user_info["from_date"],
            slutdato=user_info["to_date"],
        )

    def _add_users(self) -> None:
        logger.info("Add users")
        users = tqdm(self.lc.users.items(), desc="Export user", unit="user")
        for chunk in ichunked(users, self.chunk_size):
            for uuid, user_effects in chunk:
                for user_info in user_effects:
                    sql_user = self._generate_sql_users(uuid, user_info, WBruger)
                    self.session.add(sql_user)
            self.session.commit()

    def _generate_sql_units(self, uuid, unit_info, model: Type[_T_Enhed]) -> _T_Enhed:
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

        return model(
            uuid=str(uuid),
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

    def _add_units(self) -> None:
        logger.info("Add users")
        units = tqdm(self.lc.units.items(), desc="Export unit", unit="unit")
        for chunk in ichunked(units, self.chunk_size):
            for uuid, unit_validities in chunk:
                for unit_info in unit_validities:
                    sql_unit = self._generate_sql_units(uuid, unit_info, WEnhed)
                    self.session.add(sql_unit)
            self.session.commit()

    def _generate_sql_engagements(
        self, uuid, engagement_info, model: Type[_T_Engagement]
    ) -> _T_Engagement:
        if engagement_info["primary_type"] is not None:
            primærtype_titel = self.lc.classes[engagement_info["primary_type"]]["title"]
        else:
            primærtype_titel = ""

        engagement_type_uuid = engagement_info["engagement_type"]
        job_function_uuid, job_function_class = self._get_lora_class(
            engagement_info["job_function"]
        )

        return model(
            uuid=str(uuid),
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

    def _add_engagements(self) -> None:
        logger.info("Add engagements")
        engagements = tqdm(
            self.lc.engagements.items(), desc="Export engagement", unit="engagement"
        )
        for chunk in ichunked(engagements, self.chunk_size):
            for uuid, engagement_validity in chunk:
                for engagement_info in engagement_validity:
                    sql_engagement = self._generate_sql_engagements(
                        uuid, engagement_info, WEngagement
                    )
                    self.session.add(sql_engagement)
            self.session.commit()

    def _generate_sql_addresses(
        self, uuid, address_info, model: Type[_T_Adresse]
    ) -> _T_Adresse:
        visibility_text = None
        if address_info["visibility"] is not None:
            visibility_text = self.lc.classes[address_info["visibility"]]["title"]
        visibility_scope = None
        if address_info["visibility"] is not None:
            visibility_scope = self.lc.classes[address_info["visibility"]]["scope"]

        return model(
            uuid=str(uuid),
            enhed_uuid=address_info["unit"],
            bruger_uuid=address_info["user"],
            værdi=address_info["value"],
            dar_uuid=address_info["dar_uuid"],
            adressetype_uuid=address_info["adresse_type"],
            adressetype_bvn=self.lc.classes[address_info["adresse_type"]]["user_key"],
            adressetype_scope=address_info["scope"],
            adressetype_titel=self.lc.classes[address_info["adresse_type"]]["title"],
            synlighed_uuid=address_info["visibility"],
            synlighed_scope=visibility_scope,
            synlighed_titel=visibility_text,
            startdato=address_info["from_date"],
            slutdato=address_info["to_date"],
        )

    def _add_addresses(self) -> None:
        logger.info("Add addresses")
        addresses = tqdm(
            self.lc.addresses.items(), desc="Export address", unit="address"
        )
        for chunk in ichunked(addresses, self.chunk_size):
            for uuid, address_validities in chunk:
                for address_info in address_validities:
                    sql_address = self._generate_sql_addresses(
                        uuid, address_info, WAdresse
                    )
                    self.session.add(sql_address)
            self.session.commit()

    def _generate_sql_dar_addresses(
        self, uuid, address_info, model: Type[_T_DARAdresse]
    ) -> _T_DARAdresse:
        return model(
            uuid=str(uuid),
            **{
                key: value
                for key, value in address_info.items()
                if key in model.__table__.columns.keys() and key not in ("id", "uuid")
            },
        )

    def _add_dar_addresses(self) -> None:
        logger.info("Add DAR addresses")
        dar = tqdm(self.lc.dar_cache.items(), desc="Export DAR", unit="DAR")
        for chunk in ichunked(dar, self.chunk_size):
            for uuid, address_info in chunk:
                sql_address = self._generate_sql_dar_addresses(
                    uuid, address_info, WDARAdresse
                )
                self.session.add(sql_address)
            self.session.commit()

    def _generate_sql_associations(
        self, uuid, association_info, model: Type[_T_Tilknytning]
    ) -> _T_Tilknytning:
        (
            association_type_uuid,
            association_type_class,
        ) = self._get_lora_class(association_info["association_type"])
        job_function_uuid, job_function_class = self._get_lora_class(
            association_info["job_function"]
        )
        return model(
            uuid=str(uuid),
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
            faglig_organisation=association_info.get("dynamic_class"),
        )

    def _add_associations(self) -> None:
        logger.info("Add associations")
        associations = tqdm(
            self.lc.associations.items(), desc="Export association", unit="association"
        )
        for chunk in ichunked(associations, self.chunk_size):
            for uuid, association_validity in chunk:
                for association_info in association_validity:
                    sql_association = self._generate_sql_associations(
                        uuid, association_info, WTilknytning
                    )
                    self.session.add(sql_association)
            self.session.commit()

    def _generate_sql_role(self, uuid, role_info, model: Type[_T_Rolle]) -> _T_Rolle:
        return model(
            uuid=str(uuid),
            bruger_uuid=role_info["user"],
            enhed_uuid=role_info["unit"],
            rolletype_uuid=role_info["role_type"],
            rolletype_titel=self.lc.classes[role_info["role_type"]]["title"],
            startdato=role_info["from_date"],
            slutdato=role_info["to_date"],
        )

    def _add_roles(self) -> None:
        logger.info("Add roles")

        roles = tqdm(self.lc.roles.items(), desc="Export role", unit="role")
        for chunk in ichunked(roles, self.chunk_size):
            for uuid, role_validity in chunk:
                for role_info in role_validity:
                    sql_role = self._generate_sql_role(uuid, role_info, WRolle)
                    self.session.add(sql_role)
            self.session.commit()

    def _generate_sql_leave(self, uuid, leave_info, model: Type[_T_Orlov]) -> _T_Orlov:
        leave_type = leave_info["leave_type"]
        return model(
            uuid=str(uuid),
            bvn=leave_info["user_key"],
            bruger_uuid=leave_info["user"],
            orlovstype_uuid=leave_type,
            orlovstype_titel=self.lc.classes[leave_type]["title"],
            engagement_uuid=leave_info["engagement"],
            startdato=leave_info["from_date"],
            slutdato=leave_info["to_date"],
        )

    def _add_leaves(self) -> None:
        logger.info("Add leaves")
        leaves = tqdm(self.lc.leaves.items(), desc="Export leave", unit="leave")
        for chunk in ichunked(leaves, self.chunk_size):
            for uuid, leave_validity in chunk:
                for leave_info in leave_validity:
                    sql_leave = self._generate_sql_leave(uuid, leave_info, WOrlov)
                    self.session.add(sql_leave)
            self.session.commit()

    def _generate_sql_it_systems(
        self, uuid, itsystem_info, model: Type[_T_ItSystem]
    ) -> _T_ItSystem:
        return model(uuid=str(uuid), navn=itsystem_info["name"])

    def _add_it_systems(self) -> None:
        logger.info("Add IT systems")
        itsystems = tqdm(
            self.lc.itsystems.items(), desc="Export itsystem", unit="itsystem"
        )
        for chunk in ichunked(itsystems, self.chunk_size):
            for uuid, itsystem_info in chunk:
                sql_itsystem = self._generate_sql_it_systems(
                    uuid, itsystem_info, WItSystem
                )
                self.session.add(sql_itsystem)
            self.session.commit()

    def _generate_sql_it_user(
        self, uuid, it_connection_info, model: Type[_T_ItForbindelse]
    ) -> _T_ItForbindelse:
        return model(
            uuid=str(uuid),
            it_system_uuid=it_connection_info["itsystem"],
            bruger_uuid=it_connection_info["user"],
            enhed_uuid=it_connection_info["unit"],
            brugernavn=it_connection_info["username"],
            startdato=it_connection_info["from_date"],
            slutdato=it_connection_info["to_date"],
            primær_boolean=it_connection_info.get("primary_boolean"),
        )

    def _add_it_users(self):
        logger.info("Add IT users")
        it_connections = tqdm(
            self.lc.it_connections.items(),
            desc="Export it connection",
            unit="it connection",
        )
        for chunk in ichunked(it_connections, self.chunk_size):
            for uuid, it_connection_validity in chunk:
                for it_connection_info in it_connection_validity:
                    sql_it_connection = self._generate_sql_it_user(
                        uuid, it_connection_info, WItForbindelse
                    )
                    self.session.add(sql_it_connection)
            self.session.commit()

    def _generate_sql_kle(self, uuid, kle_info, model: Type[_T_KLE]) -> _T_KLE:
        return model(
            uuid=str(uuid),
            enhed_uuid=kle_info["unit"],
            kle_aspekt_uuid=kle_info["kle_aspect"],
            kle_aspekt_titel=self.lc.classes[kle_info["kle_aspect"]]["title"],
            kle_nummer_uuid=kle_info["kle_number"],
            kle_nummer_titel=self.lc.classes[kle_info["kle_number"]]["title"],
            startdato=kle_info["from_date"],
            slutdato=kle_info["to_date"],
        )

    def _add_kles(self) -> None:
        logger.info("Add KLES")
        kles = tqdm(self.lc.kles.items(), desc="Export KLE", unit="KLE")
        for chunk in ichunked(kles, self.chunk_size):
            for uuid, kle_validity in chunk:
                for kle_info in kle_validity:
                    sql_kle = self._generate_sql_kle(uuid, kle_info, WKLE)
                    self.session.add(sql_kle)
            self.session.commit()

    def _add_receipt(self, query_time, start_time=None, end_time=None):
        logger.info("Add Receipt")
        sql_kvittering = Kvittering(
            query_tid=query_time,
            start_levering_tid=start_time,
            slut_levering_tid=end_time,
        )
        self.session.add(sql_kvittering)
        self.session.commit()
        return sql_kvittering

    def _update_receipt(self, sql_kvittering, start_time=None, end_time=None):
        logger.info("Update Receipt")
        sql_kvittering.start_levering_tid = start_time
        sql_kvittering.slut_levering_tid = end_time
        self.session.commit()

    def _generate_sql_related(
        self, uuid, related_info, model: Type[_T_Enhedssammenkobling]
    ) -> _T_Enhedssammenkobling:
        return model(
            uuid=str(uuid),
            enhed1_uuid=related_info["unit1_uuid"],
            enhed2_uuid=related_info["unit2_uuid"],
            startdato=related_info["from_date"],
            slutdato=related_info["to_date"],
        )

    def _add_related(self) -> None:
        logger.info("Add Enhedssammenkobling")
        relateds = tqdm(self.lc.related.items(), desc="Export related", unit="related")
        for chunk in ichunked(relateds, self.chunk_size):
            for uuid, related_validity in chunk:
                for related_info in related_validity:
                    sql_related = self._generate_sql_related(
                        uuid, related_info, WEnhedssammenkobling
                    )
                    self.session.add(sql_related)
            self.session.commit()

    def _generate_sql_managers(
        self, uuid, manager_info, model: Type[_T_Leder]
    ) -> _T_Leder:
        return model(
            uuid=str(uuid),
            bruger_uuid=manager_info["user"],
            enhed_uuid=manager_info["unit"],
            niveautype_uuid=manager_info["manager_level"],
            ledertype_uuid=manager_info["manager_type"],
            niveautype_titel=self.lc.classes[manager_info["manager_level"]]["title"],
            ledertype_titel=self.lc.classes[manager_info["manager_type"]]["title"],
            startdato=manager_info["from_date"],
            slutdato=manager_info["to_date"],
        )

    def _generate_sql_manager_responsibility(
        self, uuid, manager_uuid, manager_info, model: Type[_T_LederAnsvar]
    ) -> _T_LederAnsvar:
        return model(
            leder_uuid=str(manager_uuid),
            lederansvar_uuid=uuid,
            lederansvar_titel=self.lc.classes[uuid]["title"],
            startdato=manager_info["from_date"],
            slutdato=manager_info["to_date"],
        )

    def _add_managers(self) -> None:
        logger.info("Add managers")
        managers = tqdm(self.lc.managers.items(), desc="Export manager", unit="manager")
        for chunk in ichunked(managers, self.chunk_size):
            for manager_uuid, manager_validity in chunk:
                for manager_info in manager_validity:
                    sql_manager = self._generate_sql_managers(
                        manager_uuid, manager_info, WLeder
                    )
                    self.session.add(sql_manager)

                    for responsibility_uuid in manager_info["manager_responsibility"]:
                        sql_responsibility = self._generate_sql_manager_responsibility(
                            responsibility_uuid,
                            manager_uuid,
                            manager_info,
                            WLederAnsvar,
                        )
                        self.session.add(sql_responsibility)
            self.session.commit()

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

    def update_sql(self, uuid: UUID, objects: list[sql_type], table: Type[sql_type]):
        """Updates sql with the provided objects matching the objects UUID.

        Given a UUID, a list of objects and a table  we find any objects currently in sql for the given uuid.
        Then we add the objects that are not allready in sql - either new or changed in MO -  and remove any that
        do not match.
        """
        search_key = table.leder_uuid if table == LederAnsvar else table.uuid
        # Lookup engagement in sql
        current_objects = self.session.execute(
            select(table).where(search_key == str(uuid))
        ).all()
        if current_objects is not None:
            current_objects = [one(c) for c in current_objects]

        unchanged = [n for n in current_objects if n in objects]

        # Delete all rows from sql that do not match the found objects
        removed = [r for r in current_objects if r not in objects]
        logger.info(f"Delete {len(removed)} rows to {table} for {uuid=}")
        for r in removed:
            self.session.delete(r)

        # Create all rows not currently in sql
        new = [n for n in objects if n not in current_objects]
        logger.info(f"Add {len(new)} rows to {table} for {uuid=}")
        for n in new:
            self.session.add(n)

        # Check that the result is the expected amount of rows in sql.
        assert len(objects) == len(unchanged) + len(
            new
        ), f"expected {len(objects)=} to be equal to {len(unchanged)=} + {len(new)=}"

        self.session.commit()


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
@click.option("--resolve-dar", is_flag=True, envvar="RESOLVE_DAR")
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
