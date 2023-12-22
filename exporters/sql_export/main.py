# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration entrypoint."""
import logging
from contextlib import asynccontextmanager
from typing import Annotated
from typing import AsyncGenerator

import sentry_sdk
from fastapi import APIRouter
from fastapi import Depends
from fastapi import FastAPI
from fastramqpi.config import Settings as FastRAMQPISettings
from fastramqpi.depends import from_user_context
from fastramqpi.main import FastRAMQPI
from ramqp.depends import RateLimit
from ramqp.mo import MORouter
from ramqp.mo import MORoutingKey
from ramqp.mo import PayloadUUID

from .config import DatabaseSettings
from .config import GqlLoraCacheSettings
from .equivalence_test.equivalence_test import trigger_equiv_router
from .gql_lora_cache_async import GQLLoraCache
from .sql_export import SqlExport as _SqlExport
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
from .sql_table_defs import Leder
from .sql_table_defs import LederAnsvar
from .sql_table_defs import Orlov
from .sql_table_defs import Rolle
from .sql_table_defs import Tilknytning
from .trigger import trigger_router

logger = logging.getLogger(__name__)

fastapi_router = APIRouter()
actualstate_router = MORouter()
historic_router = MORouter()

SqlExport = Annotated[_SqlExport, Depends(from_user_context("sql_exporter"))]
SqlExportHistoric = Annotated[
    _SqlExport, Depends(from_user_context("sql_exporter_historic"))
]


async def handle_address(uuid: PayloadUUID, sql_exporter: SqlExport):
    result = await sql_exporter.lc._fetch_address(uuid)
    address_objects = []
    dar_address_objects = []
    for res in result.get(str(uuid), []):
        if res["scope"] == "DAR":
            dar_address_objects.append(
                sql_exporter._generate_sql_dar_addresses(uuid, res, DARAdresse)
            )

        address_objects.append(sql_exporter._generate_sql_addresses(uuid, res, Adresse))

    sql_exporter.update_sql(uuid, dar_address_objects, DARAdresse)
    sql_exporter.update_sql(uuid, address_objects, Adresse)


async def handle_association(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_associations(uuid)

    association_objects = [
        sql_exporter._generate_sql_associations(uuid, res, Tilknytning)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, association_objects, Tilknytning)


async def handle_class(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_classes(uuid)
    res = result.get(str(uuid))
    class_objects = (
        [sql_exporter._generate_sql_classes(uuid, res, Klasse)] if res else []
    )
    sql_exporter.update_sql(uuid, class_objects, Klasse)


async def handle_engagement(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_engagements(uuid)
    engagements_objects = [
        sql_exporter._generate_sql_engagements(uuid, res, Engagement)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, engagements_objects, Engagement)


async def handle_facet(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_facets(uuid)
    res = result.get(str(uuid))
    facets_objects = (
        [sql_exporter._generate_sql_facets(uuid, res, Facet)] if res else []
    )

    sql_exporter.update_sql(uuid, facets_objects, Facet)


async def handle_it_system(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_itsystems(uuid)
    res = result.get(str(uuid))
    itsystems_objects = (
        [sql_exporter._generate_sql_it_systems(uuid, res, ItSystem)] if res else []
    )

    sql_exporter.update_sql(uuid, itsystems_objects, ItSystem)


async def handle_it_user(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_it_connections(uuid)
    it_connections_objects = [
        sql_exporter._generate_sql_it_user(uuid, res, ItForbindelse)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, it_connections_objects, ItForbindelse)


async def handle_kle(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_kles(uuid)
    kle_objects = [
        sql_exporter._generate_sql_kle(uuid, res, KLE)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, kle_objects, KLE)


async def handle_leave(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_leaves(uuid)
    leaves_objects = [
        sql_exporter._generate_sql_leave(uuid, res, Orlov)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, leaves_objects, Orlov)


async def handle_manager(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_managers(uuid)
    managers_objects = []
    for res in result.get(str(uuid), []):
        managers_objects.append(sql_exporter._generate_sql_managers(uuid, res, Leder))

        for responsibility_uuid in res["manager_responsibility"]:
            manager_responsibility_objects = [
                sql_exporter._generate_sql_manager_responsibility(
                    responsibility_uuid, uuid, res, LederAnsvar
                )
                for res in result.get(str(uuid), [])
            ]
            sql_exporter.update_sql(uuid, manager_responsibility_objects, LederAnsvar)
    sql_exporter.update_sql(uuid, managers_objects, Leder)


async def handle_related(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_related(uuid)
    related_objects = [
        sql_exporter._generate_sql_related(uuid, res, Enhedssammenkobling)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, related_objects, Enhedssammenkobling)


async def handle_role(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_roles(uuid)
    roles_objects = [
        sql_exporter._generate_sql_role(uuid, res, Rolle)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, roles_objects, Rolle)


async def handle_org_unit(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_units(uuid)
    units_objects = [
        sql_exporter._generate_sql_units(uuid, res, Enhed)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, units_objects, Enhed)


async def handle_person(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
):
    result = await sql_exporter.lc._fetch_users(uuid)
    users_objects = [
        sql_exporter._generate_sql_users(uuid, res, Bruger)
        for res in result.get(str(uuid), [])
    ]

    sql_exporter.update_sql(uuid, users_objects, Bruger)


handle_function_map = {
    "address": handle_address,
    "association": handle_association,
    "class": handle_class,
    "engagement": handle_engagement,
    "facet": handle_facet,
    "itsystem": handle_it_system,
    "ituser": handle_it_user,
    "kle": handle_kle,
    "leave": handle_leave,
    "manager": handle_manager,
    "related": handle_related,
    "role": handle_role,
    "org_unit": handle_org_unit,
    "person": handle_person,
}


@actualstate_router.register("address")
@actualstate_router.register("association")
@actualstate_router.register("class")
@actualstate_router.register("engagement")
@actualstate_router.register("facet")
@actualstate_router.register("itsystem")
@actualstate_router.register("ituser")
@actualstate_router.register("kle")
@actualstate_router.register("leave")
@actualstate_router.register("manager")
@actualstate_router.register("related")
@actualstate_router.register("role")
@actualstate_router.register("org_unit")
@actualstate_router.register("person")
async def trigger_actual_state_event(
    uuid: PayloadUUID,
    sql_exporter: SqlExport,
    key: MORoutingKey,
    _: RateLimit,
):
    handle_function = handle_function_map[key]
    return await handle_function(uuid=uuid, sql_exporter=sql_exporter)


@historic_router.register("address")
@historic_router.register("association")
@historic_router.register("class")
@historic_router.register("engagement")
@historic_router.register("facet")
@historic_router.register("itsystem")
@historic_router.register("ituser")
@historic_router.register("kle")
@historic_router.register("leave")
@historic_router.register("manager")
@historic_router.register("related")
@historic_router.register("role")
@historic_router.register("org_unit")
@historic_router.register("person")
async def trigger_historic_event(
    uuid: PayloadUUID,
    sql_exporter: SqlExportHistoric,
    key: MORoutingKey,
    _: RateLimit,
):
    handle_function = handle_function_map[key]
    return await handle_function(uuid=uuid, sql_exporter=sql_exporter)


class Settings(DatabaseSettings):
    fastramqpi: FastRAMQPISettings
    eventdriven: bool = False

    class Config:
        frozen = True
        env_nested_delimiter = "__"


@fastapi_router.get("/")
async def index() -> dict[str, str]:
    return {"name": "sql_export"}


def create_app(**kwargs) -> FastAPI:
    settings: Settings = Settings(**kwargs)
    settings.start_logging_based_on_settings()
    if settings.sentry_dsn:
        sentry_sdk.init(dsn=settings.sentry_dsn)

    fastramqpi = FastRAMQPI(application_name="sql-export", settings=settings.fastramqpi)
    if settings.eventdriven:
        amqpsystem = fastramqpi.get_amqpsystem()
        amqpsystem.router.registry.update(actualstate_router.registry)
        if settings.historic_state is not None:
            amqpsystem.router.registry.update(historic_router.registry)
    else:
        fastramqpi.get_app().include_router(trigger_router)
        fastramqpi.get_app().include_router(trigger_equiv_router)
    fastramqpi.add_context(settings=settings, sql_exporter=None)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    context = fastramqpi.get_context()

    @asynccontextmanager
    async def sql_exporter(full_history) -> AsyncGenerator[None, None]:
        lc = GQLLoraCache(
            settings=GqlLoraCacheSettings().to_old_settings(),
            full_history=full_history,
            graphql_session=context["graphql_session"],
        )
        await lc._cache_lora_classes()
        await lc._cache_lora_facets()

        sql_exporter = SqlExport(
            settings=settings.to_old_settings(), historic=full_history
        )
        sql_exporter.lc = lc
        sql_exporter.session = sql_exporter._get_db_session()
        # Ensure that the tables exist
        # TODO: Once we only use event-driven sql-export we can delete the work-tables and "kvittering".
        # Then we can use create_all without the tables argument.
        Base.metadata.create_all(
            sql_exporter.engine,
            tables=[
                table
                for name, table in dict(Base.metadata.tables).items()
                if name[0] != "w" or name != "kvittering"
            ],
            checkfirst=True,
        )

        if full_history:
            fastramqpi.add_context(sql_exporter_historic=sql_exporter)
        else:
            fastramqpi.add_context(sql_exporter=sql_exporter)
        yield

    fastramqpi.add_lifespan_manager(sql_exporter(full_history=False), priority=1100)
    if settings.historic_state is not None:
        fastramqpi.add_lifespan_manager(sql_exporter(full_history=True), priority=1200)

    return fastramqpi.get_app()
