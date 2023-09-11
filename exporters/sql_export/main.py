# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration entrypoint."""
import logging

import sentry_sdk
from fastapi import APIRouter
from fastapi import FastAPI
from fastramqpi.config import Settings as FastRAMQPISettings
from fastramqpi.main import FastRAMQPI
from more_itertools import one
from more_itertools import only
from ramqp.depends import Context
from ramqp.depends import RateLimit
from ramqp.mo import MORouter
from ramqp.mo import PayloadUUID
from sqlalchemy import select

from .config import DatabaseSettings
from .config import GqlLoraCacheSettings
from .gql_lora_cache_async import GQLLoraCache
from .sql_export import SqlExport
from .sql_table_defs import Engagement
from .trigger import trigger_router

logger = logging.getLogger(__name__)

fastapi_router = APIRouter()

amqp_router = MORouter()


class Settings(GqlLoraCacheSettings):
    fastramqpi: FastRAMQPISettings
    eventdriven: bool = False
    resolve_dar: bool = False
    full_history: bool = False
    skip_past: bool = False

    class Config:
        frozen = True
        env_nested_delimiter = "__"


@amqp_router.register("engagement")
async def export_engagements(context: Context, uuid: PayloadUUID, _: RateLimit) -> None:
    loracache: GQLLoraCache = context["user_context"]["loracache"]
    sql_exporter: SqlExport = context["user_context"]["sql_exporter"]

    # This shouldn't  be necessary, it should be configured at startup
    loracache.gql_client_session = context["graphql_session"]
    sql_exporter.lc = loracache

    try:
        res = await loracache._cache_lora_engagements(uuid=uuid)
        res = only(res)
    except ValueError:
        logger.info("No engagement found in MO.")
        res = None
    # TODO: reuse session
    with sql_exporter._get_db_session() as session:

        # Lookup engagement in sql
        current_obj = session.execute(
            select(Engagement).where(Engagement.uuid == str(uuid))
        ).one_or_none()
        if current_obj is not None:
            current_obj = one(current_obj)

        if res:
            # The engagement exists in MO. We need to create or update it in sql.
            sql_obj = await sql_exporter._generate_sql_engagement(uuid, res)
            if current_obj:
                # Update:
                # use primary_key from existing row to update
                sql_obj.id = current_obj.id
                session.merge(sql_obj)
                logger.info(f"Updated existing engagement {uuid=}")
            else:
                # Create:
                session.add(sql_obj)
                logger.info(f"Created new engagement {uuid=}")
        elif current_obj:
            # Delete:
            session.delete(current_obj)
            logger.info(f"Deleted engagement {uuid=}")
        else:
            logger.info("Engagement not in os2mo or sql.")
        session.commit()


@fastapi_router.get("/")
async def index() -> dict[str, str]:
    return {"name": "sql_export"}


def create_fastramqpi(**kwargs) -> FastRAMQPI:
    settings: Settings = Settings(**kwargs)
    settings.start_logging_based_on_settings()
    if settings.sentry_dsn:
        sentry_sdk.init(dsn=settings.sentry_dsn)

    fastramqpi = FastRAMQPI(application_name="sql-export", settings=settings.fastramqpi)
    if settings.eventdriven:
        assert (
            settings.use_new_cache
        ), "Eventdriven sql-export requires graphql-based loracache. Set 'USE_NEW_CACHE': 'true'"
        amqpsystem = fastramqpi.get_amqpsystem()
        amqpsystem.router.registry.update(amqp_router.registry)

        fastramqpi.add_context(
            loracache=GQLLoraCache(
                settings=settings,
                resolve_dar=settings.resolve_dar,
                full_history=settings.full_history,
                skip_past=settings.skip_past,
            ),
            sql_exporter=SqlExport(
                settings=DatabaseSettings().to_old_settings(),
                historic=settings.full_history,
            ),
        )
    fastramqpi.get_app().include_router(trigger_router)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    return fastramqpi


def create_app(**kwargs) -> FastAPI:
    fastramqpi = create_fastramqpi(**kwargs)
    return fastramqpi.get_app()
