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
from ra_utils.job_settings import JobSettings
from ramqp.depends import RateLimit
from ramqp.mo import MORouter
from ramqp.mo import MORoutingKey
from ramqp.mo import PayloadUUID

from .equivalence_test.equivalence_test import trigger_equiv_router
from .config import DatabaseSettings
from .config import GqlLoraCacheSettings
from .gql_lora_cache_async import GQLLoraCache
from .sql_export import SqlExport
from .trigger import trigger_router

logger = logging.getLogger(__name__)

fastapi_router = APIRouter()
amqp_router = MORouter()


sql_exporter_ = Annotated[SqlExport, Depends(from_user_context("sql_exporter"))]


@amqp_router.register("*")
async def amqp_trigger(
    uuid: PayloadUUID,
    sql_exporter: sql_exporter_,
    key: MORoutingKey,
    _: RateLimit,
):
    logger.info(f"Event Triggered on {key} with {uuid=}")

    # TODO: find a way to set this up at startup
    lc.gql_client_session = gql_session

    func_name = lc.cache_functions_map.get(key)
    if func_name is None:
        logger.warn("No matching cache functions for this change.")
        return
    res = await func_name(uuid)

    print(res)


class Settings(JobSettings):
    fastramqpi: FastRAMQPISettings
    eventdriven: bool = False

    class Config:
        frozen = True
        env_nested_delimiter = "__"


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
        amqpsystem = fastramqpi.get_amqpsystem()
        amqpsystem.router.registry.update(amqp_router.registry)
    fastramqpi.get_app().include_router(trigger_router)
    fastramqpi.get_app().include_router(trigger_equiv_router)
    fastramqpi.add_context(settings=settings, sql_exporter=None)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    return fastramqpi


def create_app(**kwargs) -> FastAPI:
    fastramqpi = create_fastramqpi(**kwargs)
    context = fastramqpi.get_context()

    @asynccontextmanager
    async def sql_exporter() -> AsyncGenerator[None, None]:
        lc = GQLLoraCache(
            settings=GqlLoraCacheSettings().to_old_settings(),
            graphql_session=context["graphql_session"],
        )
        await lc._cache_lora_classes()
        sql_exporter = SqlExport(
            settings=DatabaseSettings(use_new_cache=True).to_old_settings()
        )
        sql_exporter.lc = lc
        fastramqpi.add_context(sql_exporter=sql_exporter)
        yield

    fastramqpi.add_lifespan_manager(sql_exporter(), priority=1100)
    return fastramqpi.get_app()
