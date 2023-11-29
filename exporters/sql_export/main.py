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
from more_itertools import partition
from ra_utils.job_settings import JobSettings
from ramqp.depends import RateLimit
from ramqp.mo import MORouter
from ramqp.mo import MORoutingKey
from ramqp.mo import PayloadUUID
from sqlalchemy import inspect

from .config import DatabaseSettings
from .config import GqlLoraCacheSettings
from .equivalence_test.equivalence_test import trigger_equiv_router
from .gql_lora_cache_async import GQLLoraCache
from .sql_export import SqlExport
from .sql_table_defs import DARAdresse
from .sql_table_defs import LederAnsvar
from .sql_table_defs import type_map
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

    sql_func = sql_exporter.generate_functions_map.get(key)
    if sql_func is None:
        logger.warn("No matching cache functions for this change.")
        return

    sql_objects = [sql_obj async for sql_obj in sql_func(str(uuid))]

    if key == "address":
        sql_objects, dar_adresses = partition(
            lambda m: isinstance(m, DARAdresse), sql_objects  # type: ignore
        )
        sql_exporter.check_sql(uuid, list(dar_adresses), DARAdresse)  # type: ignore
    elif key == "manager":
        sql_objects, responsibility_objects = partition(
            lambda m: isinstance(m, LederAnsvar), sql_objects
        )
        sql_exporter.check_sql(
            uuid, list(responsibility_objects), LederAnsvar  # type: ignore
        )

    sql_exporter.check_sql(uuid, list(sql_objects), type_map[key])  # type: ignore


class Settings(JobSettings):
    fastramqpi: FastRAMQPISettings
    eventdriven: bool = False
    full_history: bool = False

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
        amqpsystem.router.registry.update(amqp_router.registry)
    fastramqpi.get_app().include_router(trigger_router)
    fastramqpi.get_app().include_router(trigger_equiv_router)
    fastramqpi.add_context(settings=settings, sql_exporter=None)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    context = fastramqpi.get_context()

    @asynccontextmanager
    async def sql_exporter() -> AsyncGenerator[None, None]:
        lc = GQLLoraCache(
            settings=GqlLoraCacheSettings().to_old_settings(),
            full_history=settings.full_history,
            graphql_session=context["graphql_session"],
        )
        await lc._cache_lora_classes()
        sql_exporter = SqlExport(
            settings=DatabaseSettings(use_new_cache=True).to_old_settings()
        )
        sql_exporter.lc = lc
        sql_exporter.session = sql_exporter._get_db_session()
        # Check wether the tables exists (https://stackoverflow.com/a/64862306)
        ins = inspect(sql_exporter.engine)
        if not ins.dialect.has_table(sql_exporter.engine.connect(), "brugere"):
            await sql_exporter.lc.populate_cache_async()
            sql_exporter.export(resolve_dar=True, use_pickle=False)

        yield

    fastramqpi.add_lifespan_manager(sql_exporter(), priority=1100)

    return fastramqpi.get_app()
