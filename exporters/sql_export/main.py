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
from ra_utils.job_settings import JobSettings

from .tests.test_gql_lora_cache_equivalence import trigger_equiv_router
from .trigger import trigger_router

logger = logging.getLogger(__name__)

fastapi_router = APIRouter()


class Settings(JobSettings):
    fastramqpi: FastRAMQPISettings

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
    fastramqpi.get_app().include_router(trigger_router)
    fastramqpi.get_app().include_router(trigger_equiv_router)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    return fastramqpi


def create_app(**kwargs) -> FastAPI:
    fastramqpi = create_fastramqpi(**kwargs)
    return fastramqpi.get_app()
