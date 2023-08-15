# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration entrypoint."""
import logging
from typing import Dict

import ra_utils
import sentry_sdk
from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastramqpi.config import Settings as FastRAMQPISettings
from fastramqpi.main import FastRAMQPI  # type: ignore
from ra_utils.ensure_single_run import ensure_single_run
from ra_utils.job_settings import JobSettings

from .lora_gql_equivalence_tester import cache_equivalence
from .lora_gql_equivalence_tester import LoraGqlEquivalenceTesterSettings
from .lora_gql_equivalence_tester import notify_prometheus
from .sql_export import SqlExport
from .sql_export import SqlExportSettings
from .trigger import trigger_router

logger = logging.getLogger(__name__)

fastapi_router = APIRouter()


class Settings(FastRAMQPISettings, JobSettings):
    """Settings for the SQLExport FastAPI application."""

    class Config:
        """Settings are frozen."""

        frozen = True


class ExportSettings(LoraGqlEquivalenceTesterSettings, SqlExportSettings):
    class Config:
        pass


def wrap_export(args: dict) -> None:
    settings = ExportSettings()
    sql_export = SqlExport(
        force_sqlite=args["force_sqlite"],
        historic=args["historic"],
        settings=settings,
    )
    try:
        lock_name = "sql_export_actual"

        if args["historic"]:
            lock_name = "sql_export_historic"
        try:
            notify_prometheus(settings=settings, job=args["job_name"], start=True)
            ensure_single_run(
                func=sql_export.export,
                lock_name=lock_name,
                resolve_dar=args["resolve_dar"],
                use_pickle=args["read_from_cache"],
            )
            notify_prometheus(settings=settings, job=args["job_name"])
        except Exception as e:
            notify_prometheus(settings=settings, job=args["job_name"], error=True)
            raise e

    except ra_utils.ensure_single_run.LockTaken as name_of_lock:
        logger.warning(f"Lock {name_of_lock} taken, aborting export")
        if "log_overlapping_aak" in settings and settings.get("log_overlapping_aak"):
            sql_export.log_overlapping_runs_aak()


@fastapi_router.post("/trigger_sql_export", status_code=202)
def trigger_sql_exporter(
    job_name: str,
    historic: bool,
    skip_past: bool,
    resolve_dar: bool,
    dry_run: bool = False,
    skip_associations: bool = False,
):
    args = {
        "job_name": job_name,
        "historic": historic,
        "skip_past": skip_past,
        "resolve_dar": resolve_dar,
        "dry_run": dry_run,
        "skip_associations": skip_associations,
    }
    wrap_export(args)


@fastapi_router.post("/trigger_cache_equivalence", status_code=202)
async def trigger_cache_equivalence(
    background_tasks: BackgroundTasks,
) -> Dict[str, str]:
    background_tasks.add_task(cache_equivalence)
    return {"triggered": "OK"}


@fastapi_router.get("/")
async def index() -> dict[str, str]:
    return {"name": "sql_export"}


def create_fastramqpi(**kwargs) -> FastRAMQPI:
    settings: Settings = Settings(**kwargs)
    settings.start_logging_based_on_settings()
    if settings.sentry_dsn:
        sentry_sdk.init(dsn=settings.sentry_dsn)

    fastramqpi = FastRAMQPI(application_name="sql-export", settings=settings)
    fastramqpi.get_app().include_router(trigger_router)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    return fastramqpi


def create_app(**kwargs) -> FastAPI:
    fastramqpi = create_fastramqpi(**kwargs)
    return fastramqpi.get_app()
