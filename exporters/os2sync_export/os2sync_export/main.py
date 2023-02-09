import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastapi import Query
from fastapi import Request
from fastramqpi.context import Context
from fastramqpi.main import FastRAMQPI
from os2sync_export import os2mo
from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings
from os2sync_export.os2synccli import update_single_orgunit
from os2sync_export.os2synccli import update_single_user
from ramqp.mo import MORouter
from ramqp.mo.models import ObjectType
from ramqp.mo.models import PayloadType
from ramqp.mo.models import RequestType
from ramqp.mo.models import ServiceType

logger = logging.getLogger(__name__)


fastapi_router = APIRouter()


@fastapi_router.post("/trigger/all")
async def update_all(request: Request) -> dict[str, str]:
    context: dict[str, Any] = request.app.state.context
    graphql_session = context["grapqh_session"]
    program_settings = context["user_context"]["settings"]
    ...
    return {"status": "OK"}


amqp_router = MORouter()


@amqp_router.register("*.*.*")
async def listen_to_all(context: dict, payload: PayloadType) -> None:
    graphql_session = context["grapqh_session"]
    program_settings = context["user_context"]["settings"]
    print("HEST!")


def create_fastramqpi(**kwargs: Any) -> FastRAMQPI:
    settings = get_os2sync_settings()
    settings.start_logging_based_on_settings()
    fastramqpi = FastRAMQPI(
        application_name="os2sync_export", settings=settings.fastramqpi
    )
    fastramqpi.add_context(settings=settings)

    # Add our AMQP router(s)
    amqpsystem = fastramqpi.get_amqpsystem()
    amqpsystem.router.registry.update(amqp_router.registry)

    # Add our FastAPI router(s)
    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    return fastramqpi


def create_app(**kwargs: Any) -> FastAPI:
    fastramqpi = create_fastramqpi(**kwargs)
    return fastramqpi.get_app()


# TODO: wrap in a function read on fastapi startup


# def clear_caches():
#     """Clear all lru_caches."""
#     # TODO: rewrite as a cache context manager
#     os2mo.os2mo_get.cache_clear()
#     os2mo.get_org_unit_hierarchy.cache_clear()
#     os2mo.org_unit_uuids.cache_clear()
#     os2mo.organization_uuid.cache_clear()


# @app.get("/")
# async def index() -> Dict[str, str]:
#     return {"name": "os2sync_export"}


# @app.post("/trigger", status_code=202)
# async def trigger_all(background_tasks: BackgroundTasks) -> Dict[str, str]:
#     clear_caches()
#     background_tasks.add_task(main, settings=settings)
#     return {"triggered": "OK"}


# @app.post("/trigger/user/{uuid}")
# async def trigger_user(
#     uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
#     dry_run: bool = False,
# ) -> List[Optional[Dict[str, str]]]:
#     clear_caches()
#     return update_single_user(uuid, settings, dry_run)


# @app.post("/trigger/orgunit/{uuid}")
# async def trigger_orgunit(
#     uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
#     dry_run: bool = False,
# ) -> Optional[Dict[str, str]]:
#     clear_caches()
#     return update_single_orgunit(uuid, settings, dry_run)
