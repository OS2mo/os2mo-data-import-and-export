import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi import status
from fastramqpi.main import FastRAMQPI  # type: ignore
from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings
from os2sync_export.os2sync_models import OrgUnit
from os2sync_export.os2synccli import update_single_orgunit
from os2sync_export.os2synccli import update_single_user
from ramqp.depends import Context
from ramqp.depends import SleepOnError
from ramqp.mo import MORouter  # type: ignore
from ramqp.mo import PayloadUUID


# from ramqp.utils import SleepOnError

logger = logging.getLogger(__name__)

fastapi_router = APIRouter()
amqp_router = MORouter()
# # TODO: wrap in a function read on fastapi startup
# settings = get_os2sync_settings()
# settings.start_logging_based_on_settings()




@fastapi_router.get("/")
async def index() -> Dict[str, str]:
    return {"name": "os2sync_export"}


@fastapi_router.post("/trigger", status_code=202)
async def trigger_all(
    request: Request, background_tasks: BackgroundTasks
) -> Dict[str, str]:
    logger.warn(request.__dict__)
    context: dict[str, Any] = request.app.state.context

    background_tasks.add_task(main, settings=context["settings"])
    return {"triggered": "OK"}


@amqp_router.register("engagement")
async def amqp_trigger_eng(context: Context, uuid: PayloadUUID, **kwargs: Any) -> None:
    logger.warn(f"engagement {uuid=}")


@amqp_router.register("person")
async def amqp_trigger_employee(
    context:Context, uuid: PayloadUUID, _: SleepOnError

) -> None:

    user = update_single_user(uuid, settings=context["settings"], dry_run=False)
    logger.info("Synced user to fk-org", user)
    return user


@fastapi_router.post("/trigger/user/{uuid}")
async def trigger_user(
    context: dict,
    uuid: UUID,
    dry_run: bool = False,
) -> List[Optional[Dict]]:

    return update_single_user(uuid, settings=context["settings"], dry_run=dry_run)

# @amqp_router.register("org_unit.*.*")
# async def amqp_trigger_org_unit(
#     context: dict, payload: PayloadType, _: SleepOnError, **kwargs: Any
# ) -> None:
#     logger.info(f"Changes triggered sync of org_unit {payload.uuid}")
#     org_unit, changes = update_single_orgunit(
#         payload.uuid, settings=context["settings"], dry_run=False
#     )
#     status_msg = "Synced to FK-org" if changes else "No changes found - not synced."
#     logger.debug(status_msg + f"{org_unit=}")


@fastapi_router.post("/trigger/orgunit/{uuid}", status_code=200)
async def trigger_orgunit(
    context: dict,
    uuid: UUID,
    dry_run: bool,
    response: Response,
) -> Optional[OrgUnit]:

    org_unit, changes = update_single_orgunit(
        uuid, settings=context["settings"], dry_run=dry_run
    )
    if changes:
        response.status_code = status.HTTP_201_CREATED
    if not org_unit:
        response.status_code = status.HTTP_404_NOT_FOUND
    return org_unit


def create_fastramqpi(**kwargs) -> FastRAMQPI:
    settings = get_os2sync_settings()
    settings.start_logging_based_on_settings()

    fastramqpi = FastRAMQPI(
        application_name="os2sync-export", settings=settings.fastramqpi
    )

    amqpsystem = fastramqpi.get_amqpsystem()
    amqpsystem.router.registry.update(amqp_router.registry)
    fastramqpi.add_context(settings=settings)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    return fastramqpi


def create_app(**kwargs) -> FastAPI:
    fastramqpi = create_fastramqpi(**kwargs)
    return fastramqpi.get_app()
