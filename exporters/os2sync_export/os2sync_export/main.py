import logging
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
from os2sync_export import os2sync
from os2sync_export import os2synccli
from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings
from os2sync_export.config import Settings
from os2sync_export.os2mo import get_sts_orgunit
from os2sync_export.os2mo import get_sts_user
from os2sync_export.os2sync_models import OrgUnit
from ramqp.depends import Context
from ramqp.depends import SleepOnError
from ramqp.mo import MORouter  # type: ignore
from ramqp.mo import PayloadUUID


# from ramqp.utils import SleepOnError

logger = logging.getLogger(__name__)

fastapi_router = APIRouter()
amqp_router = MORouter()


async def update_single_user(uuid: UUID, gql_session, settings: Settings) -> None:

    sts_users = await get_sts_user(
        str(uuid), gql_session=gql_session, settings=settings
    )

    for sts_user in sts_users:
        if sts_user:
            os2sync.os2sync_post("{BASE}/user", json=sts_user)


async def update_single_orgunit(uuid: UUID, settings: Settings) -> None:

    sts_org_unit = get_sts_orgunit(str(uuid), settings=settings)

    if sts_org_unit:
        os2sync.os2sync_post("{BASE}/OrgUnit", json=sts_org_unit)
    else:
        os2sync.delete_orgunit(uuid)


@fastapi_router.get("/")
async def index() -> Dict[str, str]:
    return {"name": "os2sync_export"}


@fastapi_router.post("/trigger", status_code=202)
async def trigger_all(
    request: Request, background_tasks: BackgroundTasks
) -> Dict[str, str]:
    background_tasks.add_task(main, settings=get_os2sync_settings())
    return {"triggered": "OK"}


@amqp_router.register("engagement")
async def amqp_trigger_eng(context: Context, uuid: PayloadUUID) -> None:
    logger.warn(f"engagement {uuid=}")


@amqp_router.register("person")
async def amqp_trigger_employee(
    context: Context, uuid: PayloadUUID, _: SleepOnError
) -> None:

    user = await update_single_user(
        uuid,
        gql_session=context["graphql_session"],
        settings=context["user_context"]["settings"],
    )
    logger.info("Synced user to fk-org", user)
    return user


@fastapi_router.post("/trigger/user/{uuid}")
async def trigger_user(
    context: dict,
    uuid: UUID,
    dry_run: bool = False,
) -> List[Optional[Dict]]:

    return await os2synccli.update_single_user(
        uuid, settings=get_os2sync_settings(), dry_run=dry_run
    )


@fastapi_router.post("/trigger/orgunit/{uuid}", status_code=200)
async def trigger_orgunit(
    context: dict,
    uuid: UUID,
    dry_run: bool,
    response: Response,
) -> Optional[OrgUnit]:

    org_unit, changes = os2synccli.update_single_orgunit(
        uuid, settings=get_os2sync_settings(), dry_run=dry_run
    )
    if changes:
        response.status_code = status.HTTP_201_CREATED
    if not org_unit:
        response.status_code = status.HTTP_404_NOT_FOUND
    return org_unit


def create_fastramqpi(**kwargs) -> FastRAMQPI:
    settings = get_os2sync_settings(**kwargs)
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
