import logging
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastapi import Response
from fastapi import status
from os2sync_export import os2mo
from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings
from os2sync_export.os2sync_models import OrgUnit
from os2sync_export.os2synccli import update_single_orgunit
from os2sync_export.os2synccli import update_single_user

logger = logging.getLogger(__name__)

app = FastAPI()
# TODO: wrap in a function read on fastapi startup
settings = get_os2sync_settings()
settings.start_logging_based_on_settings()


def clear_caches():
    """Clear all lru_caches."""
    # TODO: rewrite as a cache context manager
    os2mo.os2mo_get.cache_clear()
    os2mo.get_org_unit_hierarchy.cache_clear()
    os2mo.org_unit_uuids.cache_clear()
    os2mo.organization_uuid.cache_clear()


@app.get("/")
async def index() -> Dict[str, str]:
    return {"name": "os2sync_export"}


@app.post("/trigger", status_code=202)
async def trigger_all(background_tasks: BackgroundTasks) -> Dict[str, str]:
    clear_caches()
    background_tasks.add_task(main, settings=settings)
    return {"triggered": "OK"}


@app.post("/trigger/user/{uuid}")
async def trigger_user(
    uuid: UUID,
    dry_run: bool = False,
) -> List[Optional[Dict]]:
    clear_caches()
    return update_single_user(uuid, settings, dry_run)


@app.post("/trigger/orgunit/{uuid}", status_code=200)
async def trigger_orgunit(
    uuid: UUID,
    dry_run: bool,
    response: Response,
) -> Optional[OrgUnit]:
    clear_caches()
    org_unit, changes = update_single_orgunit(uuid, settings, dry_run)
    if changes:
        response.status_code = status.HTTP_201_CREATED
    if not org_unit:
        response.status_code = status.HTTP_404_NOT_FOUND
    return org_unit
