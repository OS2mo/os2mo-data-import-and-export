import logging
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastapi import Query
from os2sync_export import os2mo
from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings
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
    uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
    dry_run: bool = False,
) -> List[Optional[Dict[str, str]]]:
    clear_caches()
    return update_single_user(uuid, settings, dry_run)


@app.post("/trigger/orgunit/{uuid}")
async def trigger_orgunit(
    uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
    dry_run: bool = False,
) -> Optional[Dict[str, str]]:
    clear_caches()
    return update_single_orgunit(uuid, settings, dry_run)
