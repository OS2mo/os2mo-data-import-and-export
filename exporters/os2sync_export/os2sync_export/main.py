from uuid import UUID

from fastapi import FastAPI
from fastapi import Query
from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings
from os2sync_export.os2synccli import update_single_orgunit
from os2sync_export.os2synccli import update_single_user

app = FastAPI()


@app.get("/")
async def index() -> dict[str, str]:
    return {"name": "os2sync_export"}


@app.post("/trigger")
async def trigger_all() -> dict[str, str]:
    settings = get_os2sync_settings()
    main(settings=settings)
    return {"triggered": "OK"}


@app.post("/trigger/user/{uuid}")
async def trigger_user(
    uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
    dry_run: bool = False,
) -> dict[str, str]:
    settings = get_os2sync_settings()

    return update_single_user(uuid, settings, dry_run)


@app.post("/trigger/orgunit/{uuid}")
async def trigger_orgunit(
    uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
    dry_run: bool = False,
) -> dict[str, str]:
    settings = get_os2sync_settings()

    return update_single_orgunit(uuid, settings, dry_run)
