from contextlib import asynccontextmanager
from typing import AsyncGenerator
from typing import Dict
from typing import List
from uuid import UUID

from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastapi import Query
from gql.client import AsyncClientSession
from starlette.requests import Request

from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings
from os2sync_export.config import setup_gql_client
from os2sync_export.os2synccli import update_single_orgunit
from os2sync_export.os2synccli import update_single_user

app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    settings = get_os2sync_settings()
    async with setup_gql_client(settings) as gql_session:
        app.state.gql_session = gql_session

        # Yield to keep the GraphQL client open until the ASGI application is closed.
        # Control will be returned to here when the ASGI application is shut down.
        yield


# Ideally, FastAPI would support the `lifespan=` keyword-argument like Starlette,
# but that is not supported: https://github.com/tiangolo/fastapi/issues/2943.
app.router.lifespan_context = lifespan


@app.get("/")
async def index() -> Dict[str, str]:
    return {"name": "os2sync_export"}


@app.post("/trigger", status_code=202)
async def trigger_all(
    request: Request, background_tasks: BackgroundTasks
) -> Dict[str, str]:
    settings = get_os2sync_settings()
    gql_session: AsyncClientSession = request.app.state.gql_session
    background_tasks.add_task(main, settings=settings, gql_session=gql_session)
    return {"triggered": "OK"}


@app.post("/trigger/user/{uuid}")
async def trigger_user(
    request: Request,
    uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    settings = get_os2sync_settings()
    gql_session: AsyncClientSession = request.app.state.gql_session
    return await update_single_user(uuid, settings, gql_session, dry_run)


@app.post("/trigger/orgunit/{uuid}")
async def trigger_orgunit(
    uuid: UUID = Query(..., description="UUID of the organisation unit to recalculate"),
    dry_run: bool = False,
) -> Dict[str, str]:
    settings = get_os2sync_settings()
    return update_single_orgunit(uuid, settings, dry_run)
