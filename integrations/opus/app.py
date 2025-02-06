from pathlib import Path

from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import HTTPException
from fastramqpi.ra_utils.load_settings import load_settings

from integrations.ad_integration import ad_reader
from integrations.opus import opus_helpers
from integrations.opus.opus_diff_import import start_opus_diff
from integrations.opus.opus_exceptions import ImporterrunNotCompleted
from integrations.opus.opus_exceptions import RunDBInitException

router = APIRouter()


def create_app() -> FastAPI:
    app = FastAPI()

    app.include_router(router)
    settings = load_settings()
    run_db = Path(settings["integrations.opus.import.run_db"])
    if not run_db.is_file():
        opus_helpers.initialize_db(settings["integrations.opus.import.run_db"])
    return app


@router.get("/")
async def name() -> dict:
    return {"name": "Opus"}


@router.post("/trigger")
async def trigger(force: bool = False, full_sync: bool = False) -> int:
    settings = load_settings()
    reader = ad_reader.ADParameterReader() if settings.get("integrations.ad") else None
    try:
        start_opus_diff(reader, force, full_sync)
    except RunDBInitException:
        raise HTTPException(status_code=500, detail="Rundb not initialized")
    except ImporterrunNotCompleted:
        raise HTTPException(status_code=500, detail="Last run not completed")
    return 0
