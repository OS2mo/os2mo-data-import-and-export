from fastapi import FastAPI
from fastapi import HTTPException
from fastramqpi.ra_utils.load_settings import load_settings

from integrations.ad_integration import ad_reader
from integrations.opus.opus_diff_import import start_opus_diff
from integrations.opus.opus_exceptions import ImporterrunNotCompleted
from integrations.opus.opus_exceptions import RunDBInitException

app = FastAPI()


@app.get("/")
async def name() -> dict:
    return {"name": "Opus"}


@app.post("/trigger")
async def trigger():
    dict_settings = load_settings()

    reader = (
        ad_reader.ADParameterReader() if dict_settings.get("integrations.ad") else None
    )
    try:
        start_opus_diff(ad_reader=reader)
    except RunDBInitException:
        raise HTTPException(status_code=500, detail="Rundb not initialized")
    except ImporterrunNotCompleted:
        raise HTTPException(status_code=500, detail="Last run not completed")
