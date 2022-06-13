from fastapi import FastAPI
from os2sync_export.__main__ import main
from os2sync_export.config import get_os2sync_settings

app = FastAPI()


@app.get("/")
async def index() -> dict[str, str]:
    return {"name": "os2sync_export"}


@app.post("/trigger")
async def trigger() -> dict[str, str]:
    settings = get_os2sync_settings()
    main(settings=settings)
    return {"triggered": "OK"}
