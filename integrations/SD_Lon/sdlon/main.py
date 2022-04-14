import asyncio
from functools import partial

from fastapi import FastAPI

from .sd_changed_at import changed_at


app = FastAPI()


@app.get("/")
async def index() -> dict[str, str]:
    return {"name": "sdlon"}


@app.get("/trigger")
async def trigger(force: bool = False) -> dict[str, str]:
    loop = asyncio.get_running_loop()
    loop.call_soon(partial(changed_at, False, force))
    return {"triggered": "OK"}
