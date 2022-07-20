import asyncio
from contextlib import contextmanager
from functools import partial

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Enum
from integrations.rundb.db_overview import DBOverview

from .config import get_changed_at_settings
from .sd_changed_at import changed_at
from .sd_changed_at import import_state
from .sd_importer import full_import
from typing import Optional

state = Enum(
    "sd_changed_at_state",
    "Current state of the SD Changed At integration",
    states=["running", "ok", "failure", "unknown"],
)
state.state("unknown")

app = FastAPI()
Instrumentator().instrument(app).expose(app)


@contextmanager
def update_state_metric() -> None:
    """Update SDChangedAt state metrics."""
    settings = get_changed_at_settings()
    run_db = settings.sd_import_run_db
    db_overview = DBOverview(run_db)
    try:
        status = db_overview._read_last_line("status")
        state.state("running")
        yield
        if "Running since" in status:
            state.state("failure")
        elif "Update finished" in status:
            state.state("ok")
        else:
            state.state("unknown")
    except:
        state.state("failure")
        raise


@app.get("/")
async def index() -> dict[str, str]:
    return {"name": "sdlon"}


def run_initial_import() -> None:
    full_import()
    import_state()


@app.post("/initial")
async def initial_import() -> dict[str, str]:
    loop = asyncio.get_running_loop()
    loop.call_soon(run_initial_import)
    return {"triggered": "OK"}


@app.post("/trigger")
@app.get("/trigger", deprecated=True)
async def trigger(force: bool = False) -> dict[str, str]:
    loop = asyncio.get_running_loop()
    loop.call_soon(partial(update_state_metric()(changed_at), init=False, force=force))
    return {"triggered": "OK"}
