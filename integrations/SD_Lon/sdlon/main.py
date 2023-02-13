import asyncio
from typing import Iterable
from contextlib import contextmanager
from functools import partial

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Enum
from prometheus_client import Gauge
from integrations.rundb.db_overview import DBOverview

from .config import get_changed_at_settings
from .sd_changed_at import changed_at

state = Enum(
    "sd_changed_at_state",
    "Current state of the SD Changed At integration",
    states=["running", "ok", "failure", "unknown"],
)
state.state("unknown")
start_time = Gauge(
    "sd_changed_at_start_time", "Start time of the latest SD Changed At run"
)
end_time = Gauge("sd_changed_at_end_time", "End time of the latest SD Changed At run")


app = FastAPI()
Instrumentator().instrument(app).expose(app)


@contextmanager
def update_state_metric() -> Iterable[None]:
    """Update SDChangedAt state metrics."""
    start_time.set_to_current_time()

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
    finally:
        end_time.set_to_current_time()


@app.get("/")
async def index() -> dict[str, str]:
    return {"name": "sdlon"}


@app.post("/trigger")
@app.get("/trigger", deprecated=True)
async def trigger(force: bool = False) -> dict[str, str]:
    loop = asyncio.get_running_loop()
    loop.call_soon(partial(update_state_metric()(changed_at), init=False, force=force))
    return {"triggered": "OK"}
