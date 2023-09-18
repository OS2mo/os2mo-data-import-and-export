import asyncio
import enum
from datetime import datetime
from typing import Iterable
from contextlib import contextmanager
from functools import partial
from uuid import UUID

import structlog
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Enum
from prometheus_client import Gauge
from integrations.rundb.db_overview import DBOverview
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from .config import get_changed_at_settings
from .fix_departments import FixDepartments
from .sd_changed_at import changed_at


logger = structlog.get_logger(__name__)


class State(enum.Enum):
    RUNNING = "running"
    OK = "ok"
    FAILURE = "failure"
    UNKNOWN = "unknown"


def get_state() -> State:
    try:
        settings = get_changed_at_settings()
        run_db = settings.sd_import_run_db
        db_overview = DBOverview(run_db)

        status_line = db_overview._read_last_line("status")

        if "Running since" in status_line:
            return State.RUNNING
        if "Update finished" in status_line:
            return State.OK

        return State.UNKNOWN
    except:
        return State.FAILURE


state = Enum(
    "sd_changed_at_state",
    "Current state of the SD Changed At integration",
    states=[s.value for s in State],
)
state.state(get_state().value)
start_time = Gauge(
    "sd_changed_at_start_time", "Start time of the latest SD Changed At run"
)
end_time = Gauge("sd_changed_at_end_time", "End time of the latest SD Changed At run")


@contextmanager
def update_state_metric() -> Iterable[None]:
    """Update SDChangedAt state metrics."""
    start_time.set_to_current_time()

    # TODO: write a test of this contextmanager
    # TODO: refactor the contextmanager to use the get_state function above
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


def create_app(**kwargs) -> FastAPI:
    settings = get_changed_at_settings(**kwargs)
    settings.job_settings.start_logging_based_on_settings()

    app = FastAPI(fix_departments=FixDepartments(settings))
    Instrumentator().instrument(app).expose(app)

    @app.get("/")
    async def index() -> dict[str, str]:
        return {"name": "sdlon"}

    @app.post("/trigger")
    @app.get("/trigger", deprecated=True)
    async def trigger(force: bool = False) -> dict[str, str]:
        loop = asyncio.get_running_loop()
        loop.call_soon(
            partial(update_state_metric()(changed_at), init=False, force=force)
        )
        return {"triggered": "OK"}

    @app.post("/trigger/fix-departments/{ou}")
    async def fix_departments(
        ou: UUID, request: Request, response: Response
    ) -> dict[str, str]:
        logger.info("Triggered fix_department", ou=str(ou))

        today = datetime.today().date()
        fix_departments = request.app.extra["fix_departments"]

        try:
            fix_departments.fix_department(str(ou), today)
            fix_departments.fix_NY_logic(str(ou), today)
            return {"msg": "success"}
        except Exception as err:
            logger.exception("Error calling fix_department or fix_NY_logic", err=err)
            response.status_code = HTTP_500_INTERNAL_SERVER_ERROR
            return {"msg": str(err)}

    return app
