import logging

from fastapi import FastAPI
from fastapi import Query
from prometheus_fastapi_instrumentator import Instrumentator

from .config import get_settings
from .sql_export import SqlExport

app = FastAPI()
Instrumentator().instrument(app).expose(app)

logger = logging.getLogger("SqlExport")


@app.get("/")
async def index() -> dict[str, str]:
    return {"name": "sql_export"}


@app.post("/trigger")
def trigger(
    resolve_dar: bool = Query(False),
    historic: bool = Query(False),
    read_from_cache: bool = Query(False),
) -> dict[str, str]:
    settings = get_settings()

    sql_export = SqlExport(
        force_sqlite=False,
        historic=historic,
        settings=settings,
    )
    sql_export.perform_export(
        resolve_dar=resolve_dar,
        use_pickle=read_from_cache,
    )
    logger.info("*Running sqp tables*")
    sql_export.swap_tables()
    logger.info("*SQL export ended*")

    return {"triggered": "OK"}
