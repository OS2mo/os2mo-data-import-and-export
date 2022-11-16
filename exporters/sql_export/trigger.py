# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration endpoints."""
import logging

from fastapi import APIRouter
from fastapi import Query
from ra_utils.load_settings import load_settings

from .sql_export import SqlExport
from .sql_export import SqlExportSettings


logger = logging.getLogger(__name__)
trigger_router = APIRouter()


@trigger_router.post("/trigger")
def trigger(
    resolve_dar: bool = Query(False),
    historic: bool = Query(False),
    read_from_cache: bool = Query(False),
) -> dict[str, str]:
    pydantic_settings = SqlExportSettings()
    pydantic_settings.start_logging_based_on_settings()

    settings = load_settings()

    sql_export = SqlExport(
        force_sqlite=False,
        historic=historic,
        settings=settings,
    )

    sql_export.perform_export(
        resolve_dar=resolve_dar,
        use_pickle=read_from_cache,
    )

    sql_export.swap_tables()

    logger.info("*SQL export ended*")

    return {"status": "OK"}
