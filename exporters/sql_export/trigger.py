# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration endpoints."""
import logging
from threading import Lock

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import HTTPException
from fastapi import Query
from fastramqpi.metrics import dipex_last_success_timestamp

from .config import DatabaseSettings
from .sql_export import SqlExport

logger = logging.getLogger(__name__)
trigger_router = APIRouter()
# Concurrency lock to ensure that only 1 operation is running at a time
lock_actual = Lock()
lock_historic = Lock()


def refresh_db(
    resolve_dar: bool, historic: bool, read_from_cache: bool, lock: Lock
) -> None:
    try:
        logger.info("*SQL export started*")
        database_settings = DatabaseSettings()

        sql_export = SqlExport(
            force_sqlite=False,
            historic=historic,
            settings=database_settings.to_old_settings(),
        )
        sql_export.perform_export(
            resolve_dar=resolve_dar,
            use_pickle=read_from_cache,
        )

        sql_export.swap_tables()
        logger.info("*SQL export ended*")
        dipex_last_success_timestamp.set_to_current_time()
    finally:
        # Another operation can start now
        lock.release()


@trigger_router.post("/trigger")
def trigger(
    background_tasks: BackgroundTasks,
    resolve_dar: bool = Query(False),
    historic: bool = Query(False),
    read_from_cache: bool = Query(False),
) -> dict[str, str]:
    if historic:
        lock = lock_historic
    else:
        lock = lock_actual
    acquired = lock.acquire(blocking=False)
    if not acquired:
        settings = DatabaseSettings()
        if settings.log_overlapping_aak:
            sql_export = SqlExport(
                force_sqlite=False,
                historic=historic,
                settings=settings.to_old_settings(),
            )
            sql_export.log_overlapping_runs_aak()
        raise HTTPException(409, "Already running")

    background_tasks.add_task(refresh_db, resolve_dar, historic, read_from_cache, lock)
    return {"detail": "Triggered"}
