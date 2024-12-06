# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration endpoints."""
import logging
from threading import Lock

import xlsxwriter
from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import HTTPException
from fastapi import Query
from fastramqpi.metrics import dipex_last_success_timestamp
from more_itertools import first
from more_itertools import flatten
from more_itertools import prepend
from raclients.upload import file_uploader

from .config import DatabaseSettings
from .depends import SqlExport
from .sql_export import SqlExport as SqlExport_  # type: ignore
from .xlsx_exporter import XLSXExporter

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

        sql_export = SqlExport_(
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


# TODO: run as background task
@trigger_router.post("/trigger-excel")
async def trigger_excel(
    sql_export: SqlExport,
    background_tasks: BackgroundTasks,
    resolve_dar: bool = Query(False),
    historic: bool = Query(False),
    read_from_cache: bool = Query(False),
) -> dict[str, str]:

    logger.info("Populating cache")
    await sql_export.lc.populate_cache_async(dry_run=read_from_cache)
    logger.info("Done caching")
    with file_uploader(sql_export.lc.settings, "Actual_state.xlsx") as report_file:
        # write data as excel file
        workbook = xlsxwriter.Workbook(report_file)
        excel = XLSXExporter(report_file)
        for sheet_name, model in (
            ("Facetter", sql_export.lc.facets),
            ("Klasser", sql_export.lc.classes),
            # ("IT systemer", sql_export.lc.itsystems)
            ("Enheder", sql_export.lc.units),
            ("Brugere", sql_export.lc.users),
            # ("Engagementer", sql_export.lc.engagements),
            # ("Ledere", sql_export.lc.managers),
            # ("Orlov", sql_export.lc.leaves ),
            ("Addresser", sql_export.lc.addresses),
            ("DAR", sql_export.lc.dar_cache),
            ("IT konti", sql_export.lc.it_connections),
            # ("KLE opmærkninger", sql_export.lc.kles),
            # ("Relaterede Enheder", sql_export.lc.related),
        ):
            logger.info(sheet_name)
            if sheet_name in (
                "Facetter",
                "Klasser",
                "IT systemer",
                "DAR",
            ):
                cache_data = list(model.values())
            else:
                # Everything is nested in lists
                cache_data = list(flatten(model.values()))
            # Find the keys of the dictionaries to use as titles
            keys = list(first(cache_data).keys())

            data = [list(row.values()) for row in cache_data]
            data_with_titles = list(prepend(keys, data))
            excel.add_sheet(workbook, sheet_name, data_with_titles)
        workbook.close()

    return {"status": "ok"}


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
