# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Integration endpoints."""
import logging
from threading import Lock
from typing import Any

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import HTTPException
from fastapi import Query
from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import Field
from pydantic import SecretStr

from .sql_export import SqlExport

logger = logging.getLogger(__name__)
trigger_router = APIRouter()
# Concurrency lock to ensure that only 1 operation is running at a time
lock = Lock()


class DatabaseConfiguration(BaseSettings):
    class Config:
        frozen = True

    type: str | None
    host: str | None
    db_name: str
    user: str | None
    password: SecretStr | None


class DatabaseSettings(BaseSettings):
    class Config:
        frozen = True
        env_nested_delimiter = "__"

    mora_base: AnyHttpUrl = Field("http://mo-service:5000")
    mox_base: AnyHttpUrl = Field("http://mo-service:5000/lora")

    actual_state: DatabaseConfiguration = Field(default_factory=DatabaseConfiguration)
    historic_state: DatabaseConfiguration | None

    def to_old_settings(self) -> dict[str, Any]:
        """Convert our DatabaseSettings to a settings.json format.

        This serves to implement the adapter pattern, adapting from pydantic and its
        corresponding 12-factor configuration paradigm with environmental variables, to
        the current functionality of the program, based on the settings format from
        settings.json.

        Eventually the entire settings-processing within the program should be
        rewritten with a process similar to what has been done for the SD integration,
        but it was out of scope for the change when this code was introduces.
        """

        def secret_val_or_none(value: SecretStr | None) -> str | None:
            if value is None:
                return None
            return value.get_secret_value()

        settings = {
            "mora.base": self.mora_base,
            "mox.base": self.mox_base,
            "exporters.actual_state.type": self.actual_state.type,
            "exporters.actual_state.host": self.actual_state.host,
            "exporters.actual_state.db_name": self.actual_state.db_name,
            "exporters.actual_state.user": self.actual_state.user,
            "exporters.actual_state.password": secret_val_or_none(
                self.actual_state.password
            ),
        }
        if self.historic_state is not None:
            settings.update(
                {
                    "exporters.actual_state_historic.type": self.historic_state.type
                    or self.actual_state.type,
                    "exporters.actual_state_historic.host": self.historic_state.host
                    or self.actual_state.host,
                    "exporters.actual_state_historic.db_name": self.historic_state.db_name
                    or self.actual_state.db_name,
                    "exporters.actual_state_historic.user": self.historic_state.user
                    or self.actual_state.user,
                    "exporters.actual_state_historic.password": secret_val_or_none(
                        self.historic_state.password
                    )
                    or secret_val_or_none(self.actual_state.password),
                }
            )
        return settings


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
    acquired = lock.acquire(blocking=False)
    if not acquired:
        raise HTTPException(409, "Already running")

    background_tasks.add_task(refresh_db, resolve_dar, historic, read_from_cache, lock)
    return {"detail": "Triggered"}
