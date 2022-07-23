#!/usr/bin/env python3
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import logging
from enum import Enum
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import Optional

from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import SecretStr
from ra_utils.load_settings import load_settings


logger = logging.getLogger("SqlExport")


class DatabaseFunction(Enum):
    ACTUAL_STATE = 1
    ACTUAL_STATE_HISTORIC = 2


class ConfigurationError(Exception):
    """Throw whenever a configuration issue in settings.json is detected."""


def gen_json_file_settings_func(settings_class: Any):
    def json_file_settings(settings: BaseSettings) -> Dict[str, Any]:
        try:
            json_settings = load_settings()
        except FileNotFoundError:
            return dict()

        # Remove the "integrations.SD_Lon." part of the key name
        json_settings = {
            key.replace("exporters.actual_state.", "sql_export_"): value
            for key, value in json_settings.items()
        }

        # Remove any double "sd_sd_" in the keys
        json_settings = {
            key.replace("sd_sd_", "sd_"): value for key, value in json_settings.items()
        }

        # Replace dots with underscores to be Pydantic compliant
        json_settings = {
            key.replace(".", "_"): value for key, value in json_settings.items()
        }

        # Remove settings forbidden according to the Settings model
        properties = settings_class.schema()["properties"].keys()
        json_settings = {
            key: value for key, value in json_settings.items() if key in properties
        }

        return json_settings

    return json_file_settings


class Settings(BaseSettings):
    class Config:
        extra = Extra.forbid

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                gen_json_file_settings_func(Settings),
                file_secret_settings,
            )

    mora_base: AnyHttpUrl = Field("http://mo-service:5000")
    mox_base: AnyHttpUrl = Field("http://mox-service:8080")

    sql_export_export_cpr: bool = True
    sql_export_manager_responsibility_class: Optional[str] = None

    sql_export_type: str = "Postgres"
    sql_export_host: str = "localhost"
    sql_export_db_name: str = "actualstate"
    sql_export_user: str = "postgres"
    sql_export_password: SecretStr = ""

    sql_export_historic_type: Optional[str]
    sql_export_historic_host: Optional[str]
    sql_export_historic_db_name: Optional[str]
    sql_export_historic_user: Optional[str]
    sql_export_historic_password: Optional[SecretStr]

    def get_db_setting(self, key: str, database_function: DatabaseFunction) -> Any:
        prefix = "sql_export_"
        if database_function == DatabaseFunction.ACTUAL_STATE_HISTORIC:
            prefix = "sql_export_historic_"
        return getattr(self, prefix + key)

    def get_db_type(
        self, database_function: DatabaseFunction, force_sqlite: bool
    ) -> str:
        if force_sqlite:
            return "SQLite"

        value = self.get_db_setting("type", database_function)
        if not value:
            raise ConfigurationError("Missing value in db_type")
        return value

    def get_db_name(self, database_function: DatabaseFunction) -> str:
        value = self.get_db_setting("db_name", database_function)
        if not value:
            raise ConfigurationError("Missing value in db_name")
        return value

    def get_db_host(self, database_function: DatabaseFunction) -> str:
        return self.get_db_setting("host", database_function)

    def get_db_username(self, database_function: DatabaseFunction) -> str:
        return self.get_db_setting("user", database_function)

    def get_db_password(self, database_function: DatabaseFunction) -> str:
        return self.get_db_setting("password", database_function).get_secret_value()

    @root_validator
    def historic_uses_non_historic_as_default(cls, values):
        mapping = {"type", "host", "user", "password"}
        for key in mapping:
            setting = f"sql_export_historic_{key}"
            fallback_setting = f"sql_export_{key}"

            if values.get(setting) is None:
                logger.warning(f"Utilizing {fallback_setting} in place of {setting}!")
                values[setting] = values.get(fallback_setting)
        return values


@lru_cache()
def get_settings(**kwargs) -> Settings:
    return Settings(**kwargs)
