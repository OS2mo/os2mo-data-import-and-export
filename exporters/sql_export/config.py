# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from functools import lru_cache
from typing import Any

from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import Field
from pydantic import SecretStr
from ra_utils.job_settings import JobSettings


class GqlLoraCacheSettings(JobSettings):  # type: ignore
    class Config:
        frozen = True

    use_new_cache: bool = False
    primary_manager_responsibility: str | None = None
    prometheus_pushgateway: str = "pushgateway"
    mox_base: str = "http://mo:5000/lora"
    std_page_size: int = 300

    def to_old_settings(self) -> dict[str, Any]:
        """Convert our DatabaseSettings to a settings.json format.

        This serves to implement the adapter pattern, adapting from pydantic and its
        corresponding 12-factor configuration paradigm with environment variables, to
        the current functionality of the program, based on the settings format from
        settings.json.

        Eventually the entire settings-processing within the program should be
        rewritten with a process similar to what has been done for the SD integration,
        but it was out of scope for the change when this code was introduced.
        """

        settings = {
            "mora.base": self.mora_base,
            "mox.base": self.mox_base,
            "exporters": {
                "actual_state": {
                    "manager_responsibility_class": self.primary_manager_responsibility
                }
            },
            "exporters.actual_state.manager_responsibility_class": self.primary_manager_responsibility,
            "use_new_cache": self.use_new_cache,
        }

        return settings


@lru_cache()
def get_gql_cache_settings(*args, **kwargs) -> GqlLoraCacheSettings:
    return GqlLoraCacheSettings(*args, **kwargs)


class DatabaseConfiguration(BaseSettings):  # type: ignore
    class Config:
        frozen = True

    type: str | None
    host: str | None
    db_name: str
    user: str | None
    password: SecretStr | None


class DatabaseSettings(JobSettings):
    class Config:
        frozen = True
        env_nested_delimiter = "__"

    mora_base: AnyHttpUrl = Field("http://mo-service:5000")
    mox_base: AnyHttpUrl = Field("http://mo-service:5000/lora")

    actual_state: DatabaseConfiguration = Field(default_factory=DatabaseConfiguration)
    historic_state: DatabaseConfiguration | None
    log_overlapping_aak: bool = False
    use_new_cache: bool = False
    primary_manager_responsibility: str | None = None

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
            "primary_manager_responsibility": self.primary_manager_responsibility,
            "exporters.actual_state.manager_responsibility_class": self.primary_manager_responsibility,
            "use_new_cache": self.use_new_cache,
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
                    "primary_manager_responsibility": self.primary_manager_responsibility,
                    "use_new_cache": self.use_new_cache,
                }
            )
        return settings
