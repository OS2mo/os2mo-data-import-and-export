#!/usr/bin/env python3
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
from datetime import date
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import conint
from pydantic import constr
from pydantic import Extra
from pydantic import Field
from pydantic import PositiveInt
from pydantic import SecretStr
from pydantic import UUID4
from pydantic.types import Path
from ra_utils.load_settings import load_settings

from .models import JobFunction


class CommonSettings(BaseSettings):
    """
    Settings common to both the SD importer and SD-changed-at
    """

    mora_base: AnyHttpUrl = Field("http://mo-service:5000")
    mox_base: AnyHttpUrl = Field("http://mo-service:5000/lora")

    sd_employment_field: Optional[str] = Field(default=None, regex="extension_[0-9]+")
    sd_global_from_date: date
    sd_import_run_db: str
    sd_import_too_deep: List[str] = []
    sd_institution_identifier: str
    sd_password: SecretStr
    sd_user: str
    sd_job_function: JobFunction
    sd_monthly_hourly_divide: PositiveInt

    cpr_uuid_map_path: str = (
        "/opt/dipex/os2mo-data-import-and-export/settings/cpr_uuid_map.csv"
    )
    cache_folder_path: str = "/opt/dipex/os2mo-data-import-and-export/tmp/"


def gen_json_file_settings_func(settings_class: Type[CommonSettings]):
    def json_file_settings(settings: BaseSettings) -> Dict[str, Any]:
        try:
            json_settings = load_settings()
        except FileNotFoundError:
            return dict()

        # Remove the "integrations.SD_Lon." part of the key name
        json_settings = {
            key.replace("integrations.SD_Lon.", "sd_"): value
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


class SDCommonSettings(CommonSettings):
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
                gen_json_file_settings_func(SDCommonSettings),
                file_secret_settings,
            )


class ImporterSettings(CommonSettings):
    municipality_code: conint(ge=100, le=999)  # type: ignore
    municipality_name: str
    sd_importer_create_associations: bool = True
    sd_importer_create_email_addresses: bool = True

    # Deprecated
    sd_importer_employment_date_as_engagement_start_date: bool = False

    sd_no_salary_minimum_id: Optional[int] = None
    sd_skip_employment_types: List[str] = []
    sd_skip_job_functions: List[str] = []
    sd_use_ad_integration: bool = True

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
                gen_json_file_settings_func(ImporterSettings),
                file_secret_settings,
            )


class ChangedAtSettings(CommonSettings):
    sd_fix_departments_root: Optional[UUID4] = None
    sd_no_salary_minimum_id: Optional[int] = None

    # List of CRPs to either include OR exclude in the run
    sd_cprs: List[constr(regex="^[0-9]{10}$")] = []  # type: ignore # noqa

    # If true, the sd_cprs will be excluded and if false, only the
    # CPRs in sd_cprs will be included in the run
    sd_exclude_cprs_mode: bool = True

    sd_read_forced_uuids: bool = True
    sd_skip_job_functions: List[str] = []
    sd_update_primary_engagement: bool = True
    sd_use_ad_integration: bool = True
    sd_log_file: Path = Path("mo_integrations.log")

    sentry_dsn: Optional[str] = None

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
                gen_json_file_settings_func(ChangedAtSettings),
                file_secret_settings,
            )


@lru_cache()
def get_common_settings(*args, **kwargs) -> SDCommonSettings:
    return SDCommonSettings(*args, **kwargs)


@lru_cache()
def get_importer_settings(*args, **kwargs) -> ImporterSettings:
    return ImporterSettings(*args, **kwargs)


@lru_cache()
def get_changed_at_settings(*args, **kwargs) -> ChangedAtSettings:
    return ChangedAtSettings(*args, **kwargs)
