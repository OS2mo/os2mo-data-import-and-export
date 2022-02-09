#!/usr/bin/env python3
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
from functools import lru_cache
from typing import List

from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import Extra
from pydantic import Field
from pydantic import PositiveInt
from ra_utils.load_settings import load_settings

from integrations.SD_Lon.date_utils import DATE_REGEX_STR


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
    properties = Settings.schema()["properties"].keys()
    json_settings = {
        key: value for key, value in json_settings.items() if key in properties
    }

    return json_settings


class Settings(BaseSettings):

    # Should the strings below be optional?

    mora_base: AnyHttpUrl = Field("http://mo-service:5000")
    mox_base: AnyHttpUrl = Field("http://mox-service:8080")
    municipality_code: str
    municipality_cvr: PositiveInt
    municipality_name: str
    sd_employment_field: str
    sd_global_from_date: str = Field(regex=DATE_REGEX_STR)
    sd_import_run_db: str
    sd_import_too_deep: List[str] = []
    sd_importer_create_associations: bool = True
    sd_importer_create_email_addresses: bool = True
    sd_importer_employment_date_as_engagement_start_date: bool = False
    sd_institution_identifier: str
    sd_job_function: str
    sd_monthly_hourly_divide: PositiveInt
    sd_password: str
    sd_skip_employment_types: List[str] = []
    sd_terminate_engagement_with_to_only: bool = True
    sd_use_ad_integration: bool = False
    sd_user: str

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
                json_file_settings,
                file_secret_settings,
            )


@lru_cache()
def get_settings(*args, **kwargs) -> Settings:
    return Settings(*args, **kwargs)
