#!/usr/bin/env python3
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
from pydantic import BaseSettings
from pydantic import Extra
from pydantic import AnyHttpUrl

from ra_utils.load_settings import load_settings


def json_file_settings(settings: BaseSettings):
    json_settings = load_settings()

    # Remove the "integrations.SD_Lon." part of the key name
    json_settings = {
        key.replace("integrations.SD_Lon.", "sd_"): value
        for key, value in json_settings.items()
    }

    # Remove any double "sd_sd_" in the keys
    json_settings = {
        key.replace("sd_sd_", "sd_"): value
        for key, value in json_settings.items()
    }

    # Replace dots with underscores to be Pydantic compliant
    json_settings = {
        key.replace(".", "_"): value
        for key, value in json_settings.items()
    }

    return json_settings


class Settings(BaseSettings):
    mora_base: AnyHttpUrl = "http://mo-service:5000"
    sd_importer_employment_date_as_engagement_start_date: bool

    class Config:
        # TODO: change this to "ignore" for "forbid"
        extra = Extra.allow

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


settings = Settings()
