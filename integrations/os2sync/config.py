import json
import logging
import os
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseSettings
from ra_utils.apply import apply
from ra_utils.headers import TokenSettings

# flake8: noqa
from ra_utils.load_settings import load_settings


def _relpath(filename):
    return os.path.join(os.getcwd(), filename)


def json_config_settings_source(settings: BaseSettings) -> dict[str, Any]:
    """
    Read config from settings.json.

    Reads all keys starting with 'os2sync.' and a few common settings into Settings.
    """
    try: 
        all_settings = load_settings()
    except FileNotFoundError:
        #No settingsfile found. Using environment variables"
        return {}
    #Read os2sync specific settings
    os2sync_settings = dict(
        filter(
            apply(lambda key, value: key.startswith("os2sync")), all_settings.items()
        )
    )
    
    trim_len = len("os2sync.")
    # Trim leading 'os2sync.' from keys and replace dots with underscore.
    final_settings = {key[trim_len:].replace(".", "_"): val for key, val in os2sync_settings.items()}

    #Add needed common settings
    final_settings.update(
        {
            "municipality": all_settings.get("municipality.cvr"),
            "mora_base": all_settings.get("mora.base")
        }
    )

    return final_settings


class Settings(BaseSettings):
    #common:
    municipality: str   #Called "municipality.cvr" in settings.json
    mora_base: str = "http://localhost:5000"     #"mora.base" from settings.json + /service
    
    #os2sync:
    top_unit_uuid: UUID
    api_url: str = "http://localhost:8081"
    
    use_lc_db: bool = False
    
    log_level: int = logging.INFO
    log_file: Path = _relpath("../os2sync.log")
    hash_cache: Path = _relpath("../os2sync_hash_cache")
    xfer_cpr: bool = False
    
    autowash: bool = False
    ca_verify_os2sync: bool = True
    ca_verify_os2mo: bool = True

    phone_scope_classes: list[UUID] = []
    email_scope_classes: list[UUID] = []
    ignored_unit_levels: list[UUID] = []
    ignored_unit_types: list[UUID] = []
    templates: dict = {}



    class Config:
        env_prefix = "os2sync_"
        env_file_encoding = "utf-8"
        extra = "ignore"

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
                json_config_settings_source,
                file_secret_settings,
            )


settings = Settings().dict()
logformat = "%(levelname)s %(asctime)s %(name)s %(message)s"
loggername = "os2sync"

if __name__ == "__main__":
    print(settings)