import json
import logging
import os
from pathlib import Path
from typing import Any, Optional, Dict, List
from uuid import UUID

from pydantic import BaseSettings, AnyHttpUrl
from ra_utils.apply import apply
from ra_utils.headers import TokenSettings

# flake8: noqa
from ra_utils.load_settings import load_settings

from functools import lru_cache


def _relpath(filename):
    return os.path.join(os.getcwd(), filename)


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
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
    
    # replace dots with underscore. eg: os2sync.ignored.unit_levels -> os2sync_ignored_unit_levels
    final_settings = {key.replace(".", "_"): val for key, val in os2sync_settings.items()}

    #Add needed common settings
    municipality = all_settings.get("municipality.cvr")
    if municipality:
        final_settings["municipality"] = municipality
    mora_base = all_settings.get("mora.base")
    if mora_base:
        final_settings["mora_base"] = mora_base

    return final_settings


class Settings(BaseSettings):
    #common:
    municipality: str   #Called "municipality.cvr" in settings.json
    mora_base: AnyHttpUrl = "http://localhost:5000"     #"mora.base" from settings.json + /service
    
    #os2sync:
    os2sync_top_unit_uuid: UUID
    os2sync_api_url: AnyHttpUrl = "http://localhost:8081"

    os2sync_use_lc_db: bool = False
    
    os2sync_log_level: int = logging.INFO
    os2sync_log_file: Path = _relpath("../os2sync.log")
    os2sync_hash_cache: Path = _relpath("../os2sync_hash_cache")
    os2sync_xfer_cpr: bool = False
    
    os2sync_autowash: bool = False
    os2sync_ca_verify_os2sync: bool = True
    os2sync_ca_verify_os2mo: bool = True

    os2sync_phone_scope_classes: List[UUID] = []
    os2sync_email_scope_classes: List[UUID] = []
    os2sync_ignored_unit_levels: List[UUID] = []
    os2sync_ignored_unit_types: List[UUID] = []
    os2sync_templates: Dict = {}
    
    os2sync_use_contact_for_tasks: bool = False
    os2sync_employee_engagement_address: List[str] = []

    
    os2sync_truncate: int = 200
    #MO config - don't set:
    os2mo_has_kle: bool = True
    os2mo_org_uuid: Optional[UUID] = None


    class Config:
        
        env_file_encoding = "utf-8"
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

@lru_cache()
def get_os2sync_settings(*args, **kwargs) -> Settings:
    return Settings(*args, **kwargs)

logformat = "%(levelname)s %(asctime)s %(name)s %(message)s"
loggername = "os2sync"

if __name__ == "__main__":
    print(get_os2sync_settings())